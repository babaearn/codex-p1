from __future__ import annotations

import argparse
import asyncio
import contextlib
import time

from project_phantom.config import Layer0Config, Layer1Config, Layer2Config, Layer3Config
from project_phantom.core.types import AbsorptionEvent, ExecutionEvent, HealthCounters, PrePumpEvent, TrapSetupEvent
from project_phantom.layer0.trap_detector import run_layer0
from project_phantom.layer1.absorption_engine import run_layer1
from project_phantom.layer2.ignition_engine import run_layer2
from project_phantom.layer3.executor import run_layer3
from project_phantom.layer3.health_report import format_health_report, run_binance_auth_check, run_public_api_checks


async def _execution_printer(queue: asyncio.Queue[ExecutionEvent], stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            event = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        print(
            f"[EXECUTION] symbol={event.symbol} direction={event.direction} "
            f"entry={event.plan.entry} sl={event.plan.sl} tp1={event.plan.tp1} tp2={event.plan.tp2} "
            f"rr=1:{event.plan.rr} mode={event.raw.get('execution_mode')}"
        )


def _format_last_ts(ts_ms: int | None) -> str:
    if ts_ms is None:
        return "n/a"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts_ms / 1000))


def _format_stats_report(
    *,
    symbol: str,
    queue_sizes: dict[str, int],
    counters: dict[str, HealthCounters],
) -> str:
    lines = [
        f"PHANTOM STATS - {symbol}",
        "================================",
        f"q_layer0             : {queue_sizes.get('l0', 0)}",
        f"q_layer1             : {queue_sizes.get('l1', 0)}",
        f"q_layer2             : {queue_sizes.get('l2', 0)}",
        f"q_layer3             : {queue_sizes.get('l3', 0)}",
        "",
    ]
    for layer_name in ("layer0", "layer1", "layer2", "layer3"):
        counter = counters[layer_name]
        reconnects = sum(counter.reconnects.values())
        lines.append(
            f"{layer_name:<20}: emitted={counter.emitted_events} reconnects={reconnects} "
            f"queue_drops={counter.queue_drops}"
        )
        lines.append(f"{layer_name}_last_signal    : {_format_last_ts(counter.last_emitted_ts_ms)}")
    lines.append("================================")
    return "<pre>" + "\n".join(lines) + "</pre>"


def _format_mode_report(*, symbol: str, layer3: Layer3Config) -> str:
    lines = [
        f"PHANTOM MODE - {symbol}",
        "================================",
        f"execution_mode       : {layer3.execution_mode}",
        f"execution_enabled    : {layer3.enable_execution}",
        f"telegram_enabled     : {layer3.telegram.enabled}",
        f"telegram_health      : {layer3.telegram.health_enabled}",
        f"symbol               : {symbol}",
        f"fixed_quantity       : {layer3.fixed_quantity}",
        f"binance_testnet      : {layer3.binance.testnet}",
        "================================",
    ]
    return "<pre>" + "\n".join(lines) + "</pre>"


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run Project PHANTOM 4-layer pipeline.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()

    stop_event = asyncio.Event()
    queue_l0: asyncio.Queue[TrapSetupEvent] = asyncio.Queue(maxsize=200)
    queue_l1: asyncio.Queue[AbsorptionEvent] = asyncio.Queue(maxsize=200)
    queue_l2: asyncio.Queue[PrePumpEvent] = asyncio.Queue(maxsize=200)
    queue_l3: asyncio.Queue[ExecutionEvent] = asyncio.Queue(maxsize=200)

    layer0 = Layer0Config(symbol=args.symbol)
    layer1 = Layer1Config(symbol=args.symbol)
    layer2 = Layer2Config(symbol=args.symbol)
    layer3 = Layer3Config(symbol=args.symbol, execution_mode=args.mode)
    if args.no_telegram:
        layer3.telegram.enabled = False

    health_l0 = HealthCounters()
    health_l1 = HealthCounters()
    health_l2 = HealthCounters()
    health_l3 = HealthCounters()

    tasks = [
        asyncio.create_task(run_layer0(layer0, queue_l0, stop_event=stop_event, health=health_l0), name="layer0"),
        asyncio.create_task(run_layer1(layer1, queue_l0, queue_l1, stop_event=stop_event, health=health_l1), name="layer1"),
        asyncio.create_task(run_layer2(layer2, queue_l1, queue_l2, stop_event=stop_event, health=health_l2), name="layer2"),
        asyncio.create_task(
            run_layer3(layer3, queue_l2, out_queue=queue_l3, stop_event=stop_event, health=health_l3),
            name="layer3",
        ),
        asyncio.create_task(_execution_printer(queue_l3, stop_event), name="execution-printer"),
    ]

    if layer3.telegram.enabled and layer3.telegram.health_enabled and layer3.telegram.bot_token and layer3.telegram.chat_id:
        from telegram import Bot  # type: ignore

        from project_phantom.layer3.notifiers.telegram_health import TelegramHealthService

        async def _build_health_report() -> str:
            api_checks = await run_public_api_checks(
                layer0.endpoints,
                whale_alert_enabled=layer1.whale_alert.enabled,
                whale_alert_api_key=layer1.whale_alert.api_key,
            )
            binance_auth = await run_binance_auth_check(
                enabled=layer3.enable_execution,
                mode=layer3.execution_mode,
                api_key=layer3.binance.api_key,
                api_secret=layer3.binance.api_secret,
                testnet=layer3.binance.testnet,
            )
            return format_health_report(
                symbol=args.symbol,
                mode=layer3.execution_mode,
                queue_sizes={
                    "l0": queue_l0.qsize(),
                    "l1": queue_l1.qsize(),
                    "l2": queue_l2.qsize(),
                    "l3": queue_l3.qsize(),
                },
                counters={
                    "layer0": health_l0,
                    "layer1": health_l1,
                    "layer2": health_l2,
                    "layer3": health_l3,
                },
                api_checks=api_checks,
                binance_auth_check=binance_auth,
                env_presence={
                    "TG_BOT_TOKEN": bool(layer3.telegram.bot_token),
                    "TG_CHAT_ID": bool(layer3.telegram.chat_id),
                    "BINANCE_API_KEY": bool(layer3.binance.api_key),
                    "BINANCE_API_SECRET": bool(layer3.binance.api_secret),
                },
            )

        async def _build_stats_report() -> str:
            return _format_stats_report(
                symbol=args.symbol,
                queue_sizes={
                    "l0": queue_l0.qsize(),
                    "l1": queue_l1.qsize(),
                    "l2": queue_l2.qsize(),
                    "l3": queue_l3.qsize(),
                },
                counters={
                    "layer0": health_l0,
                    "layer1": health_l1,
                    "layer2": health_l2,
                    "layer3": health_l3,
                },
            )

        async def _build_mode_report() -> str:
            return _format_mode_report(symbol=args.symbol, layer3=layer3)

        health_service = TelegramHealthService(
            bot=Bot(token=layer3.telegram.bot_token),
            allowed_chat_id=layer3.telegram.chat_id,
            command_handlers={
                "/health": _build_health_report,
                "/stats": _build_stats_report,
                "/mode": _build_mode_report,
            },
            poll_interval_seconds=layer3.telegram.health_poll_interval_seconds,
            cooldown_seconds=layer3.telegram.health_cooldown_seconds,
        )
        tasks.append(asyncio.create_task(health_service.run(stop_event), name="telegram-health"))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        raise
    finally:
        stop_event.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
