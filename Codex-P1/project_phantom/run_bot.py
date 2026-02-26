from __future__ import annotations

import argparse
import asyncio
import contextlib
import time
from dataclasses import dataclass

from project_phantom.config import Layer0Config, Layer1Config, Layer2Config, Layer3Config
from project_phantom.core.types import AbsorptionEvent, ExecutionEvent, HealthCounters, PrePumpEvent, TrapSetupEvent
from project_phantom.layer0.trap_detector import run_layer0
from project_phantom.layer1.absorption_engine import run_layer1
from project_phantom.layer2.ignition_engine import run_layer2
from project_phantom.layer3.executor import run_layer3
from project_phantom.layer3.health_report import format_health_report, run_binance_auth_check, run_public_api_checks
from project_phantom.universe import discover_common_futures_symbols

DEFAULT_ALL_COMMON_MAX_SYMBOLS = 20
DEFAULT_HARD_CAP_SYMBOLS = 25


@dataclass
class _SymbolRuntime:
    symbol: str
    queue_l0: asyncio.Queue[TrapSetupEvent]
    queue_l1: asyncio.Queue[AbsorptionEvent]
    queue_l2: asyncio.Queue[PrePumpEvent]
    health_l0: HealthCounters
    health_l1: HealthCounters
    health_l2: HealthCounters
    health_l3: HealthCounters


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _parse_symbol_csv(raw: str) -> list[str]:
    seen: set[str] = set()
    symbols: list[str] = []
    for part in raw.split(","):
        symbol = _normalize_symbol(part)
        if not symbol:
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


def _symbol_scope(symbols: list[str]) -> str:
    if len(symbols) == 1:
        return symbols[0]
    return f"MULTI[{len(symbols)}]"


async def _resolve_symbols(args: argparse.Namespace) -> list[str]:
    if args.symbols is None or not args.symbols.strip():
        return [_normalize_symbol(args.symbol)]

    if args.symbols.strip().upper() == "ALL_COMMON":
        endpoints = Layer0Config().endpoints
        requested_max_symbols = args.max_symbols if args.max_symbols > 0 else DEFAULT_ALL_COMMON_MAX_SYMBOLS
        discovered = await discover_common_futures_symbols(
            endpoints=endpoints,
            max_symbols=requested_max_symbols,
        )
        if not discovered:
            raise RuntimeError("No futures symbols were discovered from Binance/Bybit endpoints")
        return discovered

    symbols = _parse_symbol_csv(args.symbols)
    if not symbols:
        raise RuntimeError("No valid symbols were provided in --symbols")
    return symbols


def _apply_fanout_cap(symbols: list[str], *, hard_cap_symbols: int, allow_unsafe_fanout: bool) -> list[str]:
    if allow_unsafe_fanout:
        return symbols
    if hard_cap_symbols <= 0:
        return symbols
    if len(symbols) <= hard_cap_symbols:
        return symbols
    trimmed = symbols[:hard_cap_symbols]
    print(
        f"[SAFE-CAP] Requested {len(symbols)} symbols; using first {hard_cap_symbols} to avoid OOM. "
        "Use --allow-unsafe-fanout to override.",
        flush=True,
    )
    return trimmed


def _aggregate_queue_sizes(
    runtimes: list[_SymbolRuntime],
    execution_queue: asyncio.Queue[ExecutionEvent],
) -> dict[str, int]:
    return {
        "l0": sum(runtime.queue_l0.qsize() for runtime in runtimes),
        "l1": sum(runtime.queue_l1.qsize() for runtime in runtimes),
        "l2": sum(runtime.queue_l2.qsize() for runtime in runtimes),
        "l3": execution_queue.qsize(),
    }


def _aggregate_counters(runtimes: list[_SymbolRuntime]) -> dict[str, HealthCounters]:
    aggregate = {
        "layer0": HealthCounters(),
        "layer1": HealthCounters(),
        "layer2": HealthCounters(),
        "layer3": HealthCounters(),
    }

    for runtime in runtimes:
        per_layer = {
            "layer0": runtime.health_l0,
            "layer1": runtime.health_l1,
            "layer2": runtime.health_l2,
            "layer3": runtime.health_l3,
        }
        for layer_name, counter in per_layer.items():
            target = aggregate[layer_name]
            target.stale_cycles += counter.stale_cycles
            target.queue_drops += counter.queue_drops
            target.emitted_events += counter.emitted_events
            if counter.last_emitted_ts_ms is not None:
                if target.last_emitted_ts_ms is None or counter.last_emitted_ts_ms > target.last_emitted_ts_ms:
                    target.last_emitted_ts_ms = counter.last_emitted_ts_ms
            for exchange, count in counter.reconnects.items():
                target.reconnects[exchange] = target.reconnects.get(exchange, 0) + count

    return aggregate


async def _execution_printer(queue: asyncio.Queue[ExecutionEvent], stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            event = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        print(
            f"[EXECUTION] symbol={event.symbol} direction={event.direction} "
            f"entry={event.plan.entry} sl={event.plan.sl} tp1={event.plan.tp1} tp2={event.plan.tp2} "
            f"rr=1:{event.plan.rr} mode={event.raw.get('execution_mode')}",
            flush=True,
        )


def _format_last_ts(ts_ms: int | None) -> str:
    if ts_ms is None:
        return "n/a"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts_ms / 1000))


def _format_stats_report(
    *,
    symbol_scope: str,
    queue_sizes: dict[str, int],
    counters: dict[str, HealthCounters],
    runtimes: list[_SymbolRuntime],
) -> str:
    lines = [
        f"PHANTOM STATS - {symbol_scope}",
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

    if len(runtimes) > 1:
        lines.append("")
        lines.append("SYMBOL EMITS (l0/l1/l2/l3)")
        shown = 0
        for runtime in sorted(runtimes, key=lambda item: item.symbol):
            lines.append(
                f"{runtime.symbol:<12} : "
                f"{runtime.health_l0.emitted_events}/"
                f"{runtime.health_l1.emitted_events}/"
                f"{runtime.health_l2.emitted_events}/"
                f"{runtime.health_l3.emitted_events}"
            )
            shown += 1
            if shown >= 20:
                break
        if len(runtimes) > shown:
            lines.append(f"... +{len(runtimes) - shown} more symbols")

    lines.append("================================")
    return "<pre>" + "\n".join(lines) + "</pre>"


def _format_mode_report(*, symbol_scope: str, symbol_count: int, layer3: Layer3Config) -> str:
    lines = [
        f"PHANTOM MODE - {symbol_scope}",
        "================================",
        f"execution_mode       : {layer3.execution_mode}",
        f"execution_enabled    : {layer3.enable_execution}",
        f"telegram_enabled     : {layer3.telegram.enabled}",
        f"telegram_health      : {layer3.telegram.health_enabled}",
        f"symbols_count        : {symbol_count}",
        f"fixed_quantity       : {layer3.fixed_quantity}",
        f"binance_testnet      : {layer3.binance.testnet}",
        f"entry_cooldown_s     : {layer3.guard.min_seconds_between_entries}",
        f"max_entries_per_hour : {layer3.guard.max_entries_per_hour}",
        f"sizing_enabled       : {layer3.sizing.enabled}",
        f"sizing_min_max_mult  : {layer3.sizing.min_multiplier}-{layer3.sizing.max_multiplier}",
        f"session_enabled      : {layer3.session.enabled}",
        f"session_hours_utc    : {','.join(str(x) for x in layer3.session.allowed_hours_utc)}",
        "================================",
    ]
    return "<pre>" + "\n".join(lines) + "</pre>"


async def _heartbeat_logger(
    *,
    stop_event: asyncio.Event,
    runtimes: list[_SymbolRuntime],
    execution_queue: asyncio.Queue[ExecutionEvent],
    interval_seconds: int = 60,
) -> None:
    while not stop_event.is_set():
        queue_sizes = _aggregate_queue_sizes(runtimes, execution_queue)
        counters = _aggregate_counters(runtimes)
        print(
            "[HEARTBEAT] "
            f"symbols={len(runtimes)} "
            f"q=({queue_sizes['l0']},{queue_sizes['l1']},{queue_sizes['l2']},{queue_sizes['l3']}) "
            f"emitted=({counters['layer0'].emitted_events},{counters['layer1'].emitted_events},"
            f"{counters['layer2'].emitted_events},{counters['layer3'].emitted_events}) "
            f"drops=({counters['layer0'].queue_drops},{counters['layer1'].queue_drops},"
            f"{counters['layer2'].queue_drops},{counters['layer3'].queue_drops})",
            flush=True,
        )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            continue


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run Project PHANTOM 4-layer pipeline.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated symbols or ALL_COMMON (common Binance+Bybit USDT futures).",
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=DEFAULT_ALL_COMMON_MAX_SYMBOLS,
        help=f"Cap when --symbols=ALL_COMMON. Default={DEFAULT_ALL_COMMON_MAX_SYMBOLS}.",
    )
    parser.add_argument(
        "--hard-cap-symbols",
        type=int,
        default=DEFAULT_HARD_CAP_SYMBOLS,
        help=f"Safety cap on total symbol fanout. Default={DEFAULT_HARD_CAP_SYMBOLS}.",
    )
    parser.add_argument(
        "--allow-unsafe-fanout",
        action="store_true",
        help="Disable safety cap and allow very high symbol fanout (higher OOM risk).",
    )
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()

    symbols = await _resolve_symbols(args)
    symbols = _apply_fanout_cap(
        symbols,
        hard_cap_symbols=args.hard_cap_symbols,
        allow_unsafe_fanout=args.allow_unsafe_fanout,
    )
    symbol_scope = _symbol_scope(symbols)

    stop_event = asyncio.Event()
    execution_queue: asyncio.Queue[ExecutionEvent] = asyncio.Queue(maxsize=max(200, len(symbols) * 12))

    print(
        "[BOOT] "
        f"symbol_scope={symbol_scope} mode={args.mode} symbols_count={len(symbols)} "
        f"symbols_sample={','.join(symbols[:8])}",
        flush=True,
    )

    runtimes: list[_SymbolRuntime] = []
    tasks: list[asyncio.Task[object]] = []
    if len(symbols) <= 10:
        per_symbol_queue_max = 200
    elif len(symbols) <= 25:
        per_symbol_queue_max = 80
    else:
        per_symbol_queue_max = 40

    for symbol in symbols:
        queue_l0: asyncio.Queue[TrapSetupEvent] = asyncio.Queue(maxsize=per_symbol_queue_max)
        queue_l1: asyncio.Queue[AbsorptionEvent] = asyncio.Queue(maxsize=per_symbol_queue_max)
        queue_l2: asyncio.Queue[PrePumpEvent] = asyncio.Queue(maxsize=per_symbol_queue_max)

        health_l0 = HealthCounters()
        health_l1 = HealthCounters()
        health_l2 = HealthCounters()
        health_l3 = HealthCounters()

        layer0 = Layer0Config(symbol=symbol, queue_maxsize=per_symbol_queue_max)
        layer1 = Layer1Config(symbol=symbol, queue_maxsize=per_symbol_queue_max)
        layer2 = Layer2Config(symbol=symbol, queue_maxsize=per_symbol_queue_max)
        layer3 = Layer3Config(symbol=symbol, queue_maxsize=per_symbol_queue_max, execution_mode=args.mode)
        if args.no_telegram:
            layer3.telegram.enabled = False

        runtimes.append(
            _SymbolRuntime(
                symbol=symbol,
                queue_l0=queue_l0,
                queue_l1=queue_l1,
                queue_l2=queue_l2,
                health_l0=health_l0,
                health_l1=health_l1,
                health_l2=health_l2,
                health_l3=health_l3,
            )
        )

        symbol_name = symbol.lower()
        tasks.extend(
            [
                asyncio.create_task(
                    run_layer0(layer0, queue_l0, stop_event=stop_event, health=health_l0),
                    name=f"layer0-{symbol_name}",
                ),
                asyncio.create_task(
                    run_layer1(layer1, queue_l0, queue_l1, stop_event=stop_event, health=health_l1),
                    name=f"layer1-{symbol_name}",
                ),
                asyncio.create_task(
                    run_layer2(layer2, queue_l1, queue_l2, stop_event=stop_event, health=health_l2),
                    name=f"layer2-{symbol_name}",
                ),
                asyncio.create_task(
                    run_layer3(
                        layer3,
                        queue_l2,
                        out_queue=execution_queue,
                        stop_event=stop_event,
                        health=health_l3,
                    ),
                    name=f"layer3-{symbol_name}",
                ),
            ]
        )

    tasks.append(asyncio.create_task(_execution_printer(execution_queue, stop_event), name="execution-printer"))
    tasks.append(
        asyncio.create_task(
            _heartbeat_logger(
                stop_event=stop_event,
                runtimes=runtimes,
                execution_queue=execution_queue,
            ),
            name="heartbeat-logger",
        )
    )

    primary_symbol = symbols[0]
    primary_layer0 = Layer0Config(symbol=primary_symbol)
    primary_layer1 = Layer1Config(symbol=primary_symbol)
    primary_layer3 = Layer3Config(symbol=primary_symbol, execution_mode=args.mode)
    if args.no_telegram:
        primary_layer3.telegram.enabled = False

    if (
        primary_layer3.telegram.enabled
        and primary_layer3.telegram.health_enabled
        and primary_layer3.telegram.bot_token
        and primary_layer3.telegram.chat_id
    ):
        from telegram import Bot  # type: ignore

        from project_phantom.layer3.notifiers.telegram_health import TelegramHealthService

        async def _build_health_report() -> str:
            api_checks = await run_public_api_checks(
                primary_layer0.endpoints,
                whale_alert_enabled=primary_layer1.whale_alert.enabled,
                whale_alert_api_key=primary_layer1.whale_alert.api_key,
            )
            binance_auth = await run_binance_auth_check(
                enabled=primary_layer3.enable_execution,
                mode=primary_layer3.execution_mode,
                api_key=primary_layer3.binance.api_key,
                api_secret=primary_layer3.binance.api_secret,
                testnet=primary_layer3.binance.testnet,
            )
            return format_health_report(
                symbol=symbol_scope,
                mode=primary_layer3.execution_mode,
                queue_sizes=_aggregate_queue_sizes(runtimes, execution_queue),
                counters=_aggregate_counters(runtimes),
                api_checks=api_checks,
                binance_auth_check=binance_auth,
                env_presence={
                    "TG_BOT_TOKEN": bool(primary_layer3.telegram.bot_token),
                    "TG_CHAT_ID": bool(primary_layer3.telegram.chat_id),
                    "BINANCE_API_KEY": bool(primary_layer3.binance.api_key),
                    "BINANCE_API_SECRET": bool(primary_layer3.binance.api_secret),
                },
            )

        async def _build_stats_report() -> str:
            return _format_stats_report(
                symbol_scope=symbol_scope,
                queue_sizes=_aggregate_queue_sizes(runtimes, execution_queue),
                counters=_aggregate_counters(runtimes),
                runtimes=runtimes,
            )

        async def _build_mode_report() -> str:
            return _format_mode_report(
                symbol_scope=symbol_scope,
                symbol_count=len(symbols),
                layer3=primary_layer3,
            )

        health_service = TelegramHealthService(
            bot=Bot(token=primary_layer3.telegram.bot_token),
            allowed_chat_id=primary_layer3.telegram.chat_id,
            command_handlers={
                "/health": _build_health_report,
                "/stats": _build_stats_report,
                "/mode": _build_mode_report,
            },
            poll_interval_seconds=primary_layer3.telegram.health_poll_interval_seconds,
            cooldown_seconds=primary_layer3.telegram.health_cooldown_seconds,
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
