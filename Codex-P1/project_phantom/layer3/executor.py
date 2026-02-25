from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections import deque
from typing import Any

from project_phantom.config import Layer3Config
from project_phantom.core.types import (
    ExecutionEvent,
    FuturesExecutionClient,
    HealthCounters,
    PrePumpEvent,
    TelegramNotifier,
)
from project_phantom.layer3.planner import build_execution_plan, derive_entry_price
from project_phantom.layer3.telegram_formatter import format_telegram_signal


def _now_ms() -> int:
    return int(time.time() * 1000)


def _side_from_direction(direction: str) -> tuple[str, str]:
    if direction == "LONG":
        return ("BUY", "SELL")
    return ("SELL", "BUY")


def _extract_entry_price(order_response: dict[str, Any]) -> float | None:
    for key in ("avgPrice", "price"):
        value = order_response.get(key)
        try:
            if value is not None and float(value) > 0:
                return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _order_id(order_response: dict[str, Any]) -> str:
    value = order_response.get("orderId")
    if value is None:
        return "n/a"
    return str(value)


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> bool:
    if seconds <= 0:
        return stop_event.is_set()
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
        return True
    except asyncio.TimeoutError:
        return False


async def _emit_with_drop_oldest(
    out_queue: asyncio.Queue[ExecutionEvent] | None,
    event: ExecutionEvent,
    health: HealthCounters,
) -> None:
    if out_queue is None:
        return
    if out_queue.maxsize > 0 and out_queue.full():
        with contextlib.suppress(asyncio.QueueEmpty):
            out_queue.get_nowait()
            health.queue_drops += 1
    await out_queue.put(event)


async def _place_execution_orders(
    config: Layer3Config,
    event: PrePumpEvent,
    client: FuturesExecutionClient,
) -> tuple[dict[str, str], dict[str, Any], float]:
    entry_side, exit_side = _side_from_direction(event.direction)
    quantity = config.fixed_quantity

    entry_response = await client.futures_create_order(
        symbol=config.symbol,
        side=entry_side,
        type="MARKET",
        quantity=quantity,
    )
    entry_price = _extract_entry_price(entry_response)
    if entry_price is None:
        derived = derive_entry_price(event)
        if derived is None:
            raise RuntimeError("Unable to derive entry price from order response or event payload")
        entry_price = derived

    plan = build_execution_plan(
        event,
        entry_price=entry_price,
        quantity=quantity,
        risk_config=config.risk,
    )

    tp1_qty = round(quantity * config.risk.tp1_quantity_ratio, 6)
    tp2_qty = round(max(quantity - tp1_qty, 0.0), 6)
    if tp1_qty <= 0:
        tp1_qty = round(quantity, 6)
    if tp2_qty <= 0:
        tp2_qty = round(quantity, 6)

    sl_response = await client.futures_create_order(
        symbol=config.symbol,
        side=exit_side,
        type="STOP_MARKET",
        stopPrice=plan.sl,
        quantity=round(quantity, 6),
        reduceOnly=True,
        workingType="MARK_PRICE",
    )
    tp1_response = await client.futures_create_order(
        symbol=config.symbol,
        side=exit_side,
        type="TAKE_PROFIT_MARKET",
        stopPrice=plan.tp1,
        quantity=tp1_qty,
        reduceOnly=True,
        workingType="MARK_PRICE",
    )
    tp2_response = await client.futures_create_order(
        symbol=config.symbol,
        side=exit_side,
        type="TAKE_PROFIT_MARKET",
        stopPrice=plan.tp2,
        quantity=tp2_qty,
        reduceOnly=True,
        workingType="MARK_PRICE",
    )

    order_ids = {
        "entry": _order_id(entry_response),
        "sl": _order_id(sl_response),
        "tp1": _order_id(tp1_response),
        "tp2": _order_id(tp2_response),
    }
    return (order_ids, {"entry": entry_response, "sl": sl_response, "tp1": tp1_response, "tp2": tp2_response}, entry_price)


async def run_layer3(
    config: Layer3Config,
    in_queue: asyncio.Queue[PrePumpEvent],
    *,
    out_queue: asyncio.Queue[ExecutionEvent] | None = None,
    stop_event: asyncio.Event | None = None,
    execution_client: FuturesExecutionClient | None = None,
    telegram_notifier: TelegramNotifier | None = None,
    health: HealthCounters | None = None,
) -> None:
    own_stop_event = stop_event is None
    if stop_event is None:
        stop_event = asyncio.Event()
    if health is None:
        health = HealthCounters()

    active_execution_client = execution_client
    active_telegram_notifier = telegram_notifier
    last_entry_ts_ms: int | None = None
    recent_entry_ts: deque[int] = deque()
    seen_source_event_ids: set[str] = set()
    seen_source_fifo: deque[str] = deque(maxlen=4000)

    async def _process_event(event: PrePumpEvent) -> None:
        nonlocal last_entry_ts_ms
        if not event.passed:
            return
        if event.symbol.upper() != config.symbol.upper():
            return
        if (_now_ms() - event.ts_ms) > config.pre_pump_ttl_ms:
            return
        if event.event_id in seen_source_event_ids:
            return

        now_ms = _now_ms()
        if last_entry_ts_ms is not None and config.guard.min_seconds_between_entries > 0:
            min_gap_ms = int(config.guard.min_seconds_between_entries * 1000)
            if (now_ms - last_entry_ts_ms) < min_gap_ms:
                print(
                    f"[L3-GUARD] symbol={event.symbol} skip=COOLDOWN "
                    f"remaining_ms={min_gap_ms - (now_ms - last_entry_ts_ms)}",
                    flush=True,
                )
                return

        cutoff_ms = now_ms - 3_600_000
        while recent_entry_ts and recent_entry_ts[0] < cutoff_ms:
            recent_entry_ts.popleft()
        if config.guard.max_entries_per_hour > 0 and len(recent_entry_ts) >= config.guard.max_entries_per_hour:
            print(
                f"[L3-GUARD] symbol={event.symbol} skip=RATE_LIMIT_PER_HOUR "
                f"limit={config.guard.max_entries_per_hour}",
                flush=True,
            )
            return

        degraded = False
        degraded_reasons: list[str] = []
        order_ids: dict[str, str] = {}
        execution_raw: dict[str, Any] = {}
        plan = None

        try:
            if config.enable_execution and config.execution_mode.lower() == "live":
                if active_execution_client is None:
                    raise RuntimeError("Execution client is required for live mode")
                order_ids, execution_raw, entry_price = await _place_execution_orders(config, event, active_execution_client)
            else:
                entry_price = derive_entry_price(event)
                if entry_price is None:
                    raise RuntimeError("Unable to derive entry price for paper execution")
                plan = build_execution_plan(
                    event,
                    entry_price=entry_price,
                    quantity=config.fixed_quantity,
                    risk_config=config.risk,
                )
                order_ids = {"entry": "paper-entry", "sl": "paper-sl", "tp1": "paper-tp1", "tp2": "paper-tp2"}
                execution_raw = {"mode": "paper"}

            if plan is None:
                plan = build_execution_plan(
                    event,
                    entry_price=entry_price,
                    quantity=config.fixed_quantity,
                    risk_config=config.risk,
                )

            if active_telegram_notifier is not None and config.telegram.enabled:
                try:
                    message = format_telegram_signal(event, plan, order_ids=order_ids)
                    await active_telegram_notifier.send_message(message)
                except Exception as exc:
                    degraded = True
                    degraded_reasons.append(f"TELEGRAM_{exc.__class__.__name__.upper()}")

            execution_event = ExecutionEvent(
                event_type="EXECUTION_EVENT",
                event_id=str(uuid.uuid4()),
                ts_ms=_now_ms(),
                symbol=config.symbol,
                direction=event.direction,
                passed=True,
                source_pre_pump_event_id=event.event_id,
                plan=plan,
                order_ids=order_ids,
                raw={
                    "execution_mode": config.execution_mode,
                    "source_pre_pump_score": event.score,
                    "source_pre_pump_confirmations": event.components.confirmations,
                    "execution_raw": execution_raw,
                },
                degraded=degraded,
                degrade_reason="|".join(sorted(set(degraded_reasons))) if degraded_reasons else None,
            )
            await _emit_with_drop_oldest(out_queue, execution_event, health)
            health.mark_emitted(execution_event.ts_ms)
            last_entry_ts_ms = execution_event.ts_ms
            recent_entry_ts.append(execution_event.ts_ms)
            if len(seen_source_fifo) == seen_source_fifo.maxlen and seen_source_fifo:
                old_id = seen_source_fifo.popleft()
                seen_source_event_ids.discard(old_id)
            seen_source_event_ids.add(event.event_id)
            seen_source_fifo.append(event.event_id)
        except Exception as exc:
            health.increment_reconnect("layer3_executor")
            if active_telegram_notifier is not None and config.telegram.enabled:
                with contextlib.suppress(Exception):
                    await active_telegram_notifier.send_message(
                        f"<pre>PHANTOM EXECUTION ERROR\n{event.symbol} {event.direction}\n{exc}</pre>"
                    )

    async def _consume_loop() -> None:
        while not stop_event.is_set():
            try:
                event = await asyncio.wait_for(in_queue.get(), timeout=max(config.cadence_seconds, 0.1))
            except asyncio.TimeoutError:
                continue
            await _process_event(event)
            if await _sleep_or_stop(stop_event, config.cadence_seconds):
                return

    if active_execution_client is None and config.enable_execution and config.execution_mode.lower() == "live":
        from project_phantom.layer3.exchanges.binance_futures_client import BinanceFuturesExecutionClient

        active_execution_client = await BinanceFuturesExecutionClient.create(config.binance)
    if active_telegram_notifier is None and config.telegram.enabled:
        from project_phantom.layer3.notifiers.telegram_client import TelegramBotNotifier

        active_telegram_notifier = await TelegramBotNotifier.create(config.telegram)

    task = asyncio.create_task(_consume_loop(), name="layer3-prepump-consumer")
    try:
        await stop_event.wait()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if own_stop_event:
            stop_event.set()
        if active_execution_client is not None:
            with contextlib.suppress(Exception):
                await active_execution_client.close()
        if active_telegram_notifier is not None:
            with contextlib.suppress(Exception):
                await active_telegram_notifier.close()
