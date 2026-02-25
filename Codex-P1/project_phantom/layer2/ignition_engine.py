from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from dataclasses import asdict
from dataclasses import dataclass
from typing import Any

from project_phantom.config import Layer2Config
from project_phantom.core.types import (
    AbsorptionEvent,
    Candle,
    CandleClient,
    HealthCounters,
    PrePumpEvent,
    SMCDetector,
)
from project_phantom.layer2.signals import build_ignition_breakdown
from project_phantom.layer2.smc_detector import SmartMoneyConceptsDetector


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class _Layer2State:
    active_absorption: AbsorptionEvent | None = None
    candles: list[Candle] | None = None
    last_candle_error: str | None = None
    last_smc_error: str | None = None


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> bool:
    if seconds <= 0:
        return stop_event.is_set()
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
        return True
    except asyncio.TimeoutError:
        return False


async def _emit_with_drop_oldest(
    out_queue: asyncio.Queue[PrePumpEvent],
    event: PrePumpEvent,
    health: HealthCounters,
) -> None:
    if out_queue.maxsize > 0 and out_queue.full():
        with contextlib.suppress(asyncio.QueueEmpty):
            out_queue.get_nowait()
            health.queue_drops += 1
    await out_queue.put(event)


async def _absorption_consumer(
    in_queue: asyncio.Queue[AbsorptionEvent],
    config: Layer2Config,
    state: _Layer2State,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        try:
            event = await asyncio.wait_for(in_queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue

        if not event.passed:
            continue
        if event.symbol.upper() != config.symbol.upper():
            continue
        state.active_absorption = event


async def _candle_poller(
    config: Layer2Config,
    candle_client: CandleClient,
    state: _Layer2State,
    stop_event: asyncio.Event,
    health: HealthCounters,
) -> None:
    backoff = config.backoff.min_seconds
    while not stop_event.is_set():
        try:
            candles = await candle_client.fetch_candles(
                symbol=config.symbol,
                interval=config.candle_interval,
                limit=config.candle_limit,
            )
            state.candles = candles
            state.last_candle_error = None
            backoff = config.backoff.min_seconds
            if await _sleep_or_stop(stop_event, config.rest_poll_interval_seconds):
                return
        except Exception as exc:
            state.last_candle_error = f"CANDLE_{exc.__class__.__name__.upper()}"
            health.increment_reconnect(getattr(candle_client, "name", "candle_client"))
            if await _sleep_or_stop(stop_event, backoff):
                return
            backoff = min(config.backoff.max_seconds, backoff * 2)


async def _scoring_loop(
    config: Layer2Config,
    state: _Layer2State,
    out_queue: asyncio.Queue[PrePumpEvent],
    smc_detector: SMCDetector,
    stop_event: asyncio.Event,
    health: HealthCounters,
) -> None:
    while not stop_event.is_set():
        now_ms = _now_ms()
        absorption = state.active_absorption
        candles = state.candles

        if absorption is None or candles is None:
            if await _sleep_or_stop(stop_event, config.cadence_seconds):
                return
            continue

        if (now_ms - absorption.ts_ms) > config.setup_ttl_ms:
            state.active_absorption = None
            if await _sleep_or_stop(stop_event, config.cadence_seconds):
                return
            continue

        degraded = False
        degraded_reasons: list[str] = []
        choch_signal = False
        order_block_signal = False
        smc_meta: dict[str, Any] = {}

        if state.last_candle_error:
            degraded = True
            degraded_reasons.append(state.last_candle_error)

        if config.enable_smartmoneyconcepts:
            try:
                choch_signal, order_block_signal, smc_meta = await smc_detector.detect(
                    candles=candles,
                    direction=absorption.direction,
                )
                state.last_smc_error = None
            except Exception as exc:
                degraded = True
                state.last_smc_error = f"SMC_{exc.__class__.__name__.upper()}"
                degraded_reasons.append(state.last_smc_error)
                smc_meta = {"backend": "error"}

        if state.last_smc_error:
            degraded = True
            degraded_reasons.append(state.last_smc_error)

        breakdown, derived = build_ignition_breakdown(
            absorption_event=absorption,
            candles=candles,
            direction=absorption.direction,
            choch_signal=choch_signal,
            order_block_signal=order_block_signal,
            thresholds=config.thresholds,
        )

        passed = breakdown.confirmations >= config.thresholds.min_confirmations
        if passed:
            score = breakdown.confirmations / 5.0
            event = PrePumpEvent(
                event_type="PRE_PUMP_EVENT",
                event_id=str(uuid.uuid4()),
                ts_ms=now_ms,
                symbol=config.symbol,
                direction=absorption.direction,
                score=score,
                passed=True,
                source_absorption_event_id=absorption.event_id,
                source_trap_event_id=absorption.source_trap_event_id,
                components=breakdown,
                raw={
                    "min_confirmations": config.thresholds.min_confirmations,
                    "confirmations": breakdown.confirmations,
                    "absorption_score": absorption.score,
                    "momentum_return_pct": derived["momentum_return_pct"],
                    "source_trap_score": derived["source_trap_score"],
                    "smc_backend": smc_meta.get("backend"),
                    "smc_meta": smc_meta,
                    "source_absorption_raw": absorption.raw,
                    "source_absorption_components": asdict(absorption.components),
                },
                degraded=degraded,
                degrade_reason="|".join(sorted(set(degraded_reasons))) if degraded_reasons else None,
            )
            await _emit_with_drop_oldest(out_queue, event, health)
            health.mark_emitted(now_ms)
            # Prevent repeated emissions for the same absorption setup.
            state.active_absorption = None

        if await _sleep_or_stop(stop_event, config.cadence_seconds):
            return


async def run_layer2(
    config: Layer2Config,
    in_queue: asyncio.Queue[AbsorptionEvent],
    out_queue: asyncio.Queue[PrePumpEvent],
    *,
    stop_event: asyncio.Event | None = None,
    candle_client: CandleClient | None = None,
    smc_detector: SMCDetector | None = None,
    health: HealthCounters | None = None,
) -> None:
    own_stop_event = stop_event is None
    if stop_event is None:
        stop_event = asyncio.Event()
    if health is None:
        health = HealthCounters()
    if smc_detector is None:
        smc_detector = SmartMoneyConceptsDetector()

    state = _Layer2State()

    async def _run(configured_candle_client: CandleClient) -> None:
        tasks: list[asyncio.Task[Any]] = [
            asyncio.create_task(
                _absorption_consumer(in_queue, config, state, stop_event),
                name="layer2-absorption-consumer",
            ),
            asyncio.create_task(
                _candle_poller(config, configured_candle_client, state, stop_event, health),
                name="layer2-candle-poller",
            ),
            asyncio.create_task(
                _scoring_loop(config, state, out_queue, smc_detector, stop_event, health),
                name="layer2-scoring-loop",
            ),
        ]
        try:
            await stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    if candle_client is not None:
        await _run(candle_client)
        return

    try:
        import aiohttp  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("aiohttp is required when using default Layer 2 clients") from exc

    from project_phantom.layer2.exchanges.binance_candle_client import BinanceCandleClient

    async with aiohttp.ClientSession() as session:
        default_client = BinanceCandleClient(session=session, endpoints=config.endpoints)
        try:
            await _run(default_client)
        except asyncio.CancelledError:
            if own_stop_event:
                stop_event.set()
            raise
        finally:
            with contextlib.suppress(Exception):
                await default_client.close()
