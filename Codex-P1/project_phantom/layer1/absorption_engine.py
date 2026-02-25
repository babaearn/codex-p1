from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from project_phantom.config import Layer1Config
from project_phantom.core.types import (
    AbsorptionBreakdown,
    AbsorptionEvent,
    HealthCounters,
    StablecoinFlowClient,
    StablecoinFlowObservation,
    TradeStreamClient,
    TradeTick,
    TrapSetupEvent,
)
from project_phantom.layer1.metrics import (
    compute_absorption_score,
    compute_cvd_scores,
    compute_stablecoin_inflow_score,
    compute_twap_uniformity_scores,
    compute_whale_net_flow_scores,
    passes_absorption_gate,
)


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class _Layer1State:
    active_setup: TrapSetupEvent | None = None
    trades: deque[TradeTick] = field(default_factory=deque)
    stablecoin_flow: StablecoinFlowObservation | None = None
    last_trade_error: str | None = None
    last_stablecoin_error: str | None = None


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> bool:
    if seconds <= 0:
        return stop_event.is_set()
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
        return True
    except asyncio.TimeoutError:
        return False


def _prune_trades(trades: deque[TradeTick], now_ms: int, window_ms: int) -> None:
    cutoff = now_ms - window_ms
    while trades and trades[0].ts_ms < cutoff:
        trades.popleft()


async def _emit_with_drop_oldest(
    out_queue: asyncio.Queue[AbsorptionEvent],
    event: AbsorptionEvent,
    health: HealthCounters,
) -> None:
    if out_queue.maxsize > 0 and out_queue.full():
        with contextlib.suppress(asyncio.QueueEmpty):
            out_queue.get_nowait()
            health.queue_drops += 1
    await out_queue.put(event)


async def _trap_setup_consumer(
    in_queue: asyncio.Queue[TrapSetupEvent],
    config: Layer1Config,
    state: _Layer1State,
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
        state.active_setup = event


async def _trade_collector(
    client: TradeStreamClient,
    config: Layer1Config,
    state: _Layer1State,
    stop_event: asyncio.Event,
    health: HealthCounters,
) -> None:
    backoff = config.backoff.min_seconds
    while not stop_event.is_set():
        try:
            async for trade in client.stream_trades(config.symbol):
                if stop_event.is_set():
                    return
                state.last_trade_error = None
                state.trades.append(trade)
                _prune_trades(state.trades, now_ms=trade.ts_ms, window_ms=config.trade_window_ms)
            raise RuntimeError("Trade stream unexpectedly ended")
        except Exception as exc:
            state.last_trade_error = f"TRADE_STREAM_{exc.__class__.__name__.upper()}"
            health.increment_reconnect(getattr(client, "name", "trade_client"))
            if await _sleep_or_stop(stop_event, backoff):
                return
            backoff = min(config.backoff.max_seconds, backoff * 2)


async def _stablecoin_flow_poller(
    client: StablecoinFlowClient,
    config: Layer1Config,
    state: _Layer1State,
    stop_event: asyncio.Event,
    health: HealthCounters,
) -> None:
    backoff = config.backoff.min_seconds
    while not stop_event.is_set():
        try:
            observation = await client.fetch_inflow_usd()
            state.stablecoin_flow = observation
            state.last_stablecoin_error = None
            backoff = config.backoff.min_seconds
            if await _sleep_or_stop(stop_event, config.whale_alert.poll_interval_seconds):
                return
        except Exception as exc:
            state.last_stablecoin_error = f"STABLECOIN_{exc.__class__.__name__.upper()}"
            health.increment_reconnect(getattr(client, "name", "stablecoin_client"))
            if await _sleep_or_stop(stop_event, backoff):
                return
            backoff = min(config.backoff.max_seconds, backoff * 2)


async def _scoring_loop(
    config: Layer1Config,
    state: _Layer1State,
    out_queue: asyncio.Queue[AbsorptionEvent],
    stop_event: asyncio.Event,
    health: HealthCounters,
) -> None:
    while not stop_event.is_set():
        now_ms = _now_ms()
        _prune_trades(state.trades, now_ms=now_ms, window_ms=config.trade_window_ms)

        setup = state.active_setup
        if setup is None:
            if await _sleep_or_stop(stop_event, config.cadence_seconds):
                return
            continue

        if (now_ms - setup.ts_ms) > config.setup_ttl_ms:
            state.active_setup = None
            if await _sleep_or_stop(stop_event, config.cadence_seconds):
                return
            continue

        trades = list(state.trades)
        if len(trades) < config.min_trades_for_metrics:
            if await _sleep_or_stop(stop_event, config.cadence_seconds):
                return
            continue

        whale_long, whale_short, whale_net_flow = compute_whale_net_flow_scores(
            trades=trades,
            min_notional=config.thresholds.whale_notional_usd,
            scale_usd=config.thresholds.whale_flow_scale_usd,
        )
        twap_long, twap_short, twap_cv, whale_count = compute_twap_uniformity_scores(
            trades=trades,
            min_notional=config.thresholds.whale_notional_usd,
            cv_limit=config.thresholds.twap_interval_cv_limit,
        )
        cvd_long, cvd_short, cvd_delta, price_delta_pct, hidden_long, hidden_short = compute_cvd_scores(
            trades=trades,
            cvd_scale_usd=config.thresholds.cvd_scale_usd,
        )

        degraded_reasons: list[str] = []
        stablecoin_usd = 0.0
        stablecoin_score = 0.0
        degraded = False

        if config.whale_alert.enabled:
            stable_obs = state.stablecoin_flow
            if stable_obs is None:
                degraded = True
                degraded_reasons.append("WHALE_ALERT_NO_DATA")
            else:
                stablecoin_usd = stable_obs.inflow_usd
                stablecoin_score = compute_stablecoin_inflow_score(
                    inflow_usd=stablecoin_usd,
                    scale_usd=config.thresholds.stablecoin_inflow_scale_usd,
                )
            if state.last_stablecoin_error:
                degraded = True
                degraded_reasons.append(state.last_stablecoin_error)

        breakdown = AbsorptionBreakdown(
            whale_net_flow_long=whale_long,
            whale_net_flow_short=whale_short,
            twap_uniformity_long=twap_long,
            twap_uniformity_short=twap_short,
            cvd_long=cvd_long,
            cvd_short=cvd_short,
            stablecoin_inflow=stablecoin_score,
            hidden_divergence_long=hidden_long,
            hidden_divergence_short=hidden_short,
        )
        direction = setup.direction
        score = compute_absorption_score(breakdown, direction=direction, weights=config.weights)
        passed = passes_absorption_gate(
            breakdown=breakdown,
            direction=direction,
            score=score,
            thresholds=config.thresholds,
        )

        if passed:
            event = AbsorptionEvent(
                event_type="ABSORPTION_EVENT",
                event_id=str(uuid.uuid4()),
                ts_ms=now_ms,
                symbol=config.symbol,
                direction=direction,
                score=score,
                passed=True,
                source_trap_event_id=setup.event_id,
                components=breakdown,
                raw={
                    "trade_count": len(trades),
                    "whale_trade_count": whale_count,
                    "whale_net_flow_usd": whale_net_flow,
                    "twap_interval_cv": twap_cv,
                    "cvd_delta_usd": cvd_delta,
                    "price_delta_pct": price_delta_pct,
                    "stablecoin_inflow_usd": stablecoin_usd,
                    "source_trap_score": setup.score,
                },
                degraded=degraded,
                degrade_reason="|".join(sorted(set(degraded_reasons))) if degraded_reasons else None,
            )
            await _emit_with_drop_oldest(out_queue, event, health)
            health.emitted_events += 1

        if await _sleep_or_stop(stop_event, config.cadence_seconds):
            return


async def run_layer1(
    config: Layer1Config,
    in_queue: asyncio.Queue[TrapSetupEvent],
    out_queue: asyncio.Queue[AbsorptionEvent],
    *,
    stop_event: asyncio.Event | None = None,
    trade_client: TradeStreamClient | None = None,
    stablecoin_client: StablecoinFlowClient | None = None,
    health: HealthCounters | None = None,
) -> None:
    own_stop_event = stop_event is None
    if stop_event is None:
        stop_event = asyncio.Event()
    if health is None:
        health = HealthCounters()

    state = _Layer1State()

    async def _run(
        configured_trade_client: TradeStreamClient,
        configured_stablecoin_client: StablecoinFlowClient | None,
    ) -> None:
        tasks: list[asyncio.Task[Any]] = [
            asyncio.create_task(
                _trap_setup_consumer(in_queue, config, state, stop_event),
                name="layer1-trap-consumer",
            ),
            asyncio.create_task(
                _trade_collector(configured_trade_client, config, state, stop_event, health),
                name="layer1-trade-collector",
            ),
            asyncio.create_task(
                _scoring_loop(config, state, out_queue, stop_event, health),
                name="layer1-scoring-loop",
            ),
        ]

        if configured_stablecoin_client is not None:
            tasks.append(
                asyncio.create_task(
                    _stablecoin_flow_poller(
                        configured_stablecoin_client,
                        config,
                        state,
                        stop_event,
                        health,
                    ),
                    name="layer1-stablecoin-poller",
                )
            )

        try:
            await stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    if trade_client is not None:
        await _run(trade_client, stablecoin_client)
        return

    try:
        import aiohttp  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("aiohttp is required when using default Layer 1 clients") from exc

    from project_phantom.layer1.exchanges.binance_trade_client import BinanceTradeClient
    from project_phantom.layer1.exchanges.whale_alert_client import WhaleAlertClient

    async with aiohttp.ClientSession() as session:
        default_trade_client = BinanceTradeClient(session=session, endpoints=config.endpoints)
        default_stablecoin_client: StablecoinFlowClient | None = None
        if config.whale_alert.enabled:
            default_stablecoin_client = WhaleAlertClient(
                session=session,
                endpoints=config.endpoints,
                config=config.whale_alert,
            )
        try:
            await _run(default_trade_client, default_stablecoin_client)
        except asyncio.CancelledError:
            if own_stop_event:
                stop_event.set()
            raise
        finally:
            with contextlib.suppress(Exception):
                await default_trade_client.close()
            if default_stablecoin_client is not None:
                with contextlib.suppress(Exception):
                    await default_stablecoin_client.close()
