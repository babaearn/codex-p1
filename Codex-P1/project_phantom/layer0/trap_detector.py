from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from statistics import mean
from typing import Any

from project_phantom.config import Layer0Config
from project_phantom.core.types import (
    ExchangeClient,
    ExchangeSnapshot,
    HealthCounters,
    OIObservation,
    SignalBreakdown,
    TrapSetupEvent,
)
from project_phantom.layer0.liquidation_book import LiquidationBook
from project_phantom.layer0.signals import (
    compute_adaptive_threshold,
    compute_directional_score,
    compute_funding_oi_scores,
    compute_oi_acceleration,
    compute_oi_divergence_score,
    compute_oi_pct_change,
    compute_regime_scores,
    compute_realized_volatility,
    compute_return,
    has_warmup_window,
    passes_gate,
)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _error_reason(exchange: str, exc: Exception) -> str:
    detail = str(exc)
    if exchange == "bybit" and "403" in detail:
        return "BYBIT_403"
    return f"{exchange.upper()}_{exc.__class__.__name__.upper()}"


@dataclass
class _ExchangeState:
    snapshot: ExchangeSnapshot | None = None
    oi_history: deque[OIObservation] = field(default_factory=deque)
    last_error: str | None = None
    last_error_ts_ms: int = 0


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> bool:
    if seconds <= 0:
        return stop_event.is_set()
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
        return True
    except asyncio.TimeoutError:
        return False


def _record_oi(state: _ExchangeState, ts_ms: int, open_interest: float, max_keep_ms: int = 3 * 60 * 60_000) -> None:
    state.oi_history.append(OIObservation(ts_ms=ts_ms, open_interest=open_interest))
    cutoff = ts_ms - max_keep_ms
    while state.oi_history and state.oi_history[0].ts_ms < cutoff:
        state.oi_history.popleft()


async def _emit_with_drop_oldest(
    out_queue: asyncio.Queue[TrapSetupEvent],
    event: TrapSetupEvent,
    health: HealthCounters,
) -> None:
    if out_queue.maxsize > 0 and out_queue.full():
        with contextlib.suppress(asyncio.QueueEmpty):
            out_queue.get_nowait()
            health.queue_drops += 1
    await out_queue.put(event)


async def _snapshot_poller(
    client: ExchangeClient,
    config: Layer0Config,
    state: _ExchangeState,
    stop_event: asyncio.Event,
    health: HealthCounters,
    price_history: deque[tuple[int, float]],
) -> None:
    backoff = config.backoff.min_seconds
    while not stop_event.is_set():
        try:
            snapshot = await client.fetch_snapshot(config.symbol)
            state.snapshot = snapshot
            state.last_error = None
            state.last_error_ts_ms = 0

            if snapshot.open_interest is not None:
                _record_oi(state, snapshot.ts_ms, snapshot.open_interest)

            if snapshot.mark_price is not None and snapshot.mark_price > 0:
                price_history.append((snapshot.ts_ms, snapshot.mark_price))
                cutoff = snapshot.ts_ms - 2 * 60 * 60_000
                while price_history and price_history[0][0] < cutoff:
                    price_history.popleft()

            backoff = config.backoff.min_seconds
            if await _sleep_or_stop(stop_event, config.rest_poll_interval_seconds):
                return
        except Exception as exc:
            state.last_error = _error_reason(client.name, exc)
            state.last_error_ts_ms = _now_ms()
            health.increment_reconnect(client.name)
            if await _sleep_or_stop(stop_event, backoff):
                return
            backoff = min(config.backoff.max_seconds, backoff * 2)


async def _liquidation_worker(
    client: ExchangeClient,
    config: Layer0Config,
    book: LiquidationBook,
    stop_event: asyncio.Event,
    health: HealthCounters,
) -> None:
    backoff = config.backoff.min_seconds
    while not stop_event.is_set():
        try:
            async for event in client.stream_liquidations(config.symbol):
                if stop_event.is_set():
                    return
                book.add(event)
            raise RuntimeError(f"{client.name} liquidation stream ended")
        except Exception:
            health.increment_reconnect(client.name)
            if await _sleep_or_stop(stop_event, backoff):
                return
            backoff = min(config.backoff.max_seconds, backoff * 2)


def _build_default_clients(session: Any, config: Layer0Config) -> dict[str, ExchangeClient]:
    from project_phantom.layer0.exchanges.binance_client import BinanceClient
    from project_phantom.layer0.exchanges.bybit_client import BybitClient
    from project_phantom.layer0.exchanges.okx_client import OkxClient

    clients: dict[str, ExchangeClient] = {}
    if config.enable_binance:
        clients["binance"] = BinanceClient(session=session, endpoints=config.endpoints)
    if config.enable_bybit:
        clients["bybit"] = BybitClient(session=session, endpoints=config.endpoints)
    if config.enable_okx:
        clients["okx"] = OkxClient(session=session, endpoints=config.endpoints)
    return clients


def _compose_raw_payload(
    active_exchanges: list[str],
    oi_change_map: dict[str, float],
    oi_accel_map: dict[str, float],
    spread: float,
    current_price: float,
    long_score: float,
    short_score: float,
    secondary_score: float,
    long_distance_pct: float | None,
    short_distance_pct: float | None,
    p90_short: float,
    p90_long: float,
    funding_meta: dict[str, float | str],
    threshold_score: float,
    adaptive_threshold: float,
    component_threshold: float,
    regime_long_score: float,
    regime_short_score: float,
    regime_meta: dict[str, float | str],
) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "active_exchanges": ",".join(active_exchanges),
        "oi_spread_pct": spread,
        "oi_changes_pct": oi_change_map,
        "oi_accel_pct": oi_accel_map,
        "current_price": current_price,
        "long_score": long_score,
        "short_score": short_score,
        "secondary_score": secondary_score,
        "long_cluster_distance_pct": long_distance_pct,
        "short_cluster_distance_pct": short_distance_pct,
        "short_cluster_p90_notional": p90_short,
        "long_cluster_p90_notional": p90_long,
        "score_threshold": threshold_score,
        "adaptive_score_threshold": adaptive_threshold,
        "component_threshold": component_threshold,
        "regime_long_score": regime_long_score,
        "regime_short_score": regime_short_score,
    }
    raw.update(funding_meta)
    raw.update(regime_meta)
    return raw


async def _scoring_loop(
    config: Layer0Config,
    states: dict[str, _ExchangeState],
    book: LiquidationBook,
    out_queue: asyncio.Queue[TrapSetupEvent],
    stop_event: asyncio.Event,
    health: HealthCounters,
    price_history: deque[tuple[int, float]],
) -> None:
    configured_exchanges = list(states.keys())
    score_history: deque[float] = deque(maxlen=max(1, config.adaptive_gate.window_cycles))
    while not stop_event.is_set():
        cycle_start_ms = _now_ms()
        stale_names: list[str] = []
        degraded_reasons: list[str] = []

        if not has_warmup_window(
            {name: state.oi_history for name, state in states.items()},
            now_ms=cycle_start_ms,
            warmup_ms=config.warmup_ms,
        ):
            if await _sleep_or_stop(stop_event, config.cadence_seconds):
                return
            continue

        active_snapshots: dict[str, ExchangeSnapshot] = {}
        funding_rates: list[float] = []
        oi_changes_pct: list[float] = []
        oi_accels_pct: list[float] = []
        oi_change_map: dict[str, float] = {}
        oi_accel_map: dict[str, float] = {}

        for name, state in states.items():
            snapshot = state.snapshot
            if snapshot is None:
                degraded_reasons.append(f"{name.upper()}_NO_SNAPSHOT")
                if state.last_error:
                    degraded_reasons.append(state.last_error)
                continue
            if (cycle_start_ms - snapshot.ts_ms) > config.staleness_ms:
                stale_names.append(name)
                degraded_reasons.append(f"{name.upper()}_STALE")
                if state.last_error:
                    degraded_reasons.append(state.last_error)
                continue

            active_snapshots[name] = snapshot
            if snapshot.funding_rate is not None:
                funding_rates.append(snapshot.funding_rate)

            oi_change = compute_oi_pct_change(state.oi_history, now_ms=cycle_start_ms)
            if oi_change is not None:
                oi_changes_pct.append(oi_change)
                oi_change_map[name] = oi_change

            oi_accel = compute_oi_acceleration(state.oi_history, now_ms=cycle_start_ms)
            if oi_accel is not None:
                oi_accels_pct.append(oi_accel)
                oi_accel_map[name] = oi_accel

        if stale_names:
            health.stale_cycles += 1

        missing_exchanges = [name for name in configured_exchanges if name not in active_snapshots]
        degraded = bool(missing_exchanges)
        if missing_exchanges:
            degraded_reasons.extend(f"{name.upper()}_INACTIVE" for name in missing_exchanges)

        current_price = 0.0
        if "binance" in active_snapshots and active_snapshots["binance"].mark_price is not None:
            current_price = float(active_snapshots["binance"].mark_price)
        elif active_snapshots:
            mark_prices = [snap.mark_price for snap in active_snapshots.values() if snap.mark_price is not None]
            if mark_prices:
                current_price = mean(mark_prices)

        if current_price <= 0:
            if await _sleep_or_stop(stop_event, config.cadence_seconds):
                return
            continue

        prices_1m = [item[1] for item in price_history]
        rv_1h = compute_realized_volatility(prices_1m[-60:])
        ret_5m = compute_return(prices_1m, lookback_points=5)
        regime_long_score, regime_short_score, regime_meta = compute_regime_scores(
            prices=prices_1m[-180:],
            rv_1h=rv_1h,
            ret_5m=ret_5m,
            regime=config.regime,
        )
        adaptive_threshold = compute_adaptive_threshold(
            observed_scores=list(score_history),
            config=config.adaptive_gate,
            base_threshold=config.thresholds.score_threshold,
        )

        liq = book.proximity_scores(current_price=current_price, now_ms=cycle_start_ms)
        oi_div_score, spread = compute_oi_divergence_score(
            oi_changes_pct,
            floor=config.thresholds.oi_div_spread_floor,
            span=config.thresholds.oi_div_spread_span,
        )
        if len(oi_changes_pct) < 2:
            degraded = True
            degraded_reasons.append("INSUFFICIENT_EXCHANGES_FOR_DIVERGENCE")

        funding_long, funding_short, funding_meta = compute_funding_oi_scores(
            funding_rates=funding_rates,
            oi_changes_pct=oi_changes_pct,
            oi_accels_pct=oi_accels_pct,
            rv_1h=rv_1h,
            ret_5m=ret_5m,
            thresholds=config.thresholds,
        )

        breakdown = SignalBreakdown(
            liquidation_long=liq.long_score,
            liquidation_short=liq.short_score,
            funding_oi_long=funding_long,
            funding_oi_short=funding_short,
            oi_divergence=oi_div_score,
        )
        score_long = compute_directional_score(breakdown, "LONG", config.weights)
        score_short = compute_directional_score(breakdown, "SHORT", config.weights)
        long_pass = passes_gate(
            breakdown,
            "LONG",
            score_long,
            config.thresholds,
            score_threshold_override=adaptive_threshold,
        )
        short_pass = passes_gate(
            breakdown,
            "SHORT",
            score_short,
            config.thresholds,
            score_threshold_override=adaptive_threshold,
        )

        if config.regime.enabled:
            if regime_long_score < config.regime.min_score:
                long_pass = False
            if regime_short_score < config.regime.min_score:
                short_pass = False

        if long_pass or short_pass:
            direction = "LONG"
            score = score_long
            secondary = score_short
            if short_pass and score_short > score_long:
                direction = "SHORT"
                score = score_short
                secondary = score_long

            raw_payload = _compose_raw_payload(
                active_exchanges=sorted(active_snapshots.keys()),
                oi_change_map=oi_change_map,
                oi_accel_map=oi_accel_map,
                spread=spread,
                current_price=current_price,
                long_score=score_long,
                short_score=score_short,
                secondary_score=secondary,
                long_distance_pct=liq.long_distance_pct,
                short_distance_pct=liq.short_distance_pct,
                p90_short=liq.short_cluster_p90,
                p90_long=liq.long_cluster_p90,
                funding_meta=funding_meta,
                threshold_score=config.thresholds.score_threshold,
                adaptive_threshold=adaptive_threshold,
                component_threshold=config.thresholds.component_threshold,
                regime_long_score=regime_long_score,
                regime_short_score=regime_short_score,
                regime_meta=regime_meta,
            )
            event = TrapSetupEvent(
                event_type="TRAP_SETUP_EVENT",
                event_id=str(uuid.uuid4()),
                ts_ms=cycle_start_ms,
                symbol=config.symbol,
                direction=direction,
                score=score,
                passed=True,
                components=breakdown,
                raw=raw_payload,
                degraded=degraded,
                degrade_reason="|".join(sorted(set(degraded_reasons))) if degraded_reasons else None,
            )
            await _emit_with_drop_oldest(out_queue, event, health)
            health.mark_emitted(cycle_start_ms)

        score_history.append(max(score_long, score_short))

        if await _sleep_or_stop(stop_event, config.cadence_seconds):
            return


async def run_layer0(
    config: Layer0Config,
    out_queue: asyncio.Queue[TrapSetupEvent],
    *,
    stop_event: asyncio.Event | None = None,
    clients: dict[str, ExchangeClient] | None = None,
    health: HealthCounters | None = None,
) -> None:
    """
    Run Layer 0 trap setup detection until cancelled or stop_event is set.
    """
    own_stop_event = stop_event is None
    if stop_event is None:
        stop_event = asyncio.Event()
    if health is None:
        health = HealthCounters()

    book = LiquidationBook(
        window_minutes=config.cluster_window_minutes,
        bin_size=config.cluster_bin_size,
        decay_minutes=config.cluster_decay_minutes,
    )
    price_history: deque[tuple[int, float]] = deque()

    async def _run_with_clients(client_map: dict[str, ExchangeClient]) -> None:
        states: dict[str, _ExchangeState] = {name: _ExchangeState() for name in client_map}
        tasks: list[asyncio.Task[Any]] = []
        for client in client_map.values():
            state = states[client.name]
            tasks.append(
                asyncio.create_task(
                    _snapshot_poller(
                        client=client,
                        config=config,
                        state=state,
                        stop_event=stop_event,
                        health=health,
                        price_history=price_history,
                    ),
                    name=f"{client.name}-snapshot-poller",
                )
            )
            if client.name != "okx" or config.enable_okx_liquidations:
                tasks.append(
                    asyncio.create_task(
                        _liquidation_worker(
                            client=client,
                            config=config,
                            book=book,
                            stop_event=stop_event,
                            health=health,
                        ),
                        name=f"{client.name}-liquidation-worker",
                    )
                )

        tasks.append(
            asyncio.create_task(
                _scoring_loop(
                    config=config,
                    states=states,
                    book=book,
                    out_queue=out_queue,
                    stop_event=stop_event,
                    health=health,
                    price_history=price_history,
                ),
                name="layer0-scoring-loop",
            )
        )

        try:
            await stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    if clients is not None:
        await _run_with_clients(clients)
        return

    try:
        import aiohttp  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("aiohttp is required when using default exchange clients") from exc

    async with aiohttp.ClientSession() as session:
        default_clients = _build_default_clients(session, config)
        try:
            await _run_with_clients(default_clients)
        except asyncio.CancelledError:
            if own_stop_event:
                stop_event.set()
            raise
        finally:
            for client in default_clients.values():
                with contextlib.suppress(Exception):
                    await client.close()
