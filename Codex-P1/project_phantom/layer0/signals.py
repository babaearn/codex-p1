from __future__ import annotations

import math
from collections import deque
from statistics import mean
from typing import Sequence

from project_phantom.config import SignalWeights, ThresholdConfig
from project_phantom.core.types import Direction, OIObservation, SignalBreakdown


def clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def compute_realized_volatility(prices: Sequence[float]) -> float:
    if len(prices) < 2:
        return 0.0
    log_returns = [math.log(prices[idx] / prices[idx - 1]) for idx in range(1, len(prices)) if prices[idx - 1] > 0]
    if not log_returns:
        return 0.0
    return math.sqrt(sum(r * r for r in log_returns))


def compute_return(prices: Sequence[float], lookback_points: int) -> float:
    if len(prices) <= lookback_points or prices[-lookback_points - 1] <= 0:
        return 0.0
    base = prices[-lookback_points - 1]
    return (prices[-1] / base) - 1.0


def _latest_before(history: Sequence[OIObservation], ts_ms: int) -> OIObservation | None:
    candidate: OIObservation | None = None
    for row in history:
        if row.ts_ms <= ts_ms:
            candidate = row
        else:
            break
    return candidate


def compute_oi_pct_change(history: Sequence[OIObservation], now_ms: int, window_ms: int = 300_000) -> float | None:
    if len(history) < 2:
        return None
    latest = history[-1]
    reference = _latest_before(history, now_ms - window_ms)
    if reference is None and history:
        reference = history[0]
    if reference is None or reference.open_interest <= 0:
        return None
    return ((latest.open_interest / reference.open_interest) - 1.0) * 100.0


def compute_oi_acceleration(history: Sequence[OIObservation], now_ms: int) -> float | None:
    current = compute_oi_pct_change(history, now_ms=now_ms, window_ms=300_000)
    prior = compute_oi_pct_change(history, now_ms=now_ms - 300_000, window_ms=300_000)
    if current is None or prior is None:
        return None
    return current - prior


def compute_oi_divergence_score(
    oi_changes_pct: Sequence[float],
    floor: float,
    span: float,
) -> tuple[float, float]:
    if len(oi_changes_pct) < 2:
        return (0.0, 0.0)
    spread = max(oi_changes_pct) - min(oi_changes_pct)
    score = clamp((spread - floor) / span)
    return (score, spread)


def compute_funding_oi_scores(
    funding_rates: Sequence[float],
    oi_changes_pct: Sequence[float],
    oi_accels_pct: Sequence[float],
    rv_1h: float,
    ret_5m: float,
    thresholds: ThresholdConfig,
) -> tuple[float, float, dict[str, float | str]]:
    avg_funding = mean(funding_rates) if funding_rates else 0.0
    avg_oi_change = mean(oi_changes_pct) if oi_changes_pct else 0.0
    avg_oi_accel = mean(oi_accels_pct) if oi_accels_pct else 0.0

    funding_long = clamp(-avg_funding / thresholds.funding_scale)
    funding_short = clamp(avg_funding / thresholds.funding_scale)
    oi_rise = clamp(avg_oi_change / thresholds.oi_pct_scale)
    oi_accel = clamp(avg_oi_accel / thresholds.oi_accel_scale)

    compression = clamp(1.0 - (abs(ret_5m) / thresholds.compression_return_cap))
    regime = "LOW_VOL" if rv_1h < thresholds.rv_low_vol_threshold else "HIGH_VOL"

    if regime == "LOW_VOL":
        long_score = (0.65 * funding_long + 0.35 * oi_rise) * compression
        short_score = (0.65 * funding_short + 0.35 * oi_rise) * compression
    else:
        oi_drive = max(oi_rise, oi_accel)
        long_score = (0.35 * funding_long + 0.65 * oi_drive) * compression
        short_score = (0.35 * funding_short + 0.65 * oi_drive) * compression

    return (
        clamp(long_score),
        clamp(short_score),
        {
            "avg_funding": avg_funding,
            "avg_oi_change_pct": avg_oi_change,
            "avg_oi_accel_pct": avg_oi_accel,
            "rv_1h": rv_1h,
            "ret_5m": ret_5m,
            "compression": compression,
            "regime": regime,
        },
    )


def compute_directional_score(
    breakdown: SignalBreakdown,
    direction: Direction,
    weights: SignalWeights,
) -> float:
    liq, fund_oi, oi_div = breakdown.for_direction(direction)
    return (
        weights.liquidation * liq
        + weights.funding_oi * fund_oi
        + weights.oi_divergence * oi_div
    )


def passes_gate(
    breakdown: SignalBreakdown,
    direction: Direction,
    score: float,
    threshold: ThresholdConfig,
) -> bool:
    if score < threshold.score_threshold:
        return False
    components = breakdown.for_direction(direction)
    passing_components = sum(item >= threshold.component_threshold for item in components)
    return passing_components >= 2


def has_warmup_window(history_map: dict[str, deque[OIObservation]], now_ms: int, warmup_ms: int) -> bool:
    if not history_map:
        return False
    for history in history_map.values():
        if history and (now_ms - history[0].ts_ms) >= warmup_ms:
            return True
    return False
