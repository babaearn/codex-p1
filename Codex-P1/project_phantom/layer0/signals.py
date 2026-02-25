from __future__ import annotations

import math
from collections import deque
from statistics import mean
from typing import Sequence

from project_phantom.config import AdaptiveGateConfig, RegimeFilterConfig, SignalWeights, ThresholdConfig
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


def compute_ema(prices: Sequence[float], span: int) -> float:
    if not prices:
        return 0.0
    if span <= 1:
        return float(prices[-1])
    alpha = 2.0 / (span + 1.0)
    value = float(prices[0])
    for price in prices[1:]:
        value = alpha * float(price) + (1.0 - alpha) * value
    return value


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


def compute_regime_scores(
    prices: Sequence[float],
    rv_1h: float,
    ret_5m: float,
    regime: RegimeFilterConfig,
) -> tuple[float, float, dict[str, float | str]]:
    if not prices:
        return (
            0.0,
            0.0,
            {
                "regime_state": "NO_PRICE",
                "trend_gap_pct": 0.0,
                "volatility_gate": 0.0,
            },
        )

    if len(prices) < max(regime.trend_ema_slow, 8):
        return (
            0.5,
            0.5,
            {
                "regime_state": "INSUFFICIENT_HISTORY",
                "trend_gap_pct": 0.0,
                "volatility_gate": 0.5,
            },
        )

    last_price = float(prices[-1])
    if last_price <= 0:
        return (
            0.0,
            0.0,
            {
                "regime_state": "BAD_PRICE",
                "trend_gap_pct": 0.0,
                "volatility_gate": 0.0,
            },
        )

    ema_fast = compute_ema(prices[-max(regime.trend_ema_fast * 3, regime.trend_ema_slow + 5) :], regime.trend_ema_fast)
    ema_slow = compute_ema(prices[-max(regime.trend_ema_slow * 3, regime.trend_ema_slow + 5) :], regime.trend_ema_slow)
    trend_gap_pct = (ema_fast - ema_slow) / last_price

    trend_long = clamp(0.5 + (trend_gap_pct / max(regime.trend_gap_scale_pct, 1e-9)) * 0.5)
    trend_short = clamp(1.0 - trend_long)

    compression = clamp(1.0 - (abs(ret_5m) / max(regime.max_abs_ret_5m, 1e-9)))
    if rv_1h >= regime.panic_vol_cutoff:
        volatility_gate = 0.0
        regime_state = "PANIC_VOL"
    else:
        volatility_gate = clamp(1.0 - (rv_1h / max(regime.panic_vol_cutoff, 1e-9)))
        regime_state = "NORMAL"

    long_score = clamp(0.6 * trend_long + 0.4 * (compression * volatility_gate))
    short_score = clamp(0.6 * trend_short + 0.4 * (compression * volatility_gate))
    return (
        long_score,
        short_score,
        {
            "regime_state": regime_state,
            "ema_fast": ema_fast,
            "ema_slow": ema_slow,
            "trend_gap_pct": trend_gap_pct,
            "volatility_gate": volatility_gate,
            "compression_regime": compression,
        },
    )


def compute_adaptive_threshold(
    observed_scores: Sequence[float],
    config: AdaptiveGateConfig,
    base_threshold: float,
) -> float:
    if not config.enabled:
        return base_threshold

    if len(observed_scores) < max(1, config.min_samples):
        return base_threshold

    ranked = sorted(float(item) for item in observed_scores)
    quantile = clamp(config.quantile, 0.0, 1.0)
    idx = int(round((len(ranked) - 1) * quantile))
    dynamic = ranked[idx]
    lower = max(base_threshold, config.floor)
    upper = config.ceiling
    if upper < lower:
        upper = lower
    return clamp(dynamic, lower, upper)


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
    score_threshold_override: float | None = None,
) -> bool:
    active_threshold = threshold.score_threshold if score_threshold_override is None else score_threshold_override
    if score < active_threshold:
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
