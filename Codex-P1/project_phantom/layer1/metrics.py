from __future__ import annotations

import math
from typing import Sequence

from project_phantom.config import Layer1ThresholdConfig, Layer1Weights
from project_phantom.core.types import AbsorptionBreakdown, Direction, TradeTick


def clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def signed_notional(trade: TradeTick) -> float:
    # Binance `is_buyer_maker=True` means seller was aggressive (sell pressure).
    return -trade.notional if trade.is_buyer_maker else trade.notional


def compute_whale_net_flow_scores(
    trades: Sequence[TradeTick],
    min_notional: float,
    scale_usd: float,
) -> tuple[float, float, float]:
    whale_trades = [trade for trade in trades if trade.notional >= min_notional]
    net_flow = sum(signed_notional(trade) for trade in whale_trades)
    long_score = clamp(max(net_flow, 0.0) / scale_usd) if scale_usd > 0 else 0.0
    short_score = clamp(max(-net_flow, 0.0) / scale_usd) if scale_usd > 0 else 0.0
    return (long_score, short_score, net_flow)


def compute_twap_uniformity_scores(
    trades: Sequence[TradeTick],
    min_notional: float,
    cv_limit: float,
) -> tuple[float, float, float | None, int]:
    whale_trades = [trade for trade in trades if trade.notional >= min_notional]
    if len(whale_trades) < 3:
        return (0.0, 0.0, None, len(whale_trades))

    ordered = sorted(whale_trades, key=lambda row: row.ts_ms)
    intervals = [
        (ordered[idx].ts_ms - ordered[idx - 1].ts_ms) / 1000.0
        for idx in range(1, len(ordered))
    ]
    if not intervals:
        return (0.0, 0.0, None, len(whale_trades))

    mean_interval = sum(intervals) / len(intervals)
    if mean_interval <= 0:
        return (0.0, 0.0, None, len(whale_trades))

    variance = sum((value - mean_interval) ** 2 for value in intervals) / len(intervals)
    cv = math.sqrt(variance) / mean_interval

    if cv_limit <= 0:
        uniformity = 0.0
    else:
        uniformity = clamp(1.0 - (cv / cv_limit))

    buy_aggressive = sum(1 for trade in whale_trades if not trade.is_buyer_maker)
    sell_aggressive = len(whale_trades) - buy_aggressive
    if len(whale_trades) <= 0:
        return (0.0, 0.0, cv, len(whale_trades))

    long_score = uniformity * (buy_aggressive / len(whale_trades))
    short_score = uniformity * (sell_aggressive / len(whale_trades))
    return (long_score, short_score, cv, len(whale_trades))


def compute_cvd_scores(
    trades: Sequence[TradeTick],
    cvd_scale_usd: float,
) -> tuple[float, float, float, float, bool, bool]:
    if len(trades) < 2:
        return (0.0, 0.0, 0.0, 0.0, False, False)

    ordered = sorted(trades, key=lambda row: row.ts_ms)
    cvd_delta = sum(signed_notional(trade) for trade in ordered)
    start_price = ordered[0].price
    end_price = ordered[-1].price
    if start_price <= 0:
        price_delta_pct = 0.0
    else:
        price_delta_pct = (end_price / start_price) - 1.0

    long_score = clamp(max(cvd_delta, 0.0) / cvd_scale_usd) if cvd_scale_usd > 0 else 0.0
    short_score = clamp(max(-cvd_delta, 0.0) / cvd_scale_usd) if cvd_scale_usd > 0 else 0.0

    hidden_long = price_delta_pct > 0 and cvd_delta < 0
    hidden_short = price_delta_pct < 0 and cvd_delta > 0

    if hidden_long:
        long_score = max(long_score, 0.6)
    if hidden_short:
        short_score = max(short_score, 0.6)

    return (long_score, short_score, cvd_delta, price_delta_pct, hidden_long, hidden_short)


def compute_stablecoin_inflow_score(inflow_usd: float, scale_usd: float) -> float:
    if scale_usd <= 0:
        return 0.0
    return clamp(max(inflow_usd, 0.0) / scale_usd)


def compute_absorption_score(
    breakdown: AbsorptionBreakdown,
    direction: Direction,
    weights: Layer1Weights,
) -> float:
    whale_flow, twap, cvd, stablecoin = breakdown.score_components(direction)
    score = (
        weights.whale_net_flow * whale_flow
        + weights.twap_uniformity * twap
        + weights.cvd * cvd
        + weights.stablecoin_inflow * stablecoin
    )
    if breakdown.hidden_divergence_for(direction):
        score += 0.1
    return clamp(score)


def passes_absorption_gate(
    breakdown: AbsorptionBreakdown,
    direction: Direction,
    score: float,
    thresholds: Layer1ThresholdConfig,
) -> bool:
    if score < thresholds.score_threshold:
        return False

    components = breakdown.score_components(direction)
    hits = sum(value >= thresholds.component_threshold for value in components)
    if breakdown.hidden_divergence_for(direction):
        hits += 1
    return hits >= thresholds.min_component_hits
