from __future__ import annotations

from project_phantom.config import Layer2ThresholdConfig
from project_phantom.core.types import AbsorptionEvent, Candle, Direction, IgnitionBreakdown


def _safe_source_trap_score(event: AbsorptionEvent) -> float:
    value = event.raw.get("source_trap_score")
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _momentum_signal(
    candles: list[Candle],
    direction: Direction,
    lookback_bars: int,
    min_return_pct: float,
) -> tuple[bool, float]:
    if len(candles) <= lookback_bars:
        return (False, 0.0)

    latest = candles[-1].close
    base = candles[-1 - lookback_bars].close
    if base <= 0:
        return (False, 0.0)
    ret_pct = (latest / base) - 1.0
    if direction == "LONG":
        return (ret_pct >= min_return_pct, ret_pct)
    return (ret_pct <= -min_return_pct, ret_pct)


def build_ignition_breakdown(
    *,
    absorption_event: AbsorptionEvent,
    candles: list[Candle],
    direction: Direction,
    choch_signal: bool,
    order_block_signal: bool,
    thresholds: Layer2ThresholdConfig,
) -> tuple[IgnitionBreakdown, dict[str, float]]:
    absorption_strength = absorption_event.score >= thresholds.absorption_score_min
    trap_score = _safe_source_trap_score(absorption_event)
    trap_strength = trap_score >= thresholds.trap_score_min
    momentum_signal, momentum_return = _momentum_signal(
        candles=candles,
        direction=direction,
        lookback_bars=thresholds.momentum_lookback_bars,
        min_return_pct=thresholds.momentum_min_return_pct,
    )

    confirmations = sum(
        [
            choch_signal,
            order_block_signal,
            absorption_strength,
            trap_strength,
            momentum_signal,
        ]
    )

    return (
        IgnitionBreakdown(
            choch=choch_signal,
            order_block=order_block_signal,
            absorption_strength=absorption_strength,
            trap_strength=trap_strength,
            momentum=momentum_signal,
            confirmations=confirmations,
        ),
        {
            "source_trap_score": trap_score,
            "momentum_return_pct": momentum_return,
        },
    )
