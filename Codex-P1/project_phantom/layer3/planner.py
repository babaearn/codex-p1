from __future__ import annotations

from typing import Any

from project_phantom.config import Layer3RiskConfig, Layer3SizingConfig
from project_phantom.core.types import Direction, ExecutionPlan, PrePumpEvent


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _nested_float(mapping: dict[str, Any], *keys: str) -> float | None:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return _to_float(current)


def _source_raw(event: PrePumpEvent) -> dict[str, Any]:
    source_absorption_raw = event.raw.get("source_absorption_raw")
    if not isinstance(source_absorption_raw, dict):
        return {}
    source_trap_raw = source_absorption_raw.get("source_trap_raw")
    if isinstance(source_trap_raw, dict):
        return source_trap_raw
    return {}


def derive_entry_price(event: PrePumpEvent) -> float | None:
    src = _source_raw(event)
    for candidate in (
        _nested_float(event.raw, "entry"),
        _nested_float(event.raw, "current_price"),
        _nested_float(event.raw, "source_absorption_raw", "current_price"),
        _nested_float(src, "current_price"),
    ):
        if candidate is not None and candidate > 0:
            return candidate
    return None


def build_execution_plan(
    event: PrePumpEvent,
    *,
    entry_price: float,
    quantity: float,
    risk_config: Layer3RiskConfig,
) -> ExecutionPlan:
    direction: Direction = event.direction
    src = _source_raw(event)

    zone_low = _nested_float(event.raw, "swept_liquidation_zone_low")
    zone_high = _nested_float(event.raw, "swept_liquidation_zone_high")
    if zone_low is None:
        zone_low = _nested_float(src, "swept_liquidation_zone_low")
    if zone_high is None:
        zone_high = _nested_float(src, "swept_liquidation_zone_high")

    ob_above = _nested_float(event.raw, "nearest_ob_above")
    ob_below = _nested_float(event.raw, "nearest_ob_below")
    if ob_above is None:
        ob_above = _nested_float(src, "nearest_ob_above")
    if ob_below is None:
        ob_below = _nested_float(src, "nearest_ob_below")

    if direction == "LONG":
        sl = zone_low if zone_low is not None and zone_low < entry_price else entry_price * (1 - risk_config.default_sl_buffer_pct)
        risk = max(entry_price - sl, entry_price * 0.0005)
        tp1 = ob_above if ob_above is not None and ob_above > entry_price else entry_price + (risk_config.tp1_r_multiple * risk)
        tp2 = entry_price + (risk_config.tp2_r_multiple * risk)
        tp1 = max(tp1, entry_price + risk * 0.8)
        if tp1 >= tp2:
            tp1 = entry_price + (risk_config.tp1_r_multiple * risk)
        sl_pct = (sl / entry_price) - 1.0
        tp1_pct = (tp1 / entry_price) - 1.0
        tp2_pct = (tp2 / entry_price) - 1.0
    else:
        sl = zone_high if zone_high is not None and zone_high > entry_price else entry_price * (1 + risk_config.default_sl_buffer_pct)
        risk = max(sl - entry_price, entry_price * 0.0005)
        tp1 = ob_below if ob_below is not None and ob_below < entry_price else entry_price - (risk_config.tp1_r_multiple * risk)
        tp2 = entry_price - (risk_config.tp2_r_multiple * risk)
        tp1 = min(tp1, entry_price - risk * 0.8)
        if tp1 <= tp2:
            tp1 = entry_price - (risk_config.tp1_r_multiple * risk)
        sl_pct = (entry_price / sl) - 1.0
        tp1_pct = (entry_price / tp1) - 1.0
        tp2_pct = (entry_price / tp2) - 1.0

    rr = abs((tp2 - entry_price) / (entry_price - sl)) if entry_price != sl else risk_config.tp2_r_multiple
    risk_amount = abs(entry_price - sl) * quantity

    return ExecutionPlan(
        entry=round(entry_price, 2),
        sl=round(sl, 2),
        tp1=round(tp1, 2),
        tp2=round(tp2, 2),
        rr=round(rr, 2),
        sl_pct=sl_pct,
        tp1_pct=tp1_pct,
        tp2_pct=tp2_pct,
        quantity=quantity,
        risk_amount=risk_amount,
    )


def derive_adaptive_quantity(
    event: PrePumpEvent,
    *,
    base_quantity: float,
    sizing: Layer3SizingConfig,
) -> tuple[float, float]:
    if base_quantity <= 0:
        return (0.0, 0.0)
    if not sizing.enabled:
        return (base_quantity, 0.0)

    confirmations = max(0, int(event.components.confirmations))
    confirmations_score = min(confirmations / 5.0, 1.0)
    signal_score = max(0.0, min(float(event.score), 1.0))
    trap_score = _nested_float(event.raw, "source_absorption_raw", "source_trap_score")
    if trap_score is None:
        trap_score = signal_score
    trap_score = max(0.0, min(float(trap_score), 1.0))

    regime_key = "regime_long_score" if event.direction == "LONG" else "regime_short_score"
    regime_score = _nested_float(event.raw, "source_absorption_raw", "source_trap_raw", regime_key)
    if regime_score is None:
        regime_score = 0.5
    regime_score = max(0.0, min(float(regime_score), 1.0))

    confidence = (0.35 * signal_score) + (0.30 * confirmations_score) + (0.20 * trap_score) + (0.15 * regime_score)
    floor = max(0.0, min(0.95, sizing.confidence_floor))
    if confidence <= floor:
        normalized = 0.0
    else:
        normalized = min((confidence - floor) / max(1e-9, 1.0 - floor), 1.0)

    multiplier = sizing.min_multiplier + (sizing.max_multiplier - sizing.min_multiplier) * normalized
    if event.degraded:
        multiplier *= 0.7
    quantity = max(base_quantity * multiplier, base_quantity * 0.1)
    return (round(quantity, 6), confidence)
