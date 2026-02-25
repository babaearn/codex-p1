from __future__ import annotations

from project_phantom.config import Layer3RiskConfig
from project_phantom.core.types import IgnitionBreakdown, PrePumpEvent
from project_phantom.layer3.planner import build_execution_plan, derive_entry_price


def _event(direction: str = "LONG") -> PrePumpEvent:
    return PrePumpEvent(
        event_type="PRE_PUMP_EVENT",
        event_id="pre-1",
        ts_ms=1_000_000,
        symbol="BTCUSDT",
        direction=direction,  # type: ignore[arg-type]
        score=0.8,
        passed=True,
        source_absorption_event_id="abs-1",
        source_trap_event_id="trap-1",
        components=IgnitionBreakdown(
            choch=True,
            order_block=True,
            absorption_strength=True,
            trap_strength=True,
            momentum=False,
            confirmations=4,
        ),
        raw={
            "source_absorption_raw": {
                "current_price": 62_959,
                "source_trap_raw": {
                    "swept_liquidation_zone_low": 62_680,
                    "swept_liquidation_zone_high": 63_200,
                    "nearest_ob_above": 64_200,
                    "nearest_ob_below": 61_900,
                },
            }
        },
        degraded=False,
    )


def test_derive_entry_price_from_nested_raw() -> None:
    value = derive_entry_price(_event("LONG"))
    assert value == 62_959


def test_build_execution_plan_long_uses_zone_low_and_tp2_2_5r() -> None:
    event = _event("LONG")
    plan = build_execution_plan(
        event,
        entry_price=62_959,
        quantity=0.01,
        risk_config=Layer3RiskConfig(default_sl_buffer_pct=0.0044, tp1_r_multiple=1.5, tp2_r_multiple=2.5),
    )
    assert plan.sl == 62_680.0
    expected_tp2 = round(62_959 + ((62_959 - 62_680) * 2.5), 2)
    assert plan.tp2 == expected_tp2
    assert plan.tp1 > plan.entry
    assert plan.tp1 < plan.tp2


def test_build_execution_plan_short_uses_zone_high() -> None:
    event = _event("SHORT")
    plan = build_execution_plan(
        event,
        entry_price=62_959,
        quantity=0.01,
        risk_config=Layer3RiskConfig(),
    )
    assert plan.sl == 63_200.0
    assert plan.tp2 < plan.entry
