from __future__ import annotations

from project_phantom.config import Layer2ThresholdConfig
from project_phantom.core.types import AbsorptionBreakdown, AbsorptionEvent, Candle
from project_phantom.layer2.signals import build_ignition_breakdown


def _absorption_event(score: float, trap_score: float) -> AbsorptionEvent:
    return AbsorptionEvent(
        event_type="ABSORPTION_EVENT",
        event_id="abs-1",
        ts_ms=1_000_000,
        symbol="BTCUSDT",
        direction="LONG",
        score=score,
        passed=True,
        source_trap_event_id="trap-1",
        components=AbsorptionBreakdown(
            whale_net_flow_long=1.0,
            whale_net_flow_short=0.0,
            twap_uniformity_long=0.8,
            twap_uniformity_short=0.0,
            cvd_long=1.0,
            cvd_short=0.0,
            stablecoin_inflow=0.0,
            hidden_divergence_long=False,
            hidden_divergence_short=False,
        ),
        raw={"source_trap_score": trap_score},
        degraded=False,
    )


def _candles_uptrend() -> list[Candle]:
    base = 1_000_000
    candles: list[Candle] = []
    for idx in range(8):
        price = 10_000 + (idx * 20)
        candles.append(
            Candle(
                open_time_ms=base + idx * 60_000,
                open=price,
                high=price + 30,
                low=price - 20,
                close=price + 10,
                volume=100.0,
                close_time_ms=base + (idx + 1) * 60_000 - 1,
            )
        )
    return candles


def test_build_ignition_breakdown_hits_all_confirmations() -> None:
    thresholds = Layer2ThresholdConfig(
        min_confirmations=3,
        absorption_score_min=0.6,
        trap_score_min=0.7,
        momentum_lookback_bars=5,
        momentum_min_return_pct=0.001,
    )
    breakdown, derived = build_ignition_breakdown(
        absorption_event=_absorption_event(score=0.8, trap_score=0.9),
        candles=_candles_uptrend(),
        direction="LONG",
        choch_signal=True,
        order_block_signal=True,
        thresholds=thresholds,
    )
    assert breakdown.confirmations == 5
    assert breakdown.momentum is True
    assert derived["source_trap_score"] == 0.9


def test_build_ignition_breakdown_respects_short_momentum_direction() -> None:
    thresholds = Layer2ThresholdConfig(momentum_lookback_bars=5, momentum_min_return_pct=0.001)
    candles = list(reversed(_candles_uptrend()))
    breakdown, _ = build_ignition_breakdown(
        absorption_event=AbsorptionEvent(
            event_type="ABSORPTION_EVENT",
            event_id="abs-2",
            ts_ms=1_000_000,
            symbol="BTCUSDT",
            direction="SHORT",
            score=0.8,
            passed=True,
            source_trap_event_id="trap-2",
            components=_absorption_event(0.8, 0.9).components,
            raw={"source_trap_score": 0.9},
            degraded=False,
        ),
        candles=candles,
        direction="SHORT",
        choch_signal=False,
        order_block_signal=False,
        thresholds=thresholds,
    )
    assert breakdown.momentum is True
