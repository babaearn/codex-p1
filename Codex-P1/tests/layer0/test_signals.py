from __future__ import annotations

import pytest

from project_phantom.config import Layer0Config, SignalWeights, ThresholdConfig
from project_phantom.core.types import LiquidationUpdate, SignalBreakdown
from project_phantom.layer0.liquidation_book import LiquidationBook
from project_phantom.layer0.signals import (
    compute_adaptive_threshold,
    compute_directional_score,
    compute_funding_oi_scores,
    compute_oi_divergence_score,
    compute_regime_scores,
    passes_gate,
)


def test_liquidation_proximity_scores_mixed_events() -> None:
    book = LiquidationBook(window_minutes=90, bin_size=100.0, decay_minutes=45.0)
    now_ms = 1_000_000

    # Above price short liquidations should support LONG setup score.
    book.add(
        event=LiquidationUpdate(
            exchange="binance",
            symbol="BTCUSDT",
            price=10_120.0,
            quantity=10.0,
            notional=101_200.0,
            liquidated_side="SHORT",
            ts_ms=now_ms,
        )
    )
    # Below price long liquidations should support SHORT setup score.
    book.add(
        event=LiquidationUpdate(
            exchange="binance",
            symbol="BTCUSDT",
            price=9_880.0,
            quantity=8.0,
            notional=79_040.0,
            liquidated_side="LONG",
            ts_ms=now_ms,
        )
    )
    # Stale event outside 90-minute window should be ignored.
    book.add(
        event=LiquidationUpdate(
            exchange="bybit",
            symbol="BTCUSDT",
            price=10_500.0,
            quantity=20.0,
            notional=210_000.0,
            liquidated_side="SHORT",
            ts_ms=now_ms - 6_000_000,
        )
    )

    prox = book.proximity_scores(current_price=10_000.0, now_ms=now_ms)
    assert prox.long_score > 0.0
    assert prox.short_score > 0.0
    assert prox.long_distance_pct is not None
    assert prox.short_distance_pct is not None


def test_funding_oi_regime_switch_prefers_long_on_negative_funding() -> None:
    thresholds = ThresholdConfig()
    low_long, low_short, low_meta = compute_funding_oi_scores(
        funding_rates=[-0.001],
        oi_changes_pct=[2.0],
        oi_accels_pct=[0.5],
        rv_1h=0.004,
        ret_5m=0.0,
        thresholds=thresholds,
    )
    high_long, high_short, high_meta = compute_funding_oi_scores(
        funding_rates=[-0.001],
        oi_changes_pct=[2.0],
        oi_accels_pct=[0.5],
        rv_1h=0.02,
        ret_5m=0.0,
        thresholds=thresholds,
    )

    assert low_meta["regime"] == "LOW_VOL"
    assert high_meta["regime"] == "HIGH_VOL"
    assert low_long > low_short
    assert high_long > high_short


def test_oi_divergence_score_across_cardinality() -> None:
    score_three, spread_three = compute_oi_divergence_score([1.0, -1.0, 0.2], floor=0.4, span=1.6)
    score_two, spread_two = compute_oi_divergence_score([0.5, 0.4], floor=0.4, span=1.6)
    score_one, spread_one = compute_oi_divergence_score([0.5], floor=0.4, span=1.6)

    assert spread_three == pytest.approx(2.0)
    assert score_three == pytest.approx(1.0)
    assert spread_two == pytest.approx(0.1)
    assert score_two == pytest.approx(0.0)
    assert spread_one == pytest.approx(0.0)
    assert score_one == pytest.approx(0.0)


def test_gate_edges() -> None:
    thresholds = ThresholdConfig()
    weights = SignalWeights()
    passing = SignalBreakdown(
        liquidation_long=1.0,
        liquidation_short=0.2,
        funding_oi_long=1.0,
        funding_oi_short=0.1,
        oi_divergence=0.0,
    )
    failing = SignalBreakdown(
        liquidation_long=0.95,
        liquidation_short=0.1,
        funding_oi_long=0.4,
        funding_oi_short=0.2,
        oi_divergence=0.35,
    )

    passing_score = compute_directional_score(passing, "LONG", weights)
    failing_score = compute_directional_score(failing, "LONG", weights)
    assert passing_score == pytest.approx(0.7)
    assert passing_score >= thresholds.score_threshold
    assert passes_gate(passing, "LONG", passing_score, thresholds)
    assert failing_score < thresholds.score_threshold
    assert not passes_gate(failing, "LONG", failing_score, thresholds)


def test_regime_scores_prefer_trend_direction() -> None:
    prices = [100.0 + idx * 0.2 for idx in range(90)]
    long_score, short_score, meta = compute_regime_scores(
        prices=prices,
        rv_1h=0.01,
        ret_5m=0.002,
        regime=Layer0Config().regime,
    )
    assert meta["regime_state"] == "NORMAL"
    assert long_score > short_score
    assert long_score > 0.5


def test_adaptive_threshold_respects_floor_and_quantile() -> None:
    config = Layer0Config().adaptive_gate
    observed = [0.50] * 25 + [0.72] * 15 + [0.88] * 60
    dynamic = compute_adaptive_threshold(
        observed_scores=observed,
        config=config,
        base_threshold=0.70,
    )
    assert dynamic >= 0.70
    assert dynamic <= config.ceiling
