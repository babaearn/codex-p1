from __future__ import annotations

import pytest

from project_phantom.config import Layer1ThresholdConfig, Layer1Weights
from project_phantom.core.types import AbsorptionBreakdown, TradeTick
from project_phantom.layer1.metrics import (
    compute_absorption_score,
    compute_cvd_scores,
    compute_twap_uniformity_scores,
    compute_whale_net_flow_scores,
    passes_absorption_gate,
)


def _trade(ts_ms: int, *, price: float, qty: float, is_buyer_maker: bool) -> TradeTick:
    return TradeTick(
        exchange="binance",
        symbol="BTCUSDT",
        price=price,
        quantity=qty,
        is_buyer_maker=is_buyer_maker,
        ts_ms=ts_ms,
    )


def test_whale_net_flow_scores_directional() -> None:
    trades = [
        _trade(1, price=10_000, qty=20, is_buyer_maker=False),
        _trade(2, price=10_000, qty=15, is_buyer_maker=False),
        _trade(3, price=10_000, qty=5, is_buyer_maker=True),
    ]
    long_score, short_score, net_flow = compute_whale_net_flow_scores(
        trades=trades,
        min_notional=100_000,
        scale_usd=200_000,
    )
    assert net_flow > 0
    assert long_score > 0
    assert short_score == pytest.approx(0.0)


def test_twap_uniformity_scores_prefers_uniform_buy_program() -> None:
    trades = [
        _trade(1_000, price=10_000, qty=20, is_buyer_maker=False),
        _trade(2_000, price=10_010, qty=20, is_buyer_maker=False),
        _trade(3_000, price=10_020, qty=20, is_buyer_maker=False),
        _trade(4_000, price=10_030, qty=20, is_buyer_maker=False),
    ]
    long_score, short_score, cv, whale_count = compute_twap_uniformity_scores(
        trades=trades,
        min_notional=100_000,
        cv_limit=0.35,
    )
    assert whale_count == 4
    assert cv is not None
    assert long_score > 0.9
    assert short_score == pytest.approx(0.0)


def test_cvd_hidden_divergence_sets_long_flag() -> None:
    trades = [
        _trade(1_000, price=10_000, qty=10, is_buyer_maker=True),
        _trade(2_000, price=10_050, qty=10, is_buyer_maker=True),
        _trade(3_000, price=10_100, qty=10, is_buyer_maker=True),
    ]
    long_score, short_score, cvd_delta, price_delta_pct, hidden_long, hidden_short = compute_cvd_scores(
        trades=trades,
        cvd_scale_usd=2_000_000,
    )
    assert cvd_delta < 0
    assert price_delta_pct > 0
    assert hidden_long is True
    assert hidden_short is False
    assert long_score >= 0.6
    assert short_score > 0


def test_absorption_gate_threshold() -> None:
    thresholds = Layer1ThresholdConfig()
    weights = Layer1Weights()
    breakdown = AbsorptionBreakdown(
        whale_net_flow_long=1.0,
        whale_net_flow_short=0.0,
        twap_uniformity_long=1.0,
        twap_uniformity_short=0.0,
        cvd_long=1.0,
        cvd_short=0.0,
        stablecoin_inflow=0.0,
        hidden_divergence_long=False,
        hidden_divergence_short=False,
    )
    score = compute_absorption_score(breakdown, direction="LONG", weights=weights)
    assert score > thresholds.score_threshold
    assert passes_absorption_gate(breakdown, "LONG", score, thresholds)
