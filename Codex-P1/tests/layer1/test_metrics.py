from __future__ import annotations

import pytest

from project_phantom.config import Layer1ThresholdConfig, Layer1Weights
from project_phantom.core.types import AbsorptionBreakdown, OrderBookTick, TradeTick
from project_phantom.layer1.metrics import (
    compute_absorption_score,
    compute_cvd_scores,
    compute_orderbook_imbalance_scores,
    compute_sweep_aggression_scores,
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
        orderbook_imbalance_long=0.8,
        orderbook_imbalance_short=0.1,
        sweep_aggression_long=0.7,
        sweep_aggression_short=0.2,
        hidden_divergence_long=False,
        hidden_divergence_short=False,
    )
    score = compute_absorption_score(breakdown, direction="LONG", weights=weights)
    assert score > thresholds.score_threshold
    assert passes_absorption_gate(breakdown, "LONG", score, thresholds)


def test_orderbook_imbalance_scores_directional() -> None:
    books = [
        OrderBookTick(
            exchange="binance",
            symbol="BTCUSDT",
            bid_price=10_000,
            bid_qty=120,
            ask_price=10_001,
            ask_qty=80,
            ts_ms=1,
        ),
        OrderBookTick(
            exchange="binance",
            symbol="BTCUSDT",
            bid_price=10_001,
            bid_qty=130,
            ask_price=10_002,
            ask_qty=70,
            ts_ms=2,
        ),
    ]
    long_score, short_score, avg_imbalance, avg_spread_bps = compute_orderbook_imbalance_scores(
        books=books,
        imbalance_scale=0.35,
    )
    assert avg_imbalance > 0
    assert avg_spread_bps > 0
    assert long_score > short_score


def test_sweep_aggression_scores_detects_buy_sweep() -> None:
    trades = [
        _trade(1_000, price=10_000, qty=40, is_buyer_maker=False),
        _trade(1_050, price=10_010, qty=35, is_buyer_maker=False),
        _trade(2_000, price=10_020, qty=10, is_buyer_maker=True),
    ]
    long_score, short_score, max_buy_sweep, max_sell_sweep = compute_sweep_aggression_scores(
        trades=trades,
        scale_usd=1_000_000,
    )
    assert max_buy_sweep > 0
    assert max_sell_sweep <= 0
    assert long_score > short_score
