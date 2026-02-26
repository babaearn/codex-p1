from __future__ import annotations

import pytest

from project_phantom.config import ExchangeEndpoints
from project_phantom.universe import (
    discover_common_futures_symbols,
    parse_binance_quote_volume,
    parse_binance_usdt_perpetual_symbols,
    parse_bybit_linear_usdt_symbols,
    rank_symbols_by_quote_volume,
)


def test_parse_binance_usdt_perpetual_symbols_filters_contracts() -> None:
    payload = {
        "symbols": [
            {"symbol": "BTCUSDT", "contractType": "PERPETUAL", "status": "TRADING", "quoteAsset": "USDT"},
            {"symbol": "ETHUSDT", "contractType": "CURRENT_QUARTER", "status": "TRADING", "quoteAsset": "USDT"},
            {"symbol": "SOLUSDC", "contractType": "PERPETUAL", "status": "TRADING", "quoteAsset": "USDC"},
            {"symbol": "ADAUSDT", "contractType": "PERPETUAL", "status": "BREAK", "quoteAsset": "USDT"},
        ]
    }
    symbols = parse_binance_usdt_perpetual_symbols(payload)
    assert symbols == {"BTCUSDT"}


def test_parse_bybit_linear_usdt_symbols_filters_rows() -> None:
    payload = {
        "result": {
            "list": [
                {"symbol": "BTCUSDT", "settleCoin": "USDT", "status": "Trading"},
                {"symbol": "ETHUSDT", "settleCoin": "USDT", "status": "Settling"},
                {"symbol": "SOLUSDC", "settleCoin": "USDC", "status": "Trading"},
                {"symbol": "DOGEUSDT", "settleCoin": "USDT", "status": "Trading"},
            ]
        }
    }
    symbols = parse_bybit_linear_usdt_symbols(payload)
    assert symbols == {"BTCUSDT", "DOGEUSDT"}


def test_rank_symbols_uses_quote_volume_desc_then_symbol() -> None:
    symbols = {"SOLUSDT", "BTCUSDT", "ETHUSDT"}
    quote_payload = [
        {"symbol": "ETHUSDT", "quoteVolume": "1000"},
        {"symbol": "BTCUSDT", "quoteVolume": "5000"},
    ]
    quote_map = parse_binance_quote_volume(quote_payload)
    ranked = rank_symbols_by_quote_volume(symbols, quote_map)
    assert ranked == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


@pytest.mark.asyncio
async def test_discover_falls_back_to_bybit_when_binance_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _binance_symbols(*args, **kwargs):  # noqa: ANN001
        return set()

    async def _binance_volumes(*args, **kwargs):  # noqa: ANN001
        return {}

    async def _bybit_symbols(*args, **kwargs):  # noqa: ANN001
        return {"DOGEUSDT", "BTCUSDT"}

    monkeypatch.setattr("project_phantom.universe._fetch_binance_symbols", _binance_symbols)
    monkeypatch.setattr("project_phantom.universe._fetch_binance_quote_volumes", _binance_volumes)
    monkeypatch.setattr("project_phantom.universe._fetch_bybit_symbols", _bybit_symbols)

    symbols = await discover_common_futures_symbols(ExchangeEndpoints(), max_symbols=0)
    assert symbols == ["BTCUSDT", "DOGEUSDT"]
