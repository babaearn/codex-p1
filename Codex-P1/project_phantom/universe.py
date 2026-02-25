from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from project_phantom.config import ExchangeEndpoints


def parse_binance_usdt_perpetual_symbols(payload: dict[str, Any]) -> set[str]:
    symbols: set[str] = set()
    for row in payload.get("symbols", []):
        if str(row.get("contractType", "")).upper() != "PERPETUAL":
            continue
        if str(row.get("status", "")).upper() != "TRADING":
            continue
        if str(row.get("quoteAsset", "")).upper() != "USDT":
            continue
        symbol = str(row.get("symbol", "")).upper()
        if symbol and symbol.endswith("USDT"):
            symbols.add(symbol)
    return symbols


def parse_bybit_linear_usdt_symbols(payload: dict[str, Any]) -> set[str]:
    symbols: set[str] = set()
    rows = payload.get("result", {}).get("list", [])
    for row in rows:
        status = str(row.get("status", "")).upper()
        if status and status != "TRADING":
            continue
        if str(row.get("settleCoin", "")).upper() != "USDT":
            continue
        symbol = str(row.get("symbol", "")).upper()
        if symbol and symbol.endswith("USDT"):
            symbols.add(symbol)
    return symbols


def parse_binance_quote_volume(payload: list[dict[str, Any]]) -> dict[str, float]:
    volumes: dict[str, float] = {}
    for row in payload:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol:
            continue
        try:
            volume = float(row.get("quoteVolume", 0.0))
        except (TypeError, ValueError):
            continue
        volumes[symbol] = volume
    return volumes


def rank_symbols_by_quote_volume(symbols: set[str], quote_volume_map: dict[str, float]) -> list[str]:
    return sorted(symbols, key=lambda item: (-quote_volume_map.get(item, 0.0), item))


async def _fetch_binance_symbols(session: aiohttp.ClientSession, rest_base: str) -> set[str]:
    url = f"{rest_base.rstrip('/')}/fapi/v1/exchangeInfo"
    async with session.get(url, timeout=15) as response:
        response.raise_for_status()
        payload = await response.json()
    return parse_binance_usdt_perpetual_symbols(payload)


async def _fetch_bybit_symbols(session: aiohttp.ClientSession, rest_base: str) -> set[str]:
    symbols: set[str] = set()
    cursor: str | None = None
    for _ in range(20):
        params: dict[str, Any] = {"category": "linear", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        url = f"{rest_base.rstrip('/')}/v5/market/instruments-info"
        async with session.get(url, params=params, timeout=15) as response:
            response.raise_for_status()
            payload = await response.json()

        symbols.update(parse_bybit_linear_usdt_symbols(payload))
        next_cursor = str(payload.get("result", {}).get("nextPageCursor", "")).strip()
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor

    return symbols


async def _fetch_binance_quote_volumes(session: aiohttp.ClientSession, rest_base: str) -> dict[str, float]:
    url = f"{rest_base.rstrip('/')}/fapi/v1/ticker/24hr"
    async with session.get(url, timeout=20) as response:
        response.raise_for_status()
        payload = await response.json()
    if not isinstance(payload, list):
        return {}
    return parse_binance_quote_volume(payload)


async def discover_common_futures_symbols(
    endpoints: ExchangeEndpoints,
    *,
    max_symbols: int = 0,
) -> list[str]:
    async with aiohttp.ClientSession() as session:
        binance_task = asyncio.create_task(_fetch_binance_symbols(session, endpoints.binance_rest))
        bybit_task = asyncio.create_task(_fetch_bybit_symbols(session, endpoints.bybit_rest))
        binance_symbols, bybit_symbols = await asyncio.gather(binance_task, bybit_task)

        common_symbols = binance_symbols & bybit_symbols
        if not common_symbols:
            return []

        quote_volumes = await _fetch_binance_quote_volumes(session, endpoints.binance_rest)
        ranked = rank_symbols_by_quote_volume(common_symbols, quote_volumes)
        if max_symbols > 0:
            return ranked[:max_symbols]
        return ranked
