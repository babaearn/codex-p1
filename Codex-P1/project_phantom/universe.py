from __future__ import annotations
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


async def _safe_get_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    timeout_seconds: int = 15,
) -> Any | None:
    try:
        async with session.get(url, params=params, timeout=timeout_seconds) as response:
            if response.status >= 400:
                return None
            return await response.json()
    except Exception:
        return None


async def _fetch_binance_symbols(session: aiohttp.ClientSession, rest_base: str) -> set[str]:
    url = f"{rest_base.rstrip('/')}/fapi/v1/exchangeInfo"
    payload = await _safe_get_json(session, url, timeout_seconds=15)
    if not isinstance(payload, dict):
        return set()
    return parse_binance_usdt_perpetual_symbols(payload)


async def _fetch_bybit_symbols(session: aiohttp.ClientSession, rest_base: str) -> set[str]:
    symbols: set[str] = set()
    cursor: str | None = None
    for _ in range(20):
        params: dict[str, Any] = {"category": "linear", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        url = f"{rest_base.rstrip('/')}/v5/market/instruments-info"
        payload = await _safe_get_json(session, url, params=params, timeout_seconds=15)
        if not isinstance(payload, dict):
            break

        symbols.update(parse_bybit_linear_usdt_symbols(payload))
        next_cursor = str(payload.get("result", {}).get("nextPageCursor", "")).strip()
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor

    return symbols


async def _fetch_binance_quote_volumes(session: aiohttp.ClientSession, rest_base: str) -> dict[str, float]:
    url = f"{rest_base.rstrip('/')}/fapi/v1/ticker/24hr"
    payload = await _safe_get_json(session, url, timeout_seconds=20)
    if not isinstance(payload, list):
        return {}
    return parse_binance_quote_volume(payload)


async def discover_common_futures_symbols(
    endpoints: ExchangeEndpoints,
    *,
    max_symbols: int = 0,
) -> list[str]:
    async with aiohttp.ClientSession(headers={"User-Agent": "project-phantom/1.0"}) as session:
        # Try primary + fallback Binance REST hosts to avoid temporary 418 blocks.
        binance_bases = [endpoints.binance_rest, "https://fapi1.binance.com", "https://fapi2.binance.com", "https://fapi3.binance.com"]
        binance_symbols: set[str] = set()
        quote_volumes: dict[str, float] = {}
        for base in binance_bases:
            symbols = await _fetch_binance_symbols(session, base)
            if symbols:
                binance_symbols = symbols
                quote_volumes = await _fetch_binance_quote_volumes(session, base)
                break

        bybit_symbols = await _fetch_bybit_symbols(session, endpoints.bybit_rest)

        if binance_symbols and bybit_symbols:
            selected = binance_symbols & bybit_symbols
        elif bybit_symbols:
            selected = bybit_symbols
        else:
            selected = binance_symbols

        if not selected:
            return []

        ranked = rank_symbols_by_quote_volume(selected, quote_volumes)
        if max_symbols > 0:
            return ranked[:max_symbols]
        return ranked
