from __future__ import annotations

import aiohttp

from project_phantom.config import ExchangeEndpoints
from project_phantom.core.types import Candle


class BinanceCandleClient:
    name = "binance_candles"

    def __init__(self, session: aiohttp.ClientSession, endpoints: ExchangeEndpoints) -> None:
        self._session = session
        self._rest = endpoints.binance_rest.rstrip("/")

    async def fetch_candles(self, symbol: str, interval: str, limit: int) -> list[Candle]:
        url = f"{self._rest}/fapi/v1/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }
        async with self._session.get(url, params=params, timeout=10) as response:
            response.raise_for_status()
            payload = await response.json()

        candles: list[Candle] = []
        for row in payload:
            candles.append(
                Candle(
                    open_time_ms=int(row[0]),
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=float(row[5]),
                    close_time_ms=int(row[6]),
                )
            )
        return candles

    async def close(self) -> None:
        return None
