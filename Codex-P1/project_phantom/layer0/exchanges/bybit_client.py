from __future__ import annotations

import time
from typing import AsyncIterator

import aiohttp

from project_phantom.config import ExchangeEndpoints
from project_phantom.core.types import ExchangeSnapshot, LiquidationUpdate


class BybitClient:
    name = "bybit"

    def __init__(self, session: aiohttp.ClientSession, endpoints: ExchangeEndpoints) -> None:
        self._session = session
        self._rest = endpoints.bybit_rest.rstrip("/")
        self._ws = endpoints.bybit_ws

    async def fetch_snapshot(self, symbol: str) -> ExchangeSnapshot:
        oi_url = f"{self._rest}/v5/market/open-interest"
        oi_params = {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": "5min",
            "limit": 1,
        }
        tickers_url = f"{self._rest}/v5/market/tickers"
        tickers_params = {"category": "linear", "symbol": symbol}

        async with self._session.get(oi_url, params=oi_params, timeout=10) as oi_resp:
            oi_resp.raise_for_status()
            oi_payload = await oi_resp.json()

        async with self._session.get(tickers_url, params=tickers_params, timeout=10) as ticker_resp:
            ticker_resp.raise_for_status()
            ticker_payload = await ticker_resp.json()

        oi_rows = oi_payload.get("result", {}).get("list", [])
        ticker_rows = ticker_payload.get("result", {}).get("list", [])
        if not oi_rows or not ticker_rows:
            raise RuntimeError("Bybit returned an empty payload")

        oi_row = oi_rows[0]
        ticker_row = ticker_rows[0]
        ts_ms = int(oi_row.get("timestamp", int(time.time() * 1000)))

        return ExchangeSnapshot(
            exchange=self.name,
            symbol=symbol,
            open_interest=float(oi_row["openInterest"]),
            funding_rate=float(ticker_row["fundingRate"]),
            mark_price=float(ticker_row["markPrice"]),
            ts_ms=ts_ms,
            active=True,
        )

    async def stream_liquidations(self, symbol: str) -> AsyncIterator[LiquidationUpdate]:
        async with self._session.ws_connect(self._ws, heartbeat=30) as ws:
            await ws.send_json({"op": "subscribe", "args": [f"allLiquidation.{symbol}"]})

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.ERROR:
                    raise RuntimeError("Bybit websocket stream error")
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue

                payload = msg.json()
                if payload.get("topic") != f"allLiquidation.{symbol}":
                    continue

                rows = payload.get("data", [])
                for row in rows:
                    side = str(row.get("side", "")).upper()
                    liquidated_side = "LONG" if side == "SELL" else "SHORT"
                    price = float(row.get("price", 0.0))
                    qty = float(row.get("size", 0.0))
                    ts_ms = int(row.get("updatedTime", row.get("T", int(time.time() * 1000))))
                    yield LiquidationUpdate(
                        exchange=self.name,
                        symbol=symbol,
                        price=price,
                        quantity=qty,
                        notional=price * qty,
                        liquidated_side=liquidated_side,
                        ts_ms=ts_ms,
                    )

    async def close(self) -> None:
        return None
