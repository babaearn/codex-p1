from __future__ import annotations

import time
from typing import AsyncIterator

import aiohttp

from project_phantom.config import ExchangeEndpoints
from project_phantom.core.types import ExchangeSnapshot, LiquidationUpdate


class BinanceClient:
    name = "binance"

    def __init__(self, session: aiohttp.ClientSession, endpoints: ExchangeEndpoints) -> None:
        self._session = session
        self._rest = endpoints.binance_rest.rstrip("/")
        self._ws = endpoints.binance_ws

    async def fetch_snapshot(self, symbol: str) -> ExchangeSnapshot:
        oi_url = f"{self._rest}/fapi/v1/openInterest"
        premium_url = f"{self._rest}/fapi/v1/premiumIndex"
        params = {"symbol": symbol}

        async with self._session.get(oi_url, params=params, timeout=10) as oi_resp:
            oi_resp.raise_for_status()
            oi_payload = await oi_resp.json()

        async with self._session.get(premium_url, params=params, timeout=10) as premium_resp:
            premium_resp.raise_for_status()
            premium_payload = await premium_resp.json()

        ts_ms = int(premium_payload.get("time", int(time.time() * 1000)))
        return ExchangeSnapshot(
            exchange=self.name,
            symbol=symbol,
            open_interest=float(oi_payload["openInterest"]),
            funding_rate=float(premium_payload["lastFundingRate"]),
            mark_price=float(premium_payload["markPrice"]),
            ts_ms=ts_ms,
            active=True,
        )

    async def stream_liquidations(self, symbol: str) -> AsyncIterator[LiquidationUpdate]:
        async with self._session.ws_connect(self._ws, heartbeat=30) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.ERROR:
                    raise RuntimeError("Binance websocket stream error")
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue

                payload = msg.json()
                event = payload.get("data", payload)
                order = event.get("o", {})
                if order.get("s") != symbol:
                    continue

                side = str(order.get("S", "")).upper()
                liquidated_side = "LONG" if side == "SELL" else "SHORT"
                price = float(order.get("p", 0.0))
                qty = float(order.get("q", 0.0))
                notional = price * qty
                ts_ms = int(order.get("T", int(time.time() * 1000)))

                yield LiquidationUpdate(
                    exchange=self.name,
                    symbol=symbol,
                    price=price,
                    quantity=qty,
                    notional=notional,
                    liquidated_side=liquidated_side,
                    ts_ms=ts_ms,
                )

    async def close(self) -> None:
        return None
