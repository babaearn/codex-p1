from __future__ import annotations

import time
from typing import AsyncIterator

import aiohttp

from project_phantom.config import ExchangeEndpoints
from project_phantom.core.types import OrderBookTick


class BinanceBookClient:
    name = "binance_book"

    def __init__(self, session: aiohttp.ClientSession, endpoints: ExchangeEndpoints) -> None:
        self._session = session
        self._ws_root = endpoints.binance_trade_ws.rstrip("/")

    async def stream_book_ticker(self, symbol: str) -> AsyncIterator[OrderBookTick]:
        stream_symbol = symbol.lower()
        ws_url = f"{self._ws_root}/{stream_symbol}@bookTicker"
        async with self._session.ws_connect(ws_url, heartbeat=30) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.ERROR:
                    raise RuntimeError("Binance book websocket stream error")
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue

                payload = msg.json()
                event_symbol = str(payload.get("s", "")).upper()
                if event_symbol != symbol.upper():
                    continue

                yield OrderBookTick(
                    exchange="binance",
                    symbol=event_symbol,
                    bid_price=float(payload.get("b", 0.0)),
                    bid_qty=float(payload.get("B", 0.0)),
                    ask_price=float(payload.get("a", 0.0)),
                    ask_qty=float(payload.get("A", 0.0)),
                    ts_ms=int(payload.get("E", int(time.time() * 1000))),
                )

    async def close(self) -> None:
        return None
