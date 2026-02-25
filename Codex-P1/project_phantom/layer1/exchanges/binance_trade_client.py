from __future__ import annotations

import time
from typing import AsyncIterator

import aiohttp

from project_phantom.config import ExchangeEndpoints
from project_phantom.core.types import TradeTick


class BinanceTradeClient:
    name = "binance_trades"

    def __init__(self, session: aiohttp.ClientSession, endpoints: ExchangeEndpoints) -> None:
        self._session = session
        self._ws_root = endpoints.binance_trade_ws.rstrip("/")

    async def stream_trades(self, symbol: str) -> AsyncIterator[TradeTick]:
        stream_symbol = symbol.lower()
        ws_url = f"{self._ws_root}/{stream_symbol}@trade"
        async with self._session.ws_connect(ws_url, heartbeat=30) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.ERROR:
                    raise RuntimeError("Binance trade websocket stream error")
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue

                payload = msg.json()
                if payload.get("e") != "trade":
                    continue

                event_symbol = str(payload.get("s", "")).upper()
                if event_symbol != symbol.upper():
                    continue

                trade = TradeTick(
                    exchange="binance",
                    symbol=event_symbol,
                    price=float(payload["p"]),
                    quantity=float(payload["q"]),
                    is_buyer_maker=bool(payload["m"]),
                    ts_ms=int(payload.get("T", payload.get("E", int(time.time() * 1000)))),
                )
                yield trade

    async def close(self) -> None:
        return None
