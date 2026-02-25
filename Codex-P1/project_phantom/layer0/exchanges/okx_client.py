from __future__ import annotations

import asyncio
import time
from typing import AsyncIterator

import aiohttp

from project_phantom.config import ExchangeEndpoints
from project_phantom.core.types import ExchangeSnapshot, LiquidationUpdate


def _okx_inst_id(symbol: str) -> str:
    if symbol.upper() == "BTCUSDT":
        return "BTC-USDT-SWAP"
    if symbol.upper().endswith("USDT"):
        return f"{symbol[:-4]}-USDT-SWAP"
    return symbol


class OkxClient:
    name = "okx"

    def __init__(self, session: aiohttp.ClientSession, endpoints: ExchangeEndpoints) -> None:
        self._session = session
        self._rest = endpoints.okx_rest.rstrip("/")

    async def fetch_snapshot(self, symbol: str) -> ExchangeSnapshot:
        inst_id = _okx_inst_id(symbol)

        oi_url = f"{self._rest}/api/v5/public/open-interest"
        oi_params = {"instId": inst_id}

        funding_url = f"{self._rest}/api/v5/public/funding-rate"
        funding_params = {"instId": inst_id}

        mark_url = f"{self._rest}/api/v5/public/mark-price"
        mark_params = {"instType": "SWAP", "instId": inst_id}

        async with self._session.get(oi_url, params=oi_params, timeout=10) as oi_resp:
            oi_resp.raise_for_status()
            oi_payload = await oi_resp.json()
        async with self._session.get(funding_url, params=funding_params, timeout=10) as funding_resp:
            funding_resp.raise_for_status()
            funding_payload = await funding_resp.json()
        async with self._session.get(mark_url, params=mark_params, timeout=10) as mark_resp:
            mark_resp.raise_for_status()
            mark_payload = await mark_resp.json()

        oi_row = oi_payload.get("data", [{}])[0]
        funding_row = funding_payload.get("data", [{}])[0]
        mark_row = mark_payload.get("data", [{}])[0]
        ts_ms = int(mark_row.get("ts", int(time.time() * 1000)))

        return ExchangeSnapshot(
            exchange=self.name,
            symbol=symbol,
            open_interest=float(oi_row["oi"]),
            funding_rate=float(funding_row["fundingRate"]),
            mark_price=float(mark_row["markPx"]),
            ts_ms=ts_ms,
            active=True,
        )

    async def stream_liquidations(self, symbol: str) -> AsyncIterator[LiquidationUpdate]:
        while True:
            await asyncio.sleep(300)
            if False:
                yield LiquidationUpdate(
                    exchange=self.name,
                    symbol=symbol,
                    price=0.0,
                    quantity=0.0,
                    notional=0.0,
                    liquidated_side="LONG",
                    ts_ms=0,
                )

    async def close(self) -> None:
        return None
