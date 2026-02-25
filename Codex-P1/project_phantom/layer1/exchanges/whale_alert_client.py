from __future__ import annotations

import time

import aiohttp

from project_phantom.config import ExchangeEndpoints, WhaleAlertConfig
from project_phantom.core.types import StablecoinFlowObservation


class WhaleAlertClient:
    name = "whale_alert"

    def __init__(
        self,
        session: aiohttp.ClientSession,
        endpoints: ExchangeEndpoints,
        config: WhaleAlertConfig,
    ) -> None:
        self._session = session
        self._base_url = endpoints.whale_alert_rest.rstrip("/")
        self._api_key = config.api_key
        self._min_transfer_usd = config.min_transfer_usd

    async def fetch_inflow_usd(self) -> StablecoinFlowObservation:
        if not self._api_key:
            raise RuntimeError("WHALE_ALERT_API_KEY_MISSING")

        now_s = int(time.time())
        params = {
            "api_key": self._api_key,
            "start": now_s - 120,
            "currency": "usdt",
            "min_value": int(self._min_transfer_usd),
        }
        url = f"{self._base_url}/transactions"
        async with self._session.get(url, params=params, timeout=15) as response:
            response.raise_for_status()
            payload = await response.json()

        inflow_total = 0.0
        transactions = payload.get("transactions", [])
        for row in transactions:
            to_data = row.get("to") or {}
            owner_type = str(to_data.get("owner_type", "")).lower()
            if owner_type != "exchange":
                continue
            amount_usd = row.get("amount_usd")
            if amount_usd is None:
                continue
            inflow_total += float(amount_usd)

        return StablecoinFlowObservation(
            source=self.name,
            inflow_usd=inflow_total,
            ts_ms=now_s * 1000,
        )

    async def close(self) -> None:
        return None
