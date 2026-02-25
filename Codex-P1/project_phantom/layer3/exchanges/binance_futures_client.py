from __future__ import annotations

from typing import Any

from project_phantom.config import BinanceExecutionConfig


class BinanceFuturesExecutionClient:
    name = "binance_futures"

    def __init__(self, async_client: Any) -> None:
        self._client = async_client

    @classmethod
    async def create(cls, config: BinanceExecutionConfig) -> "BinanceFuturesExecutionClient":
        if not config.api_key or not config.api_secret:
            raise RuntimeError("BINANCE_API_KEY/BINANCE_API_SECRET are required for live execution")
        try:
            from binance import AsyncClient  # type: ignore
        except ModuleNotFoundError as exc:
            raise RuntimeError("python-binance is required for Layer 3 execution") from exc

        client = await AsyncClient.create(
            api_key=config.api_key,
            api_secret=config.api_secret,
            testnet=config.testnet,
        )
        return cls(client)

    async def futures_create_order(self, **kwargs: Any) -> dict[str, Any]:
        return await self._client.futures_create_order(**kwargs)

    async def close(self) -> None:
        await self._client.close_connection()
