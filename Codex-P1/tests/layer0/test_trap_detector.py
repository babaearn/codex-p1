from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import AsyncIterator

import pytest

from project_phantom.config import BackoffConfig, Layer0Config
from project_phantom.core.types import ExchangeSnapshot, LiquidationUpdate, SignalBreakdown, TrapSetupEvent
from project_phantom.layer0.trap_detector import run_layer0


@dataclass
class FakeClient:
    name: str
    base_oi: float
    oi_step: float
    funding_rate: float
    mark_price: float
    fail_with: str | None = None
    emit_liquidation: bool = True
    liquidation_side: str = "SHORT"
    price_offset: float = 0.01

    def __post_init__(self) -> None:
        self._calls = 0

    async def fetch_snapshot(self, symbol: str) -> ExchangeSnapshot:
        if self.fail_with:
            raise RuntimeError(self.fail_with)

        self._calls += 1
        ts_ms = int(time.time() * 1000)
        oi = self.base_oi + (self.oi_step * self._calls)
        return ExchangeSnapshot(
            exchange=self.name,
            symbol=symbol,
            open_interest=oi,
            funding_rate=self.funding_rate,
            mark_price=self.mark_price,
            ts_ms=ts_ms,
            active=True,
        )

    async def stream_liquidations(self, symbol: str) -> AsyncIterator[LiquidationUpdate]:
        if self.emit_liquidation:
            multiplier = 1.0 + self.price_offset if self.liquidation_side == "SHORT" else 1.0 - self.price_offset
            price = self.mark_price * multiplier
            qty = 10.0
            yield LiquidationUpdate(
                exchange=self.name,
                symbol=symbol,
                price=price,
                quantity=qty,
                notional=price * qty,
                liquidated_side=self.liquidation_side,  # type: ignore[arg-type]
                ts_ms=int(time.time() * 1000),
            )

        while True:
            await asyncio.sleep(1)

    async def close(self) -> None:
        return None


def _base_config(
    *,
    warmup_minutes: int = 0,
    enable_bybit: bool = False,
    queue_maxsize: int = 1,
) -> Layer0Config:
    return Layer0Config(
        symbol="BTCUSDT",
        cadence_seconds=0.05,
        rest_poll_interval_seconds=0.05,
        snapshot_staleness_seconds=2.0,
        warmup_minutes=warmup_minutes,
        queue_maxsize=queue_maxsize,
        enable_binance=True,
        enable_bybit=enable_bybit,
        enable_okx=False,
        backoff=BackoffConfig(min_seconds=0.05, max_seconds=0.2),
    )


def _old_event() -> TrapSetupEvent:
    return TrapSetupEvent(
        event_type="TRAP_SETUP_EVENT",
        event_id="old",
        ts_ms=0,
        symbol="BTCUSDT",
        direction="LONG",
        score=1.0,
        passed=True,
        components=SignalBreakdown(
            liquidation_long=1.0,
            liquidation_short=0.0,
            funding_oi_long=1.0,
            funding_oi_short=0.0,
            oi_divergence=1.0,
        ),
        raw={"seed": "old"},
        degraded=False,
    )


@pytest.mark.asyncio
async def test_queue_drop_oldest_policy() -> None:
    queue: asyncio.Queue[TrapSetupEvent] = asyncio.Queue(maxsize=1)
    queue.put_nowait(_old_event())

    config = _base_config(queue_maxsize=1)
    stop_event = asyncio.Event()
    clients = {
        "binance": FakeClient(
            name="binance",
            base_oi=100.0,
            oi_step=5.0,
            funding_rate=-0.001,
            mark_price=10_000.0,
            emit_liquidation=True,
            liquidation_side="SHORT",
            price_offset=0.005,
        )
    }

    task = asyncio.create_task(run_layer0(config, queue, stop_event=stop_event, clients=clients))
    await asyncio.sleep(0.4)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert queue.qsize() == 1
    latest = queue.get_nowait()
    assert latest.event_id != "old"


@pytest.mark.asyncio
async def test_bybit_403_degrades_but_emits() -> None:
    queue: asyncio.Queue[TrapSetupEvent] = asyncio.Queue(maxsize=1)
    config = _base_config(enable_bybit=True, queue_maxsize=1)
    stop_event = asyncio.Event()
    clients = {
        "binance": FakeClient(
            name="binance",
            base_oi=100.0,
            oi_step=5.0,
            funding_rate=-0.001,
            mark_price=10_000.0,
            emit_liquidation=True,
            liquidation_side="SHORT",
            price_offset=0.005,
        ),
        "bybit": FakeClient(
            name="bybit",
            base_oi=100.0,
            oi_step=0.0,
            funding_rate=0.0,
            mark_price=10_000.0,
            fail_with="403 Forbidden",
            emit_liquidation=False,
        ),
    }

    task = asyncio.create_task(run_layer0(config, queue, stop_event=stop_event, clients=clients))
    await asyncio.sleep(0.5)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert not queue.empty()
    event = queue.get_nowait()
    assert event.degraded is True
    assert event.degrade_reason is not None
    assert "BYBIT_403" in event.degrade_reason


@pytest.mark.asyncio
async def test_warmup_suppresses_early_emission() -> None:
    queue: asyncio.Queue[TrapSetupEvent] = asyncio.Queue(maxsize=10)
    config = _base_config(warmup_minutes=1, queue_maxsize=10)
    stop_event = asyncio.Event()
    clients = {
        "binance": FakeClient(
            name="binance",
            base_oi=100.0,
            oi_step=5.0,
            funding_rate=-0.001,
            mark_price=10_000.0,
            emit_liquidation=True,
            liquidation_side="SHORT",
            price_offset=0.005,
        )
    }

    task = asyncio.create_task(run_layer0(config, queue, stop_event=stop_event, clients=clients))
    await asyncio.sleep(0.3)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert queue.empty()
