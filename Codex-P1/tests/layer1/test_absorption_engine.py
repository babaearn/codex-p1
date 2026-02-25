from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import AsyncIterator

import pytest

from project_phantom.config import BackoffConfig, Layer1Config, WhaleAlertConfig
from project_phantom.core.types import (
    AbsorptionBreakdown,
    AbsorptionEvent,
    SignalBreakdown,
    TradeTick,
    TrapSetupEvent,
)
from project_phantom.layer1.absorption_engine import run_layer1


@dataclass
class FakeTradeClient:
    name: str
    trades: list[TradeTick]

    async def stream_trades(self, symbol: str) -> AsyncIterator[TradeTick]:
        for trade in self.trades:
            yield trade
            await asyncio.sleep(0.005)
        while True:
            await asyncio.sleep(1)

    async def close(self) -> None:
        return None


def _trap_event(direction: str = "LONG") -> TrapSetupEvent:
    return TrapSetupEvent(
        event_type="TRAP_SETUP_EVENT",
        event_id=f"trap-{direction.lower()}",
        ts_ms=int(time.time() * 1000),
        symbol="BTCUSDT",
        direction=direction,  # type: ignore[arg-type]
        score=0.8,
        passed=True,
        components=SignalBreakdown(
            liquidation_long=1.0,
            liquidation_short=0.2,
            funding_oi_long=1.0,
            funding_oi_short=0.2,
            oi_divergence=0.8,
        ),
        raw={},
        degraded=False,
    )


def _seed_absorption_event() -> AbsorptionEvent:
    return AbsorptionEvent(
        event_type="ABSORPTION_EVENT",
        event_id="old",
        ts_ms=0,
        symbol="BTCUSDT",
        direction="LONG",
        score=1.0,
        passed=True,
        source_trap_event_id="seed",
        components=AbsorptionBreakdown(
            whale_net_flow_long=1.0,
            whale_net_flow_short=0.0,
            twap_uniformity_long=1.0,
            twap_uniformity_short=0.0,
            cvd_long=1.0,
            cvd_short=0.0,
            stablecoin_inflow=0.0,
            hidden_divergence_long=False,
            hidden_divergence_short=False,
        ),
        raw={},
        degraded=False,
    )


def _trade_samples() -> list[TradeTick]:
    base = int(time.time() * 1000)
    rows: list[TradeTick] = []
    for idx in range(12):
        rows.append(
            TradeTick(
                exchange="binance",
                symbol="BTCUSDT",
                price=10_000 + idx,
                quantity=20.0,
                is_buyer_maker=False,
                ts_ms=base + (idx * 1_000),
            )
        )
    return rows


def _layer1_config(*, whale_alert_enabled: bool = False) -> Layer1Config:
    return Layer1Config(
        symbol="BTCUSDT",
        cadence_seconds=0.05,
        trade_window_seconds=300,
        setup_ttl_seconds=180,
        min_trades_for_metrics=5,
        whale_alert=WhaleAlertConfig(enabled=whale_alert_enabled, poll_interval_seconds=0.05),
        backoff=BackoffConfig(min_seconds=0.05, max_seconds=0.2),
    )


@pytest.mark.asyncio
async def test_layer1_emits_absorption_event_for_active_long_setup() -> None:
    in_queue: asyncio.Queue[TrapSetupEvent] = asyncio.Queue()
    out_queue: asyncio.Queue[AbsorptionEvent] = asyncio.Queue(maxsize=10)
    stop_event = asyncio.Event()
    in_queue.put_nowait(_trap_event("LONG"))

    client = FakeTradeClient(name="fake-trades", trades=_trade_samples())
    task = asyncio.create_task(
        run_layer1(
            _layer1_config(),
            in_queue,
            out_queue,
            stop_event=stop_event,
            trade_client=client,
        )
    )
    await asyncio.sleep(0.5)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert not out_queue.empty()
    event = out_queue.get_nowait()
    assert event.event_type == "ABSORPTION_EVENT"
    assert event.direction == "LONG"
    assert event.source_trap_event_id == "trap-long"
    assert event.passed is True


@pytest.mark.asyncio
async def test_layer1_queue_drop_oldest_policy() -> None:
    in_queue: asyncio.Queue[TrapSetupEvent] = asyncio.Queue()
    out_queue: asyncio.Queue[AbsorptionEvent] = asyncio.Queue(maxsize=1)
    out_queue.put_nowait(_seed_absorption_event())
    stop_event = asyncio.Event()
    in_queue.put_nowait(_trap_event("LONG"))

    client = FakeTradeClient(name="fake-trades", trades=_trade_samples())
    task = asyncio.create_task(
        run_layer1(
            _layer1_config(),
            in_queue,
            out_queue,
            stop_event=stop_event,
            trade_client=client,
        )
    )
    await asyncio.sleep(0.5)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert out_queue.qsize() == 1
    latest = out_queue.get_nowait()
    assert latest.event_id != "old"


@pytest.mark.asyncio
async def test_layer1_whale_alert_missing_data_sets_degraded_flag() -> None:
    in_queue: asyncio.Queue[TrapSetupEvent] = asyncio.Queue()
    out_queue: asyncio.Queue[AbsorptionEvent] = asyncio.Queue(maxsize=10)
    stop_event = asyncio.Event()
    in_queue.put_nowait(_trap_event("LONG"))

    client = FakeTradeClient(name="fake-trades", trades=_trade_samples())
    task = asyncio.create_task(
        run_layer1(
            _layer1_config(whale_alert_enabled=True),
            in_queue,
            out_queue,
            stop_event=stop_event,
            trade_client=client,
            stablecoin_client=None,
        )
    )
    await asyncio.sleep(0.5)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert not out_queue.empty()
    event = out_queue.get_nowait()
    assert event.degraded is True
    assert event.degrade_reason is not None
    assert "WHALE_ALERT_NO_DATA" in event.degrade_reason
