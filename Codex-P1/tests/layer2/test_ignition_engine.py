from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import pytest

from project_phantom.config import BackoffConfig, Layer2Config, Layer2ThresholdConfig
from project_phantom.core.types import (
    AbsorptionBreakdown,
    AbsorptionEvent,
    Candle,
    IgnitionBreakdown,
    PrePumpEvent,
)
from project_phantom.layer2.ignition_engine import run_layer2


@dataclass
class FakeCandleClient:
    name: str
    candles: list[Candle]

    async def fetch_candles(self, symbol: str, interval: str, limit: int) -> list[Candle]:
        _ = (symbol, interval, limit)
        return self.candles

    async def close(self) -> None:
        return None


@dataclass
class FakeSMCDetector:
    name: str = "fake_smc"
    choch: bool = False
    order_block: bool = False
    fail: bool = False

    async def detect(self, candles: list[Candle], direction: str) -> tuple[bool, bool, dict[str, Any]]:
        _ = (candles, direction)
        if self.fail:
            raise RuntimeError("smc failed")
        return (self.choch, self.order_block, {"backend": "fake"})


def _candles(momentum: str = "up") -> list[Candle]:
    base = int(time.time() * 1000) - 20 * 60_000
    rows: list[Candle] = []
    for idx in range(20):
        if momentum == "up":
            close = 10_000 + idx * 10
        elif momentum == "down":
            close = 10_200 - idx * 10
        else:
            close = 10_000
        rows.append(
            Candle(
                open_time_ms=base + idx * 60_000,
                open=close - 3,
                high=close + 5,
                low=close - 6,
                close=close,
                volume=100.0,
                close_time_ms=base + (idx + 1) * 60_000 - 1,
            )
        )
    return rows


def _absorption_event(*, direction: str = "LONG", score: float = 0.8, trap_score: float = 0.9) -> AbsorptionEvent:
    return AbsorptionEvent(
        event_type="ABSORPTION_EVENT",
        event_id=f"abs-{direction.lower()}",
        ts_ms=int(time.time() * 1000),
        symbol="BTCUSDT",
        direction=direction,  # type: ignore[arg-type]
        score=score,
        passed=True,
        source_trap_event_id="trap-1",
        components=AbsorptionBreakdown(
            whale_net_flow_long=1.0,
            whale_net_flow_short=0.1,
            twap_uniformity_long=0.8,
            twap_uniformity_short=0.2,
            cvd_long=0.8,
            cvd_short=0.2,
            stablecoin_inflow=0.0,
            hidden_divergence_long=False,
            hidden_divergence_short=False,
        ),
        raw={"source_trap_score": trap_score},
        degraded=False,
    )


def _seed_pre_pump() -> PrePumpEvent:
    return PrePumpEvent(
        event_type="PRE_PUMP_EVENT",
        event_id="old",
        ts_ms=0,
        symbol="BTCUSDT",
        direction="LONG",
        score=1.0,
        passed=True,
        source_absorption_event_id="seed-abs",
        source_trap_event_id="seed-trap",
        components=IgnitionBreakdown(
            choch=True,
            order_block=True,
            absorption_strength=True,
            trap_strength=True,
            momentum=True,
            confirmations=5,
        ),
        raw={},
        degraded=False,
    )


def _config() -> Layer2Config:
    return Layer2Config(
        symbol="BTCUSDT",
        cadence_seconds=0.05,
        rest_poll_interval_seconds=0.05,
        setup_ttl_seconds=180,
        thresholds=Layer2ThresholdConfig(
            min_confirmations=3,
            absorption_score_min=0.6,
            trap_score_min=0.7,
            momentum_lookback_bars=5,
            momentum_min_return_pct=0.001,
        ),
        backoff=BackoffConfig(min_seconds=0.05, max_seconds=0.2),
    )


@pytest.mark.asyncio
async def test_layer2_emits_pre_pump_event_on_3_of_5() -> None:
    in_queue: asyncio.Queue[AbsorptionEvent] = asyncio.Queue()
    out_queue: asyncio.Queue[PrePumpEvent] = asyncio.Queue(maxsize=10)
    stop_event = asyncio.Event()

    in_queue.put_nowait(_absorption_event(direction="LONG", score=0.8, trap_score=0.9))

    task = asyncio.create_task(
        run_layer2(
            _config(),
            in_queue,
            out_queue,
            stop_event=stop_event,
            candle_client=FakeCandleClient(name="candles", candles=_candles("up")),
            smc_detector=FakeSMCDetector(choch=False, order_block=False),
        )
    )
    await asyncio.sleep(0.4)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert not out_queue.empty()
    event = out_queue.get_nowait()
    assert event.event_type == "PRE_PUMP_EVENT"
    assert event.passed is True
    assert event.components.confirmations >= 3
    assert event.source_absorption_event_id == "abs-long"


@pytest.mark.asyncio
async def test_layer2_does_not_emit_below_min_confirmations() -> None:
    in_queue: asyncio.Queue[AbsorptionEvent] = asyncio.Queue()
    out_queue: asyncio.Queue[PrePumpEvent] = asyncio.Queue(maxsize=10)
    stop_event = asyncio.Event()

    in_queue.put_nowait(_absorption_event(direction="LONG", score=0.3, trap_score=0.1))

    task = asyncio.create_task(
        run_layer2(
            _config(),
            in_queue,
            out_queue,
            stop_event=stop_event,
            candle_client=FakeCandleClient(name="candles", candles=_candles("flat")),
            smc_detector=FakeSMCDetector(choch=False, order_block=False),
        )
    )
    await asyncio.sleep(0.4)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert out_queue.empty()


@pytest.mark.asyncio
async def test_layer2_queue_drop_oldest_policy() -> None:
    in_queue: asyncio.Queue[AbsorptionEvent] = asyncio.Queue()
    out_queue: asyncio.Queue[PrePumpEvent] = asyncio.Queue(maxsize=1)
    stop_event = asyncio.Event()
    out_queue.put_nowait(_seed_pre_pump())
    in_queue.put_nowait(_absorption_event(direction="LONG", score=0.8, trap_score=0.9))

    task = asyncio.create_task(
        run_layer2(
            _config(),
            in_queue,
            out_queue,
            stop_event=stop_event,
            candle_client=FakeCandleClient(name="candles", candles=_candles("up")),
            smc_detector=FakeSMCDetector(choch=False, order_block=False),
        )
    )
    await asyncio.sleep(0.4)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert out_queue.qsize() == 1
    latest = out_queue.get_nowait()
    assert latest.event_id != "old"


@pytest.mark.asyncio
async def test_layer2_marks_degraded_when_smc_fails_but_still_emits() -> None:
    in_queue: asyncio.Queue[AbsorptionEvent] = asyncio.Queue()
    out_queue: asyncio.Queue[PrePumpEvent] = asyncio.Queue(maxsize=10)
    stop_event = asyncio.Event()
    in_queue.put_nowait(_absorption_event(direction="LONG", score=0.8, trap_score=0.9))

    task = asyncio.create_task(
        run_layer2(
            _config(),
            in_queue,
            out_queue,
            stop_event=stop_event,
            candle_client=FakeCandleClient(name="candles", candles=_candles("up")),
            smc_detector=FakeSMCDetector(fail=True),
        )
    )
    await asyncio.sleep(0.4)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert not out_queue.empty()
    event = out_queue.get_nowait()
    assert event.degraded is True
    assert event.degrade_reason is not None
    assert "SMC_RUNTIMEERROR" in event.degrade_reason
