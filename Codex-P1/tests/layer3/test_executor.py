from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

import pytest

from project_phantom.config import BackoffConfig, Layer3Config, Layer3RiskConfig, TelegramConfig
from project_phantom.core.types import ExecutionEvent, IgnitionBreakdown, PrePumpEvent
from project_phantom.layer3.executor import run_layer3


@dataclass
class FakeExecutionClient:
    name: str = "fake_exec"
    calls: list[dict[str, Any]] = field(default_factory=list)
    next_id: int = 1

    async def futures_create_order(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        order = {"orderId": str(self.next_id)}
        self.next_id += 1
        if kwargs.get("type") == "MARKET":
            order["avgPrice"] = "62959"
        return order

    async def close(self) -> None:
        return None


@dataclass
class FakeTelegramNotifier:
    name: str = "fake_telegram"
    messages: list[str] = field(default_factory=list)

    async def send_message(self, text: str) -> None:
        self.messages.append(text)

    async def close(self) -> None:
        return None


def _pre_pump_event(direction: str = "LONG") -> PrePumpEvent:
    return PrePumpEvent(
        event_type="PRE_PUMP_EVENT",
        event_id=f"pre-{direction.lower()}",
        ts_ms=int(time.time() * 1000),
        symbol="BTCUSDT",
        direction=direction,  # type: ignore[arg-type]
        score=0.8,
        passed=True,
        source_absorption_event_id="abs-1",
        source_trap_event_id="trap-1",
        components=IgnitionBreakdown(
            choch=True,
            order_block=True,
            absorption_strength=True,
            trap_strength=False,
            momentum=True,
            confirmations=4,
        ),
        raw={
            "source_absorption_raw": {
                "source_trap_raw": {
                    "current_price": 62_959,
                    "swept_liquidation_zone_low": 62_680,
                    "swept_liquidation_zone_high": 63_200,
                    "nearest_ob_above": 64_200,
                    "nearest_ob_below": 61_900,
                    "avg_funding": -0.00012,
                    "oi_spread_pct": 0.82,
                    "short_cluster_p90_notional": 62_750,
                }
            },
            "source_absorption_components": {
                "cvd_long": 0.7,
                "cvd_short": 0.1,
                "hidden_divergence_long": False,
                "hidden_divergence_short": False,
            },
        },
        degraded=False,
    )


def _config(mode: str = "live") -> Layer3Config:
    return Layer3Config(
        symbol="BTCUSDT",
        pre_pump_ttl_seconds=180,
        fixed_quantity=0.01,
        execution_mode=mode,
        enable_execution=True,
        cadence_seconds=0.05,
        backoff=BackoffConfig(min_seconds=0.05, max_seconds=0.2),
        risk=Layer3RiskConfig(default_sl_buffer_pct=0.0044, tp1_r_multiple=1.5, tp2_r_multiple=2.5),
        telegram=TelegramConfig(enabled=True, bot_token="x", chat_id="y"),
    )


@pytest.mark.asyncio
async def test_layer3_executes_orders_and_sends_telegram() -> None:
    in_queue: asyncio.Queue[PrePumpEvent] = asyncio.Queue()
    out_queue: asyncio.Queue[ExecutionEvent] = asyncio.Queue(maxsize=10)
    stop_event = asyncio.Event()
    exec_client = FakeExecutionClient()
    tg_client = FakeTelegramNotifier()

    in_queue.put_nowait(_pre_pump_event("LONG"))
    task = asyncio.create_task(
        run_layer3(
            _config("live"),
            in_queue,
            out_queue=out_queue,
            stop_event=stop_event,
            execution_client=exec_client,
            telegram_notifier=tg_client,
        )
    )
    await asyncio.sleep(0.35)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert len(exec_client.calls) == 4
    assert exec_client.calls[0]["type"] == "MARKET"
    assert not out_queue.empty()
    event = out_queue.get_nowait()
    assert event.event_type == "EXECUTION_EVENT"
    assert event.plan.tp2 > event.plan.entry
    assert tg_client.messages
    assert "PHANTOM SIGNAL - BTCUSDT" in tg_client.messages[0]


@pytest.mark.asyncio
async def test_layer3_queue_drop_oldest_policy() -> None:
    in_queue: asyncio.Queue[PrePumpEvent] = asyncio.Queue()
    out_queue: asyncio.Queue[ExecutionEvent] = asyncio.Queue(maxsize=1)
    stop_event = asyncio.Event()
    exec_client = FakeExecutionClient()
    tg_client = FakeTelegramNotifier()

    out_queue.put_nowait(
        ExecutionEvent(
            event_type="EXECUTION_EVENT",
            event_id="old",
            ts_ms=0,
            symbol="BTCUSDT",
            direction="LONG",
            passed=True,
            source_pre_pump_event_id="seed",
            plan=run_prebuilt_plan(),
            order_ids={},
            raw={},
            degraded=False,
        )
    )
    in_queue.put_nowait(_pre_pump_event("LONG"))
    task = asyncio.create_task(
        run_layer3(
            _config("live"),
            in_queue,
            out_queue=out_queue,
            stop_event=stop_event,
            execution_client=exec_client,
            telegram_notifier=tg_client,
        )
    )
    await asyncio.sleep(0.35)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert out_queue.qsize() == 1
    latest = out_queue.get_nowait()
    assert latest.event_id != "old"


@pytest.mark.asyncio
async def test_layer3_paper_mode_still_sends_telegram() -> None:
    in_queue: asyncio.Queue[PrePumpEvent] = asyncio.Queue()
    out_queue: asyncio.Queue[ExecutionEvent] = asyncio.Queue(maxsize=10)
    stop_event = asyncio.Event()
    tg_client = FakeTelegramNotifier()

    config = _config("paper")
    in_queue.put_nowait(_pre_pump_event("SHORT"))
    task = asyncio.create_task(
        run_layer3(
            config,
            in_queue,
            out_queue=out_queue,
            stop_event=stop_event,
            execution_client=None,
            telegram_notifier=tg_client,
        )
    )
    await asyncio.sleep(0.35)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert not out_queue.empty()
    event = out_queue.get_nowait()
    assert event.raw["execution_mode"] == "paper"
    assert event.plan.tp2 < event.plan.entry
    assert tg_client.messages


def run_prebuilt_plan():
    from project_phantom.core.types import ExecutionPlan

    return ExecutionPlan(
        entry=1.0,
        sl=0.9,
        tp1=1.1,
        tp2=1.2,
        rr=2.0,
        sl_pct=-0.1,
        tp1_pct=0.1,
        tp2_pct=0.2,
        quantity=1.0,
        risk_amount=0.1,
    )
