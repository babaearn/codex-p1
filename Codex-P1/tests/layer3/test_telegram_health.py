from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest

from project_phantom.layer3.notifiers.telegram_health import TelegramHealthService


@dataclass
class FakeBot:
    updates: list[dict[str, Any]]
    sent: list[dict[str, Any]] = field(default_factory=list)

    async def get_updates(self, offset=None, timeout=0, allowed_updates=None):  # noqa: ANN001
        _ = (offset, timeout, allowed_updates)
        batch = list(self.updates)
        self.updates.clear()
        return batch

    async def send_message(self, **kwargs: Any) -> None:
        self.sent.append(kwargs)


@pytest.mark.asyncio
async def test_telegram_health_service_replies_to_health_command() -> None:
    bot = FakeBot(
        updates=[
            {
                "update_id": 1,
                "message": {
                    "text": "/health",
                    "chat": {"id": "123"},
                },
            }
        ]
    )

    async def builder() -> str:
        return "<pre>OK</pre>"

    stop_event = asyncio.Event()
    service = TelegramHealthService(
        bot=bot,
        allowed_chat_id="123",
        command_handlers={"/health": builder},
        poll_interval_seconds=0.01,
        cooldown_seconds=0.01,
    )

    task = asyncio.create_task(service.run(stop_event))
    await asyncio.sleep(0.08)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert len(bot.sent) >= 2
    assert "Running PHANTOM health checks" in bot.sent[0]["text"]
    assert bot.sent[1]["text"] == "<pre>OK</pre>"


@pytest.mark.asyncio
async def test_telegram_health_service_rejects_unauthorized_chat() -> None:
    bot = FakeBot(
        updates=[
            {
                "update_id": 2,
                "message": {
                    "text": "/health",
                    "chat": {"id": "999"},
                },
            }
        ]
    )

    async def builder() -> str:
        return "<pre>OK</pre>"

    stop_event = asyncio.Event()
    service = TelegramHealthService(
        bot=bot,
        allowed_chat_id="123",
        command_handlers={"/health": builder},
        poll_interval_seconds=0.01,
        cooldown_seconds=0.01,
    )

    task = asyncio.create_task(service.run(stop_event))
    await asyncio.sleep(0.05)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert bot.sent
    assert "Unauthorized" in bot.sent[0]["text"]


@pytest.mark.asyncio
async def test_telegram_health_service_supports_stats_and_mode_commands() -> None:
    bot = FakeBot(
        updates=[
            {"update_id": 3, "message": {"text": "/stats", "chat": {"id": "123"}}},
            {"update_id": 4, "message": {"text": "/mode", "chat": {"id": "123"}}},
        ]
    )

    async def health_builder() -> str:
        return "<pre>HEALTH</pre>"

    async def stats_builder() -> str:
        return "<pre>STATS</pre>"

    async def mode_builder() -> str:
        return "<pre>MODE</pre>"

    stop_event = asyncio.Event()
    service = TelegramHealthService(
        bot=bot,
        allowed_chat_id="123",
        command_handlers={
            "/health": health_builder,
            "/stats": stats_builder,
            "/mode": mode_builder,
        },
        poll_interval_seconds=0.01,
        cooldown_seconds=0.01,
    )

    task = asyncio.create_task(service.run(stop_event))
    await asyncio.sleep(0.06)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    texts = [item["text"] for item in bot.sent]
    assert "<pre>STATS</pre>" in texts
    assert "<pre>MODE</pre>" in texts
