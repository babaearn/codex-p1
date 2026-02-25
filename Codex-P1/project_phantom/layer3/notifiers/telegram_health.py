from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable


CommandHandler = Callable[[], Awaitable[str]]


def _update_id(update: Any) -> int | None:
    if isinstance(update, dict):
        value = update.get("update_id")
        return int(value) if value is not None else None
    value = getattr(update, "update_id", None)
    return int(value) if value is not None else None


def _extract_message(update: Any) -> tuple[str | None, str | None]:
    if isinstance(update, dict):
        message = update.get("message", {})
        text = message.get("text")
        chat = message.get("chat", {})
        chat_id = chat.get("id")
        return (str(chat_id) if chat_id is not None else None, text)

    message = getattr(update, "message", None)
    if message is None:
        return (None, None)
    chat = getattr(message, "chat", None)
    chat_id = getattr(chat, "id", None) if chat is not None else None
    text = getattr(message, "text", None)
    return (str(chat_id) if chat_id is not None else None, text)


def _extract_command(text: str | None) -> str | None:
    if not text:
        return None
    normalized = text.strip().lower()
    if not normalized.startswith("/"):
        return None
    return normalized.split()[0]


class TelegramHealthService:
    def __init__(
        self,
        *,
        bot: Any,
        allowed_chat_id: str,
        command_handlers: dict[str, CommandHandler],
        poll_interval_seconds: float = 2.0,
        cooldown_seconds: float = 20.0,
    ) -> None:
        self._bot = bot
        self._allowed_chat_id = str(allowed_chat_id)
        self._command_handlers = command_handlers
        self._poll_interval_seconds = poll_interval_seconds
        self._cooldown_seconds = cooldown_seconds
        self._last_update_id: int | None = None
        self._last_command_ts: dict[str, float] = {}

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                updates = await self._bot.get_updates(
                    offset=(self._last_update_id + 1) if self._last_update_id is not None else None,
                    timeout=max(1, int(self._poll_interval_seconds)),
                    allowed_updates=["message"],
                )
                for update in updates:
                    await self._handle_update(update)
                    seen_id = _update_id(update)
                    if seen_id is not None:
                        self._last_update_id = seen_id
            except Exception:
                # Keep listener alive even if Telegram API has transient failures.
                pass

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._poll_interval_seconds)
            except asyncio.TimeoutError:
                continue

    async def _handle_update(self, update: Any) -> None:
        chat_id, text = _extract_message(update)
        command = _extract_command(text)
        if command not in self._command_handlers:
            return
        if chat_id != self._allowed_chat_id:
            await self._bot.send_message(
                chat_id=chat_id,
                text=f"<pre>Unauthorized {command} request</pre>",
                parse_mode="HTML",
            )
            return

        now = time.time()
        last_run = self._last_command_ts.get(command, 0.0)
        remaining = self._cooldown_seconds - (now - last_run)
        if remaining > 0:
            await self._bot.send_message(
                chat_id=chat_id,
                text=f"<pre>{command} cooldown: wait {remaining:.1f}s</pre>",
                parse_mode="HTML",
            )
            return

        self._last_command_ts[command] = now
        if command == "/health":
            await self._bot.send_message(chat_id=chat_id, text="<pre>Running PHANTOM health checks...</pre>", parse_mode="HTML")
        report = await self._command_handlers[command]()
        await self._bot.send_message(chat_id=chat_id, text=report, parse_mode="HTML", disable_web_page_preview=True)
