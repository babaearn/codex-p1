from __future__ import annotations

from project_phantom.config import TelegramConfig


class TelegramBotNotifier:
    name = "telegram_bot"

    def __init__(self, bot: object, chat_id: str) -> None:
        self._bot = bot
        self._chat_id = chat_id

    @classmethod
    async def create(cls, config: TelegramConfig) -> "TelegramBotNotifier":
        if not config.bot_token or not config.chat_id:
            raise RuntimeError("TG_BOT_TOKEN and TG_CHAT_ID are required for telegram notifications")
        try:
            from telegram import Bot  # type: ignore
        except ModuleNotFoundError as exc:
            raise RuntimeError("python-telegram-bot is required for telegram notifications") from exc

        bot = Bot(token=config.bot_token)
        return cls(bot=bot, chat_id=config.chat_id)

    async def send_message(self, text: str) -> None:
        await self._bot.send_message(
            chat_id=self._chat_id,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

    async def close(self) -> None:
        return None
