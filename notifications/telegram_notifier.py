import asyncio
import structlog
from telegram import Bot
from config.settings import settings

logger = structlog.get_logger()


class TelegramNotifier:
    def __init__(self) -> None:
        self._bot = Bot(token=settings.telegram_bot_token)
        self._chat_id = settings.telegram_chat_id
        self._enabled = bool(settings.telegram_bot_token and settings.telegram_chat_id)

    def notify(self, message: str) -> None:
        """Fire-and-forget. Never blocks execution path."""
        if not self._enabled:
            return
        try:
            asyncio.create_task(self._send(message))
        except RuntimeError:
            pass  # No event loop running — skip silently

    async def _send(self, message: str) -> None:
        try:
            await self._bot.send_message(chat_id=self._chat_id, text=message)
        except Exception as e:
            logger.warning("telegram_send_failed", error=str(e))


notifier = TelegramNotifier()
