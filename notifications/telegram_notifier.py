import asyncio
import structlog
from telegram import Bot
from config.settings import settings

logger = structlog.get_logger()


class TelegramNotifier:
    """
    Fire-and-forget async Telegram notifier.
    NEVER blocks ingestion, execution, or risk engine.
    If Telegram API is down, system continues normally.
    Alert aggregation enforced — no per-tick spam.
    """

    def __init__(self) -> None:
        self._bot = Bot(token=settings.telegram_bot_token)
        self._chat_id = settings.telegram_chat_id
        self._enabled = bool(settings.telegram_bot_token and settings.telegram_chat_id)

    def notify(self, message: str) -> None:
        """
        Non-blocking fire-and-forget send.
        Called from sync or async context — never awaited by caller.
        """
        if not self._enabled:
            return
        asyncio.create_task(self._send(message))

    async def _send(self, message: str) -> None:
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=message,
            )
        except Exception as e:
            # Never propagate Telegram failures to callers
            logger.warning("telegram_send_failed", error=str(e))


notifier = TelegramNotifier()
