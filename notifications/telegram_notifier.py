from telegram import Bot
from config.settings import settings


class TelegramNotifier:
    def __init__(self) -> None:
        self.bot = Bot(token=settings.telegram_bot_token)
        self.chat_id = settings.telegram_chat_id

    async def send_message(self, message: str) -> None:
        await self.bot.send_message(
            chat_id=self.chat_id,
            text=message,
        )


telegram_notifier = TelegramNotifier()

