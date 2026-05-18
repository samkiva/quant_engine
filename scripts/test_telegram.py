import asyncio
from config.settings import settings

async def main():
    from telegram import Bot
    bot = Bot(token=settings.telegram_bot_token)
    await bot.send_message(
        chat_id=settings.telegram_chat_id,
        text="quant_engine online\nmainnet collector active\n1.5M trades collected"
    )
    print("Message sent successfully")

asyncio.run(main())
