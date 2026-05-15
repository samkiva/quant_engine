import asyncio
from core.logging_setup import configure_logging
from db.connection import init_db_pool, close_db_pool
from db.queries import ensure_schema
from cache.redis_client import init_redis, close_redis
from services.market_data.binance_ws_client import connect_trade_stream
from services.market_data.stream_handler import TradeStreamHandler
from storage.in_memory_buffer import TradeBuffer

configure_logging()


async def main() -> None:
    await init_db_pool()
    await init_redis()
    await ensure_schema()

    buffer = TradeBuffer()
    handler = TradeStreamHandler(buffer)

    try:
        await connect_trade_stream(
            symbol="BTCUSDT",
            on_message=handler.handle,
        )
    finally:
        await close_db_pool()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
