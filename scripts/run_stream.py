import asyncio
from core.logging_setup import configure_logging
from services.market_data.binance_ws_client import connect_trade_stream
from services.market_data.stream_handler import TradeStreamHandler
from storage.in_memory_buffer import TradeBuffer

configure_logging()


async def main() -> None:
    buffer = TradeBuffer()
    handler = TradeStreamHandler(buffer)

    await connect_trade_stream(
        symbol="BTCUSDT",
        on_message=handler.handle,
    )


if __name__ == "__main__":
    asyncio.run(main())
