import structlog
from models.market_data import TradeEvent
from storage.in_memory_buffer import TradeBuffer
from db.queries import insert_trade
from cache.redis_client import set_latest_price

logger = structlog.get_logger()


class TradeStreamHandler:
    def __init__(self, buffer: TradeBuffer) -> None:
        self._buffer = buffer

    async def handle(self, raw_message: dict) -> None:
        try:
            event = TradeEvent.from_binance_message(raw_message)
            self._buffer.append(event)
            await insert_trade(event)
            await set_latest_price(event.symbol, event.price)
            logger.debug(
                "trade_processed",
                symbol=event.symbol,
                price=event.price,
                quantity=event.quantity,
                buffer_size=self._buffer.size(),
            )
        except KeyError as e:
            logger.warning("trade_parse_failed", missing_field=str(e))
        except Exception as e:
            logger.error("trade_handler_error", error=str(e), exc_info=True)
