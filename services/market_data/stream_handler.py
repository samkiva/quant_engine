import structlog
from models.market_data import TradeEvent
from storage.in_memory_buffer import TradeBuffer

logger = structlog.get_logger()


class TradeStreamHandler:
    """
    Receives raw WebSocket messages, parses them into TradeEvents,
    and writes to the buffer.

    Deliberately knows nothing about the WebSocket connection itself.
    """

    def __init__(self, buffer: TradeBuffer) -> None:
        self._buffer = buffer

    async def handle(self, raw_message: dict) -> None:
        try:
            event = TradeEvent.from_binance_message(raw_message)
            self._buffer.append(event)
            logger.debug(
                "trade_received",
                symbol=event.symbol,
                price=event.price,
                quantity=event.quantity,
                buffer_size=self._buffer.size(),
            )
        except KeyError as e:
            logger.warning(
                "trade_parse_failed",
                missing_field=str(e),
                raw=raw_message,
            )
