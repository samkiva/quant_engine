from datetime import datetime, timezone
import structlog
from models.market_data import TradeEvent
from storage.in_memory_buffer import TradeBuffer
from cache.redis_client import set_latest_price
from core.write_queue import enqueue

logger = structlog.get_logger()


class TradeStreamHandler:
    """
    Receives raw WebSocket messages, parses them into TradeEvents,
    updates the in-memory buffer and Redis cache synchronously,
    and enqueues DB persistence asynchronously via the write queue.

    The signal path is now:
    tick → parse → buffer → Redis → enqueue (non-blocking) → return

    DB writes happen in the background write worker.
    This eliminates the 50-200ms database latency from the signal path.
    """

    def __init__(self, buffer: TradeBuffer) -> None:
        self._buffer = buffer

    async def handle(self, raw_message: dict) -> None:
        try:
            event = TradeEvent.from_binance_message(raw_message)
            self._buffer.append(event)

            # Redis update — fast, stays in signal path (~10ms)
            await set_latest_price(event.symbol, event.price)

            # DB write — enqueued, never blocks signal path
            enqueue("trade", {
                "trade_id": event.trade_id,
                "symbol": event.symbol,
                "price": event.price,
                "quantity": event.quantity,
                "is_buyer_maker": event.is_buyer_maker,
                "trade_time": event.trade_time,
                "event_time": event.event_time,
            })

            logger.debug(
                "trade_received",
                symbol=event.symbol,
                price=event.price,
                quantity=event.quantity,
                buffer_size=self._buffer.size(),
            )

        except KeyError as e:
            logger.warning("trade_parse_failed", missing_field=str(e))
        except Exception as e:
            logger.error("trade_handler_error", error=str(e), exc_info=True)
