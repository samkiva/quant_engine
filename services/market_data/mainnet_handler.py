from datetime import datetime, timezone
import structlog
from models.market_data import TradeEvent
from core.write_queue import enqueue

logger = structlog.get_logger()


class MainnetTradeHandler:
    """
    Handles mainnet trade stream events.
    Writes to mainnet_trades table — completely separate from testnet data.

    No in-memory buffer — mainnet tick rate is too high for
    bounded deque to be useful for research purposes.
    All persistence goes through the async write queue.
    """

    def __init__(self) -> None:
        self._count: int = 0

    async def handle(self, raw_message: dict) -> None:
        try:
            event = TradeEvent.from_binance_message(raw_message)
            self._count += 1

            enqueue("mainnet_trade", {
                "trade_id": event.trade_id,
                "symbol": event.symbol,
                "price": event.price,
                "quantity": event.quantity,
                "is_buyer_maker": event.is_buyer_maker,
                "trade_time": event.trade_time,
                "event_time": event.event_time,
            })

            if self._count % 1000 == 0:
                logger.info(
                    "mainnet_ingestion_milestone",
                    symbol=event.symbol,
                    count=self._count,
                    price=event.price,
                )

        except KeyError as e:
            logger.warning("mainnet_parse_failed", missing_field=str(e))
        except Exception as e:
            logger.error("mainnet_handler_error", error=str(e), exc_info=True)
