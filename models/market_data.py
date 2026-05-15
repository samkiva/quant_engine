from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass(frozen=True)
class TradeEvent:
    """
    Immutable representation of a single Binance trade stream event.

    buyer_order_id and seller_order_id are Optional — Binance removed
    these fields from the trade stream. Defensive parsing with defaults
    ensures the model survives external API schema changes.
    """
    event_type: str
    event_time: datetime
    symbol: str
    trade_id: int
    price: float
    quantity: float
    trade_time: datetime
    is_buyer_maker: bool
    buyer_order_id: Optional[int] = None
    seller_order_id: Optional[int] = None

    @classmethod
    def from_binance_message(cls, msg: dict) -> "TradeEvent":
        return cls(
            event_type=msg["e"],
            event_time=datetime.fromtimestamp(msg["E"] / 1000, tz=timezone.utc),
            symbol=msg["s"],
            trade_id=msg["t"],
            price=float(msg["p"]),
            quantity=float(msg["q"]),
            trade_time=datetime.fromtimestamp(msg["T"] / 1000, tz=timezone.utc),
            is_buyer_maker=msg["m"],
            buyer_order_id=msg.get("b"),
            seller_order_id=msg.get("a"),
        )
