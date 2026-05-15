from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class TradeEvent:
    """
    Immutable representation of a single Binance trade stream event.
    Raw market data is a historical fact — it must not be mutated.
    """
    event_type: str
    event_time: datetime
    symbol: str
    trade_id: int
    price: float
    quantity: float
    buyer_order_id: int
    seller_order_id: int
    trade_time: datetime
    is_buyer_maker: bool

    @classmethod
    def from_binance_message(cls, msg: dict) -> "TradeEvent":
        return cls(
            event_type=msg["e"],
            event_time=datetime.utcfromtimestamp(msg["E"] / 1000),
            symbol=msg["s"],
            trade_id=msg["t"],
            price=float(msg["p"]),
            quantity=float(msg["q"]),
            buyer_order_id=msg["b"],
            seller_order_id=msg["a"],
            trade_time=datetime.utcfromtimestamp(msg["T"] / 1000),
            is_buyer_maker=msg["m"],
        )
