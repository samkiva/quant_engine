from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass(frozen=True)
class Tick:
    """
    A single price observation fed to the backtesting engine.
    
    timestamp:   when the trade occurred on Binance's matching engine
    received_at: when our system received this tick (local clock)
    
    Use (received_at - timestamp) for clock skew measurement.
    Use (generated_at - received_at) for true processing latency.
    """
    timestamp: datetime
    symbol: str
    price: float
    quantity: float
    is_buyer_maker: bool
    received_at: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )


class DataSource(ABC):
    @abstractmethod
    async def stream(self) -> AsyncIterator[Tick]:
        ...

    @abstractmethod
    async def count(self) -> int:
        ...


class PostgresDataSource(DataSource):
    def __init__(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        batch_size: int = 500,
    ) -> None:
        self._symbol = symbol
        self._start_time = start_time
        self._end_time = end_time
        self._batch_size = batch_size

    async def stream(self) -> AsyncIterator[Tick]:
        from db.connection import get_pool
        pool = get_pool()
        offset = 0
        now = datetime.now(tz=timezone.utc)

        while True:
            async with pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT trade_time, symbol, price, quantity, is_buyer_maker
                    FROM trades
                    WHERE symbol = $1
                      AND ($2::timestamptz IS NULL OR trade_time >= $2)
                      AND ($3::timestamptz IS NULL OR trade_time <= $3)
                    ORDER BY trade_time ASC
                    LIMIT $4 OFFSET $5
                """,
                    self._symbol,
                    self._start_time,
                    self._end_time,
                    self._batch_size,
                    offset,
                )

            if not rows:
                return

            for row in rows:
                yield Tick(
                    timestamp=row["trade_time"],
                    symbol=row["symbol"],
                    price=float(row["price"]),
                    quantity=float(row["quantity"]),
                    is_buyer_maker=row["is_buyer_maker"],
                    received_at=now,  # Historical replay — use load time
                )

            offset += self._batch_size

    async def count(self) -> int:
        from db.connection import get_pool
        pool = get_pool()
        async with pool.acquire() as conn:
            return await conn.fetchval("""
                SELECT COUNT(*) FROM trades
                WHERE symbol = $1
                  AND ($2::timestamptz IS NULL OR trade_time >= $2)
                  AND ($3::timestamptz IS NULL OR trade_time <= $3)
            """, self._symbol, self._start_time, self._end_time)
