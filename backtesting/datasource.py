from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class Tick:
    """
    A single price observation fed to the backtesting engine.
    Deliberately minimal — only what a strategy needs to make decisions.
    Immutable by design: a tick represents a historical fact.
    """
    timestamp: datetime
    symbol: str
    price: float
    quantity: float
    is_buyer_maker: bool


class DataSource(ABC):
    """
    Abstract base class for all data sources.

    The backtesting engine depends on this interface, not on any
    concrete implementation. This means:
    - Switching from historical to live data requires zero engine changes
    - Testing the engine uses a fake/mock data source
    - Multiple data sources can be swapped at runtime via config
    """

    @abstractmethod
    async def stream(self) -> AsyncIterator[Tick]:
        """
        Yields ticks in chronological order.
        Must be implemented by all concrete data sources.
        """
        ...

    @abstractmethod
    async def count(self) -> int:
        """Returns the total number of ticks available."""
        ...


class PostgresDataSource(DataSource):
    """
    Replays historical trade data from PostgreSQL in chronological order.
    Supports optional time window filtering for walk-forward validation.
    """

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
