import asyncpg
import structlog
from db.connection import get_pool
from models.market_data import TradeEvent

logger = structlog.get_logger()

CREATE_TRADES_TABLE = """
    CREATE TABLE IF NOT EXISTS trades (
        id              BIGSERIAL PRIMARY KEY,
        trade_id        BIGINT NOT NULL,
        symbol          TEXT NOT NULL,
        price           NUMERIC(20, 8) NOT NULL,
        quantity        NUMERIC(20, 8) NOT NULL,
        is_buyer_maker  BOOLEAN NOT NULL,
        trade_time      TIMESTAMPTZ NOT NULL,
        event_time      TIMESTAMPTZ NOT NULL,
        inserted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(symbol, trade_id)
    );
    CREATE INDEX IF NOT EXISTS idx_trades_symbol_time
        ON trades(symbol, trade_time DESC);
"""

INSERT_TRADE = """
    INSERT INTO trades
        (trade_id, symbol, price, quantity, is_buyer_maker, trade_time, event_time)
    VALUES
        ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (symbol, trade_id) DO NOTHING;
"""


async def ensure_schema() -> None:
    """Creates tables and indexes if they don't exist."""
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(CREATE_TRADES_TABLE)
    logger.info("db_schema_ready")


async def insert_trade(event: TradeEvent) -> None:
    """Persists a single trade event. Silently ignores duplicates."""
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            INSERT_TRADE,
            event.trade_id,
            event.symbol,
            event.price,
            event.quantity,
            event.is_buyer_maker,
            event.trade_time,
            event.event_time,
        )
