import pandas as pd
import asyncpg
import structlog
from db.connection import init_db_pool, get_pool

logger = structlog.get_logger()


async def load_trades(symbol: str, limit: int = 5000) -> pd.DataFrame:
    """
    Loads historical trade data from PostgreSQL into a Pandas DataFrame.
    Sorted by trade_time ascending — required for time-series operations.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT trade_time, price, quantity, is_buyer_maker
            FROM trades
            WHERE symbol = $1
            ORDER BY trade_time ASC
            LIMIT $2
        """, symbol, limit)

    if not rows:
        logger.warning("no_trades_found", symbol=symbol)
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["trade_time", "price", "quantity", "is_buyer_maker"])
    df["price"] = df["price"].astype(float)
    df["quantity"] = df["quantity"].astype(float)
    df = df.set_index("trade_time")
    df.index = pd.to_datetime(df.index, utc=True)

    logger.info("trades_loaded", symbol=symbol, count=len(df))
    return df
