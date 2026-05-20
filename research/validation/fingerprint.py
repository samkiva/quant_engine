import hashlib
from datetime import datetime
from typing import Optional
import structlog
from db.connection import get_pool

logger = structlog.get_logger()


async def fingerprint_dataset(
    table: str,
    symbol: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> dict:
    """
    Produces a deterministic fingerprint of a dataset window.
    Two experiments with identical fingerprints used identical data.

    Fingerprint is SHA-256 of sorted trade IDs in the window.
    Changing any trade in the window changes the fingerprint.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT trade_id
            FROM {table}
            WHERE symbol = $1
              AND ($2::timestamptz IS NULL OR trade_time >= $2)
              AND ($3::timestamptz IS NULL OR trade_time <= $3)
            ORDER BY trade_id ASC
        """, symbol, start_time, end_time)

    if not rows:
        return {
            "fingerprint": "empty",
            "table": table,
            "symbol": symbol,
            "trade_count": 0,
            "start_time": start_time,
            "end_time": end_time,
        }

    trade_ids = [str(r["trade_id"]) for r in rows]
    content = f"{table}:{symbol}:" + ",".join(trade_ids)
    fingerprint = hashlib.sha256(content.encode()).hexdigest()[:16]

    logger.info(
        "dataset_fingerprinted",
        fingerprint=fingerprint,
        table=table,
        symbol=symbol,
        trade_count=len(rows),
    )

    return {
        "fingerprint": fingerprint,
        "table": table,
        "symbol": symbol,
        "trade_count": len(trade_ids),
        "start_time": start_time,
        "end_time": end_time,
    }
