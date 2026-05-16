from datetime import datetime, timezone
import structlog
from db.connection import get_pool

logger = structlog.get_logger()


async def record_connect(symbol: str) -> int:
    """
    Records a new stream session on connect.
    Returns the session ID for use in subsequent updates.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        session_id = await conn.fetchval("""
            INSERT INTO stream_sessions (symbol, connected_at)
            VALUES ($1, $2)
            RETURNING id
        """, symbol, datetime.now(tz=timezone.utc))
    logger.info("session_started", symbol=symbol, session_id=session_id)
    return session_id


async def record_disconnect(
    session_id: int,
    trades_received: int,
    reason: str,
    is_clean: bool = False,
) -> None:
    """
    Updates the session record on disconnect.
    Captures the gap window for data integrity auditing.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE stream_sessions
            SET disconnected_at   = $1,
                disconnect_reason = $2,
                trades_received   = $3,
                is_clean_close    = $4
            WHERE id = $5
        """,
            datetime.now(tz=timezone.utc),
            reason,
            trades_received,
            is_clean,
            session_id,
        )
    logger.info(
        "session_ended",
        session_id=session_id,
        trades_received=trades_received,
        reason=reason,
        is_clean=is_clean,
    )
