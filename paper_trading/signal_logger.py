from datetime import datetime, timezone
import structlog
from db.connection import get_pool
from backtesting.strategy import StrategySignal

logger = structlog.get_logger()


async def log_signal(
    signal: StrategySignal,
    strategy_name: str,
    generated_at: datetime,
    session_id: int | None = None,
    post_reconnect: bool = False,
    risk_blocked: bool = False,
    block_reason: str | None = None,
) -> None:
    """
    Persists every signal decision to the signal_log table.
    Records both executed and risk-blocked signals — the audit trail
    must be complete regardless of whether a signal was acted upon.

    latency_ms measures the delay between when the tick occurred on
    Binance's servers (tick_timestamp) and when our strategy generated
    a signal (generated_at). This is end-to-end pipeline latency.
    """
    tick = signal.tick
    latency_ms = (
        (generated_at - tick.timestamp).total_seconds() * 1000
    )

    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO signal_log (
                strategy_name, symbol, signal,
                tick_price, tick_timestamp, generated_at,
                latency_ms, reason, risk_blocked, block_reason,
                session_id, post_reconnect
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11, $12
            )
        """,
            strategy_name,
            tick.symbol,
            signal.signal.value,
            tick.price,
            tick.timestamp,
            generated_at,
            round(latency_ms, 3),
            signal.reason,
            risk_blocked,
            block_reason,
            session_id,
            post_reconnect,
        )

    logger.debug(
        "signal_logged",
        strategy=strategy_name,
        signal=signal.signal.value,
        price=tick.price,
        latency_ms=round(latency_ms, 1),
        risk_blocked=risk_blocked,
        post_reconnect=post_reconnect,
    )
