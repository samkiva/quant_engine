from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
import structlog
from db.connection import get_pool

logger = structlog.get_logger()


@dataclass(frozen=True)
class WalkForwardWindow:
    """
    A single walk-forward period with strict train/test separation.
    Test window is always strictly AFTER train window — no overlap.
    Parameters frozen after training — never tuned on test data.
    """
    window_id: int
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime
    train_trade_count: int = 0
    test_trade_count: int = 0
    train_fingerprint: str = ""
    test_fingerprint: str = ""


async def generate_windows(
    table: str,
    symbol: str,
    train_hours: float = 1.0,
    test_hours: float = 0.5,
    min_train_trades: int = 100,
    min_test_trades: int = 50,
) -> list[WalkForwardWindow]:
    """
    Generates non-overlapping walk-forward windows from the dataset.
    Skips windows with insufficient data for statistical validity.

    train_hours: size of training window
    test_hours:  size of test window (immediately follows train)
    min_train_trades: skip window if train has fewer trades
    min_test_trades:  skip window if test has fewer trades
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        bounds = await conn.fetchrow(f"""
            SELECT MIN(trade_time) as first, MAX(trade_time) as last
            FROM {table}
            WHERE symbol = $1
        """, symbol)

    if not bounds or not bounds["first"]:
        logger.warning("no_data_for_windows", table=table, symbol=symbol)
        return []

    first = bounds["first"]
    last = bounds["last"]
    total_hours = (last - first).total_seconds() / 3600

    logger.info(
        "generating_walk_forward_windows",
        table=table,
        symbol=symbol,
        total_hours=round(total_hours, 2),
        train_hours=train_hours,
        test_hours=test_hours,
    )

    windows = []
    window_id = 0
    cursor = first

    while True:
        train_start = cursor
        train_end = cursor + timedelta(hours=train_hours)
        test_start = train_end
        test_end = test_start + timedelta(hours=test_hours)

        if test_end > last:
            break

        async with pool.acquire() as conn:
            train_count = await conn.fetchval(f"""
                SELECT COUNT(*) FROM {table}
                WHERE symbol = $1
                  AND trade_time >= $2 AND trade_time < $3
            """, symbol, train_start, train_end)

            test_count = await conn.fetchval(f"""
                SELECT COUNT(*) FROM {table}
                WHERE symbol = $1
                  AND trade_time >= $2 AND trade_time < $3
            """, symbol, test_start, test_end)

        if train_count >= min_train_trades and test_count >= min_test_trades:
            windows.append(WalkForwardWindow(
                window_id=window_id,
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
                train_trade_count=train_count,
                test_trade_count=test_count,
            ))
            window_id += 1
            logger.debug(
                "window_accepted",
                window_id=window_id,
                train_trades=train_count,
                test_trades=test_count,
            )
        else:
            logger.debug(
                "window_skipped_insufficient_data",
                train_trades=train_count,
                test_trades=test_count,
            )

        # Advance by one train window (non-overlapping)
        cursor = train_end

    logger.info("walk_forward_windows_generated", count=len(windows))
    return windows
