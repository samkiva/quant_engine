from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
import structlog
from db.connection import get_pool

logger = structlog.get_logger()

GAP_THRESHOLD_MINUTES = 15  # Gaps larger than this break a segment


@dataclass(frozen=True)
class WalkForwardWindow:
    window_id: int
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime
    train_trade_count: int = 0
    test_trade_count: int = 0


async def get_contiguous_segments(
    table: str,
    symbol: str,
    gap_threshold_minutes: float = GAP_THRESHOLD_MINUTES,
) -> list[tuple[datetime, datetime]]:
    """
    Identifies contiguous data segments by detecting gaps.
    Returns list of (segment_start, segment_end) tuples.
    Walk-forward windows are only built within segments.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT trade_time FROM {table}
            WHERE symbol = $1
            ORDER BY trade_time ASC
        """, symbol)

    if not rows:
        return []

    times = [r["trade_time"] for r in rows]
    threshold = timedelta(minutes=gap_threshold_minutes)

    segments = []
    seg_start = times[0]
    prev = times[0]

    for t in times[1:]:
        if t - prev > threshold:
            segments.append((seg_start, prev))
            seg_start = t
        prev = t
    segments.append((seg_start, prev))

    logger.info(
        "contiguous_segments_found",
        count=len(segments),
        gap_threshold_minutes=gap_threshold_minutes,
    )
    for i, (s, e) in enumerate(segments):
        duration = (e - s).total_seconds() / 3600
        logger.debug(
            "segment",
            id=i,
            start=s,
            end=e,
            duration_hours=round(duration, 2),
        )

    return segments


async def generate_windows(
    table: str,
    symbol: str,
    train_hours: float = 1.0,
    test_hours: float = 0.5,
    min_train_trades: int = 100,
    min_test_trades: int = 50,
) -> list[WalkForwardWindow]:
    """
    Generates non-overlapping walk-forward windows from contiguous
    data segments only. Never builds windows across data gaps.
    """
    segments = await get_contiguous_segments(table, symbol)

    if not segments:
        logger.warning("no_segments_found", table=table, symbol=symbol)
        return []

    pool = get_pool()
    windows = []
    window_id = 0

    for seg_start, seg_end in segments:
        seg_hours = (seg_end - seg_start).total_seconds() / 3600
        if seg_hours < train_hours + test_hours:
            logger.debug(
                "segment_too_short",
                hours=round(seg_hours, 2),
                required=train_hours + test_hours,
            )
            continue

        cursor = seg_start
        while True:
            train_start = cursor
            train_end = cursor + timedelta(hours=train_hours)
            test_start = train_end
            test_end = test_start + timedelta(hours=test_hours)

            if test_end > seg_end:
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

            cursor = train_end

    logger.info("walk_forward_windows_generated", count=len(windows))
    return windows
