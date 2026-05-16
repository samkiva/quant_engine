import asyncio
from datetime import timezone
from core.logging_setup import configure_logging
from db.connection import init_db_pool, close_db_pool, get_pool

configure_logging()


async def main() -> None:
    await init_db_pool()
    pool = get_pool()

    async with pool.acquire() as conn:

        # Basic counts and time span
        basic = await conn.fetchrow("""
            SELECT
                COUNT(*)                                    AS total_trades,
                MIN(trade_time)                             AS first_trade,
                MAX(trade_time)                             AS last_trade,
                EXTRACT(EPOCH FROM (
                    MAX(trade_time) - MIN(trade_time)
                )) / 3600.0                                 AS duration_hours,
                COUNT(DISTINCT DATE_TRUNC('minute', trade_time)) AS active_minutes
            FROM trades
            WHERE symbol = 'BTCUSDT'
        """)

        # Duplicate detection
        duplicates = await conn.fetchrow("""
            SELECT
                COUNT(*) - COUNT(DISTINCT (symbol, trade_id)) AS duplicate_count,
                COUNT(DISTINCT trade_id)                       AS unique_trade_ids
            FROM trades
            WHERE symbol = 'BTCUSDT'
        """)

        # Timestamp consistency — trades where event_time and trade_time diverge > 1 second
        timestamp_anomalies = await conn.fetchval("""
            SELECT COUNT(*)
            FROM trades
            WHERE symbol = 'BTCUSDT'
              AND ABS(EXTRACT(EPOCH FROM (event_time - trade_time))) > 1
        """)

        # Gap detection — intervals between consecutive trades > 60 seconds
        gaps = await conn.fetch("""
            WITH ordered AS (
                SELECT
                    trade_time,
                    LEAD(trade_time) OVER (ORDER BY trade_time) AS next_time,
                    EXTRACT(EPOCH FROM (
                        LEAD(trade_time) OVER (ORDER BY trade_time) - trade_time
                    ))                                           AS gap_seconds
                FROM trades
                WHERE symbol = 'BTCUSDT'
            )
            SELECT
                trade_time      AS gap_start,
                next_time       AS gap_end,
                gap_seconds
            FROM ordered
            WHERE gap_seconds > 60
            ORDER BY gap_seconds DESC
            LIMIT 20
        """)

        # Burst rate — max trades in any single second
        burst = await conn.fetchrow("""
            SELECT
                DATE_TRUNC('second', trade_time) AS second_bucket,
                COUNT(*)                          AS trades_in_second
            FROM trades
            WHERE symbol = 'BTCUSDT'
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 1
        """)

        # Average trades per minute
        avg_rate = await conn.fetchval("""
            SELECT AVG(trade_count) FROM (
                SELECT
                    DATE_TRUNC('minute', trade_time) AS minute_bucket,
                    COUNT(*)                          AS trade_count
                FROM trades
                WHERE symbol = 'BTCUSDT'
                GROUP BY 1
            ) t
        """)

        # Price range and volatility check
        price_stats = await conn.fetchrow("""
            SELECT
                MIN(price)                              AS min_price,
                MAX(price)                              AS max_price,
                AVG(price)                              AS avg_price,
                STDDEV(price::float)                    AS price_stddev,
                MAX(price) - MIN(price)                 AS price_range
            FROM trades
            WHERE symbol = 'BTCUSDT'
        """)

        # Session summary
        sessions = await conn.fetchrow("""
            SELECT
                COUNT(*)                                        AS total_sessions,
                COUNT(*) FILTER (WHERE is_clean_close = TRUE)  AS clean_closes,
                COUNT(*) FILTER (WHERE is_clean_close = FALSE) AS unclean_closes,
                AVG(trades_received)                            AS avg_trades_per_session,
                MAX(trades_received)                            AS max_trades_per_session,
                MIN(trades_received) FILTER (
                    WHERE disconnected_at IS NOT NULL
                )                                               AS min_trades_per_session,
                AVG(EXTRACT(EPOCH FROM (
                    disconnected_at - connected_at
                ))) FILTER (
                    WHERE disconnected_at IS NOT NULL
                )                                               AS avg_session_duration_secs
            FROM stream_sessions
            WHERE symbol = 'BTCUSDT'
        """)

        # Reconnect frequency
        reconnect_freq = await conn.fetchval("""
            SELECT
                COUNT(*) / NULLIF(
                    EXTRACT(EPOCH FROM (
                        MAX(connected_at) - MIN(connected_at)
                    )) / 3600.0,
                0)
            FROM stream_sessions
            WHERE symbol = 'BTCUSDT'
        """)

    print("\n" + "="*55)
    print("       DATASET AUDIT REPORT — BTCUSDT TESTNET")
    print("="*55)

    print("\n── COLLECTION OVERVIEW ──")
    print(f"  Total ticks collected:      {basic['total_trades']:,}")
    print(f"  Collection duration:        {basic['duration_hours']:.2f} hours")
    print(f"  Active minutes:             {basic['active_minutes']:,}")
    print(f"  First trade:                {basic['first_trade']}")
    print(f"  Last trade:                 {basic['last_trade']}")

    print("\n── STREAM SESSIONS ──")
    print(f"  Total sessions:             {sessions['total_sessions']:,}")
    print(f"  Clean closes:               {sessions['clean_closes']:,}")
    print(f"  Unclean closes:             {sessions['unclean_closes']:,}")
    print(f"  Avg trades/session:         {sessions['avg_trades_per_session']:.1f}")
    print(f"  Max trades/session:         {sessions['max_trades_per_session']:,}")
    print(f"  Avg session duration:       {sessions['avg_session_duration_secs']:.1f}s")
    print(f"  Reconnect frequency:        {reconnect_freq:.2f}/hour")

    print("\n── DATA INTEGRITY ──")
    print(f"  Duplicate events:           {duplicates['duplicate_count']:,}")
    print(f"  Unique trade IDs:           {duplicates['unique_trade_ids']:,}")
    print(f"  Timestamp anomalies:        {timestamp_anomalies:,}")
    print(f"  Gaps > 60s detected:        {len(gaps):,}")

    print("\n── INGESTION RATE ──")
    print(f"  Avg trades/minute:          {avg_rate:.2f}")
    print(f"  Max burst (trades/second):  {burst['trades_in_second']:,}")
    print(f"    └─ at:                    {burst['second_bucket']}")

    print("\n── PRICE STATISTICS ──")
    print(f"  Min price:                  ${float(price_stats['min_price']):,.2f}")
    print(f"  Max price:                  ${float(price_stats['max_price']):,.2f}")
    print(f"  Avg price:                  ${float(price_stats['avg_price']):,.2f}")
    print(f"  Price range:                ${float(price_stats['price_range']):,.2f}")
    print(f"  Price std dev:              ${float(price_stats['price_stddev']):,.2f}")

    if gaps:
        print("\n── GAPS > 60 SECONDS (top 5) ──")
        for i, gap in enumerate(gaps[:5]):
            print(f"  [{i+1}] {gap['gap_start']} → {gap['gap_end']}")
            print(f"       duration: {gap['gap_seconds']:.0f}s "
                  f"({gap['gap_seconds']/60:.1f} min)")

    print("\n── INTEGRITY VERDICT ──")
    issues = []
    if duplicates['duplicate_count'] > 0:
        issues.append(f"  ⚠ {duplicates['duplicate_count']} duplicate trades detected")
    if timestamp_anomalies > 0:
        issues.append(f"  ⚠ {timestamp_anomalies} timestamp anomalies (event_time vs trade_time > 1s)")
    if len(gaps) > 0:
        issues.append(f"  ⚠ {len(gaps)} gaps > 60s — these windows have no tick coverage")
    if sessions['unclean_closes'] > sessions['total_sessions'] * 0.5:
        issues.append("  ⚠ >50% of sessions closed uncleanly — reconnect logic under stress")

    if issues:
        for issue in issues:
            print(issue)
    else:
        print("  ✓ No integrity issues detected")

    print("\n" + "="*55 + "\n")

    await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
