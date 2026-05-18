from fastapi import APIRouter, HTTPException
from typing import Optional
from db.connection import get_pool

router = APIRouter()


@router.get("/paper/signals/{symbol}")
async def recent_signals(symbol: str, limit: int = 50, session_id: Optional[int] = None):
    symbol = symbol.upper()
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                strategy_name, symbol, signal,
                tick_price, tick_timestamp, generated_at,
                latency_ms, reason, risk_blocked,
                block_reason, post_reconnect, session_id
            FROM signal_log
            WHERE symbol = $1
              AND ($2::bigint IS NULL OR session_id = $2)
            ORDER BY generated_at DESC
            LIMIT $3
        """, symbol, session_id, limit)
    if not rows:
        raise HTTPException(status_code=404, detail="No signals found")
    return [dict(r) for r in rows]


@router.get("/paper/latency/{symbol}")
async def latency_stats(symbol: str, session_id: Optional[int] = None):
    symbol = symbol.upper()
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                COUNT(*)            AS total_signals,
                AVG(latency_ms)     AS avg_latency_ms,
                MIN(latency_ms)     AS min_latency_ms,
                MAX(latency_ms)     AS max_latency_ms,
                PERCENTILE_CONT(0.50) WITHIN GROUP
                    (ORDER BY latency_ms) AS p50_latency_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP
                    (ORDER BY latency_ms) AS p95_latency_ms,
                PERCENTILE_CONT(0.99) WITHIN GROUP
                    (ORDER BY latency_ms) AS p99_latency_ms,
                COUNT(*) FILTER (WHERE risk_blocked)   AS risk_blocked_count,
                COUNT(*) FILTER (WHERE post_reconnect) AS post_reconnect_count
            FROM signal_log
            WHERE symbol = $1
              AND ($2::bigint IS NULL OR session_id = $2)
        """, symbol, session_id)
    return {
        "symbol": symbol,
        "session_id": session_id,
        "total_signals": row["total_signals"],
        "avg_latency_ms": round(float(row["avg_latency_ms"] or 0), 3),
        "min_latency_ms": round(float(row["min_latency_ms"] or 0), 3),
        "max_latency_ms": round(float(row["max_latency_ms"] or 0), 3),
        "p50_latency_ms": round(float(row["p50_latency_ms"] or 0), 3),
        "p95_latency_ms": round(float(row["p95_latency_ms"] or 0), 3),
        "p99_latency_ms": round(float(row["p99_latency_ms"] or 0), 3),
        "risk_blocked_count": row["risk_blocked_count"],
        "post_reconnect_count": row["post_reconnect_count"],
    }


@router.get("/paper/sessions/{symbol}")
async def session_health(symbol: str):
    symbol = symbol.upper()
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                id, connected_at, disconnected_at,
                disconnect_reason, trades_received,
                is_clean_close,
                EXTRACT(EPOCH FROM (
                    COALESCE(disconnected_at, NOW()) - connected_at
                ))::INTEGER AS duration_seconds
            FROM stream_sessions
            WHERE symbol = $1
            ORDER BY connected_at DESC
            LIMIT 10
        """, symbol)
    return [dict(r) for r in rows]


@router.get("/paper/portfolio/{session_id}")
async def portfolio_history(session_id: int):
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                recorded_at, cash, position_side,
                position_price, position_qty,
                portfolio_value, total_pnl, cause
            FROM portfolio_state_log
            WHERE session_id = $1
            ORDER BY recorded_at ASC
        """, session_id)
    if not rows:
        raise HTTPException(status_code=404, detail="No portfolio history found")
    return [dict(r) for r in rows]
