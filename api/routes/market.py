from fastapi import APIRouter, HTTPException
from db.connection import get_pool
from cache.redis_client import get_latest_price

router = APIRouter()


@router.get("/price/{symbol}")
async def current_price(symbol: str):
    symbol = symbol.upper()
    price = await get_latest_price(symbol)
    if price is None:
        raise HTTPException(status_code=404, detail=f"No price data for {symbol}")
    return {"symbol": symbol, "price": price}


@router.get("/stats/{symbol}")
async def trade_stats(symbol: str):
    symbol = symbol.upper()
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                COUNT(*)                            AS total_trades,
                MIN(price)                          AS low,
                MAX(price)                          AS high,
                AVG(price)                          AS avg_price,
                SUM(quantity)                       AS total_volume,
                MIN(trade_time)                     AS first_seen,
                MAX(trade_time)                     AS last_seen
            FROM trades
            WHERE symbol = $1
        """, symbol)
    if not row or row["total_trades"] == 0:
        raise HTTPException(status_code=404, detail=f"No data for {symbol}")
    return {
        "symbol": symbol,
        "total_trades": row["total_trades"],
        "low": float(row["low"]),
        "high": float(row["high"]),
        "avg_price": round(float(row["avg_price"]), 2),
        "total_volume": float(row["total_volume"]),
        "first_seen": row["first_seen"],
        "last_seen": row["last_seen"],
    }
