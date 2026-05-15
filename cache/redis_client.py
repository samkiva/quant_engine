import redis.asyncio as aioredis
import structlog
from config.settings import settings

logger = structlog.get_logger()

_redis: aioredis.Redis | None = None


async def init_redis() -> None:
    """Initialises the async Redis client. Called once at startup."""
    global _redis
    _redis = aioredis.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=True,
    )
    await _redis.ping()
    logger.info("redis_connected")


async def close_redis() -> None:
    global _redis
    if _redis:
        await _redis.aclose()
        logger.info("redis_closed")


def get_redis() -> aioredis.Redis:
    if _redis is None:
        raise RuntimeError("Redis not initialised. Call init_redis() first.")
    return _redis


async def set_latest_price(symbol: str, price: float) -> None:
    r = get_redis()
    await r.set(f"price:{symbol}", str(price))


async def get_latest_price(symbol: str) -> float | None:
    r = get_redis()
    value = await r.get(f"price:{symbol}")
    return float(value) if value is not None else None
