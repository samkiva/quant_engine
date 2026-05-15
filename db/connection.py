import asyncpg
import structlog
from config.settings import settings

logger = structlog.get_logger()

_pool: asyncpg.Pool | None = None


async def init_db_pool() -> None:
    global _pool
    _pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=2,
        max_size=10,
        command_timeout=30,
        statement_cache_size=0,  # Required for PgBouncer transaction mode
    )
    logger.info("db_pool_created", min_size=2, max_size=10)


async def close_db_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        logger.info("db_pool_closed")


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database pool not initialised. Call init_db_pool() first.")
    return _pool
