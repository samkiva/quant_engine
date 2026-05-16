from contextlib import asynccontextmanager
from fastapi import FastAPI
from core.logging_setup import configure_logging
from db.connection import init_db_pool, close_db_pool
from cache.redis_client import init_redis, close_redis
from api.routes.health import router as health_router
from api.routes.market import router as market_router
from api.routes.paper import router as paper_router

configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db_pool()
    await init_redis()
    yield
    await close_db_pool()
    await close_redis()


app = FastAPI(
    title="Quant Engine",
    version="0.5.0",
    lifespan=lifespan,
)

app.include_router(health_router, prefix="/api/v1")
app.include_router(market_router, prefix="/api/v1")
app.include_router(paper_router, prefix="/api/v1")
