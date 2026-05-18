from fastapi import APIRouter
from core.write_queue import get_queue_stats

router = APIRouter()


@router.get("/health")
async def health_check():
    queue_stats = get_queue_stats()
    return {
        "status": "ok",
        "version": "0.6.0",
        "write_queue": queue_stats,
    }
