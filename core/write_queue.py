import asyncio
from datetime import datetime, timezone
import structlog
from db.connection import get_pool

logger = structlog.get_logger()

_QUEUE_MAX_SIZE = 1000
_BATCH_FLUSH_INTERVAL = 0.1

# Queue initialized inside start_write_worker() — must be created
# within a running event loop, not at module import time
_queue: asyncio.Queue | None = None
_dropped_count: int = 0
_flushed_count: int = 0
_worker_task: asyncio.Task | None = None


def enqueue(event_type: str, payload: dict) -> None:
    """
    Non-blocking enqueue. Returns immediately regardless of queue state.
    No-ops silently if worker not started (startup race condition safety).
    """
    global _dropped_count
    if _queue is None:
        return  # Worker not started yet — safe to ignore during startup

    event = {"event_type": event_type, "payload": payload}
    try:
        _queue.put_nowait(event)
    except asyncio.QueueFull:
        _dropped_count += 1
        logger.warning(
            "write_queue_overflow",
            event_type=event_type,
            total_dropped=_dropped_count,
        )
        if _dropped_count % 1000 == 0:
            try:
                from notifications.telegram_notifier import notifier
                notifier.notify(
                    f"\u26a0\ufe0f quant_engine\n"
                    f"queue overflow\n"
                    f"dropped={_dropped_count:,} events"
                )
            except Exception:
                pass


def get_queue_stats() -> dict:
    return {
        "queue_size": _queue.qsize() if _queue else 0,
        "queue_capacity": _QUEUE_MAX_SIZE,
        "dropped_events": _dropped_count,
        "flushed_events": _flushed_count,
    }


async def _flush_batch(batch: list[dict]) -> None:
    if not batch:
        return

    trade_events = [e for e in batch if e["event_type"] == "trade"]
    signal_events = [e for e in batch if e["event_type"] == "signal"]
    portfolio_events = [e for e in batch if e["event_type"] == "portfolio_state"]
    mainnet_events = [e for e in batch if e["event_type"] == "mainnet_trade"]

    pool = get_pool()

    if trade_events:
        await _insert_trades(pool, trade_events)
    if signal_events:
        await _insert_signals(pool, signal_events)
    if portfolio_events:
        await _insert_portfolio_states(pool, portfolio_events)
    if mainnet_events:
        await _insert_mainnet_trades(pool, mainnet_events)

    global _flushed_count
    _flushed_count += len(batch)

    logger.debug(
        "write_queue_flushed",
        trades=len(trade_events),
        signals=len(signal_events),
        portfolio_states=len(portfolio_events),
        mainnet_trades=len(mainnet_events),
        total_flushed=_flushed_count,
    )


async def _insert_trades(pool, events: list[dict]) -> None:
    async with pool.acquire() as conn:
        for e in events:
            r = e["payload"]
            await conn.execute("""
                INSERT INTO trades (
                    trade_id, symbol, price, quantity,
                    is_buyer_maker, trade_time, event_time
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, trade_id) DO NOTHING
            """,
                r["trade_id"], r["symbol"], r["price"], r["quantity"],
                r["is_buyer_maker"], r["trade_time"], r["event_time"],
            )


async def _insert_signals(pool, events: list[dict]) -> None:
    async with pool.acquire() as conn:
        for e in events:
            r = e["payload"]
            await conn.execute("""
                INSERT INTO signal_log (
                    strategy_name, symbol, signal,
                    tick_price, tick_timestamp, generated_at,
                    latency_ms, reason, risk_blocked,
                    block_reason, session_id, post_reconnect
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
                r["strategy_name"], r["symbol"], r["signal"],
                r["tick_price"], r["tick_timestamp"], r["generated_at"],
                r["latency_ms"], r["reason"], r["risk_blocked"],
                r["block_reason"], r["session_id"], r["post_reconnect"],
            )


async def _insert_portfolio_states(pool, events: list[dict]) -> None:
    async with pool.acquire() as conn:
        for e in events:
            r = e["payload"]
            await conn.execute("""
                INSERT INTO portfolio_state_log (
                    session_id, recorded_at, cash,
                    position_side, position_price, position_qty,
                    portfolio_value, total_pnl, cause
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
                r["session_id"], r["recorded_at"], r["cash"],
                r["position_side"], r["position_price"], r["position_qty"],
                r["portfolio_value"], r["total_pnl"], r["cause"],
            )


async def _worker() -> None:
    assert _queue is not None, "Queue must be initialized before worker starts"
    logger.info("write_queue_worker_started", capacity=_QUEUE_MAX_SIZE)
    batch = []

    while True:
        try:
            event = await asyncio.wait_for(
                _queue.get(), timeout=_BATCH_FLUSH_INTERVAL
            )
            batch.append(event)
            while not _queue.empty() and len(batch) < 500:
                batch.append(_queue.get_nowait())

        except asyncio.TimeoutError:
            pass

        except asyncio.CancelledError:
            logger.info("write_queue_worker_draining", remaining=_queue.qsize())
            if batch:
                await _flush_batch(batch)
            final_batch = []
            while not _queue.empty():
                final_batch.append(_queue.get_nowait())
            if final_batch:
                await _flush_batch(final_batch)
            logger.info(
                "write_queue_worker_stopped",
                total_flushed=_flushed_count,
                total_dropped=_dropped_count,
            )
            return

        if batch:
            try:
                await _flush_batch(batch)
            except Exception as e:
                logger.error(
                    "write_queue_flush_error",
                    error=str(e),
                    batch_size=len(batch),
                    exc_info=True,
                )
            batch = []


async def start_write_worker(max_size: int = _QUEUE_MAX_SIZE) -> None:
    """
    Creates the Queue and starts the background worker.
    Must be called from within a running event loop.
    Queue is intentionally created here — not at module level —
    to ensure it belongs to the active event loop.
    """
    global _queue, _worker_task
    _queue = asyncio.Queue(maxsize=max_size)
    _worker_task = asyncio.create_task(_worker(), name="write_queue_worker")
    # Yield to event loop so worker starts and logs before we return
    await asyncio.sleep(0)


async def stop_write_worker() -> None:
    global _worker_task
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
    if _worker_task and _worker_task.done() and not _worker_task.cancelled():
        exc = _worker_task.exception()
        if exc:
            logger.error("write_queue_worker_exception", error=str(exc))


async def _insert_mainnet_trades(pool, events: list[dict]) -> None:
    async with pool.acquire() as conn:
        for e in events:
            r = e["payload"]
            await conn.execute("""
                INSERT INTO mainnet_trades (
                    trade_id, symbol, price, quantity,
                    is_buyer_maker, trade_time, event_time
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, trade_id) DO NOTHING
            """,
                r["trade_id"], r["symbol"], r["price"], r["quantity"],
                r["is_buyer_maker"], r["trade_time"], r["event_time"],
            )
