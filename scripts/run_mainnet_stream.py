"""
Mainnet trade stream — production collector.

Failure modes addressed:
1. Silent WebSocket stall: watchdog cancels connection if no tick for STALL_TIMEOUT seconds
2. Gap alerting: Telegram notification on reconnect with gap > GAP_ALERT_MINUTES
3. Daily summary: Telegram message every 24 hours with uptime and row count
4. Clean shutdown: queue drains before DB pool closes
"""

import asyncio
import json
from datetime import datetime, timezone, timedelta

import structlog
import websockets

from config.settings import settings
from core.logging_setup import configure_logging
from core.write_queue import start_write_worker, stop_write_worker, get_queue_stats
from db.connection import init_db_pool, close_db_pool, get_pool
from db.session_tracker import record_connect, record_disconnect
from cache.redis_client import init_redis, close_redis
from services.market_data.mainnet_handler import MainnetTradeHandler

configure_logging()
logger = structlog.get_logger()

SYMBOL = settings.ws_symbol
MAINNET_URL = settings.binance_ws_mainnet_url
STREAM_URL = f"{MAINNET_URL}/{SYMBOL.lower()}@trade"

INITIAL_RETRY_DELAY = 1.0
MAX_RETRY_DELAY = 60.0
BACKOFF_MULTIPLIER = 2.0
STALL_TIMEOUT = 300        # seconds — 5 minutes without a tick = stall
GAP_ALERT_MINUTES = 10     # notify on reconnect if gap exceeded this
DAILY_SUMMARY_HOURS = 24


# --- Telegram helpers ---

async def _tg_send(text: str) -> None:
    """Fire-and-forget Telegram send. Never raises."""
    try:
        from telegram import Bot
        bot = Bot(token=settings.telegram_bot_token)
        await bot.send_message(chat_id=settings.telegram_chat_id, text=text)
    except Exception as e:
        logger.warning("telegram_send_failed", error=str(e))


# --- Database helpers ---

async def _get_row_count() -> int:
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM mainnet_trades WHERE symbol = $1", SYMBOL
            )
    except Exception:
        return -1


async def _get_largest_recent_gap_minutes() -> float:
    """Returns the largest gap in minutes from the last 24 hours of data."""
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval("""
                WITH ordered AS (
                    SELECT trade_time,
                           LEAD(trade_time) OVER (ORDER BY trade_time) AS next_time
                    FROM mainnet_trades
                    WHERE symbol = $1
                      AND trade_time >= NOW() - INTERVAL '24 hours'
                )
                SELECT COALESCE(
                    MAX(EXTRACT(EPOCH FROM (next_time - trade_time)) / 60),
                    0
                )
                FROM ordered
                WHERE next_time IS NOT NULL
            """, SYMBOL)
            return float(result or 0)
    except Exception:
        return -1.0


# --- Daily summary task ---

async def _daily_summary_loop(start_time: datetime) -> None:
    """Sends a Telegram summary every DAILY_SUMMARY_HOURS hours."""
    while True:
        await asyncio.sleep(DAILY_SUMMARY_HOURS * 3600)
        row_count = await _get_row_count()
        max_gap = await _get_largest_recent_gap_minutes()
        uptime_hours = (
            datetime.now(tz=timezone.utc) - start_time
        ).total_seconds() / 3600

        await _tg_send(
            f"\U0001f4ca quant_engine daily summary\n"
            f"symbol: {SYMBOL}\n"
            f"uptime: {uptime_hours:.1f}h\n"
            f"total rows: {row_count:,}\n"
            f"max gap (24h): {max_gap:.1f} min"
        )
        logger.info(
            "daily_summary_sent",
            uptime_hours=round(uptime_hours, 1),
            row_count=row_count,
            max_gap_24h=round(max_gap, 1),
        )


# --- Stream with stall detection ---

async def _run_stream(
    handler: MainnetTradeHandler,
    trades_received_ref: list,
) -> None:
    """
    Connects to Binance WebSocket and ingests trades.
    Reconnects with exponential backoff on any failure.
    Stall detection: cancels and reconnects if no tick for STALL_TIMEOUT seconds.
    Gap alerting: Telegram message if reconnect gap > GAP_ALERT_MINUTES.
    """
    retry_delay = INITIAL_RETRY_DELAY
    last_disconnect_time: datetime | None = None

    while True:
        try:
            logger.info("mainnet_ws_connecting", url=STREAM_URL)
            async with websockets.connect(
                STREAM_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                logger.info("mainnet_ws_connected", symbol=SYMBOL)

                # Alert if gap since last disconnect exceeds threshold
                if last_disconnect_time is not None:
                    gap_minutes = (
                        datetime.now(tz=timezone.utc) - last_disconnect_time
                    ).total_seconds() / 60
                    if gap_minutes > GAP_ALERT_MINUTES:
                        await _tg_send(
                            f"\U0001f504 quant_engine reconnected\n"
                            f"gap: {gap_minutes:.0f} min\n"
                            f"symbol: {SYMBOL}"
                        )
                    logger.info("reconnect_gap_minutes", gap=round(gap_minutes, 1))

                retry_delay = INITIAL_RETRY_DELAY

                # Stall detection: wrap each recv in a timeout
                while True:
                    try:
                        raw_message = await asyncio.wait_for(
                            ws.recv(), timeout=STALL_TIMEOUT
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            "mainnet_ws_stall_detected",
                            stall_timeout_seconds=STALL_TIMEOUT,
                        )
                        await _tg_send(
                            f"\u26a0\ufe0f quant_engine stall detected\n"
                            f"no tick for {STALL_TIMEOUT//60} min\n"
                            f"forcing reconnect"
                        )
                        break  # exit inner loop → reconnect

                    try:
                        data = json.loads(raw_message)
                        await handler.handle(data)
                        trades_received_ref[0] += 1
                    except json.JSONDecodeError as e:
                        logger.warning("mainnet_invalid_json", error=str(e))
                    except Exception as e:
                        logger.error("mainnet_handler_error", error=str(e))

        except websockets.exceptions.ConnectionClosedOK:
            logger.info("mainnet_ws_closed_cleanly")
            break

        except asyncio.CancelledError:
            logger.info("mainnet_ws_cancelled")
            return

        except Exception as e:
            logger.warning(
                "mainnet_ws_error",
                error=str(e),
                retry_in=retry_delay,
            )

        last_disconnect_time = datetime.now(tz=timezone.utc)
        logger.info("mainnet_ws_reconnecting", delay=retry_delay)
        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * BACKOFF_MULTIPLIER, MAX_RETRY_DELAY)


# --- Entry point ---

async def main() -> None:
    await init_db_pool()
    await init_redis()
    await start_write_worker(max_size=10_000)

    session_id = await record_connect(f"MAINNET_{SYMBOL}")
    handler = MainnetTradeHandler()
    start_time = datetime.now(tz=timezone.utc)
    trades_received_ref = [0]  # mutable ref for coroutine sharing

    await _tg_send(
        f"\U0001f7e2 quant_engine online\n"
        f"mainnet collection started\n"
        f"symbol: {SYMBOL}\n"
        f"stall_timeout: {STALL_TIMEOUT//60}min\n"
        f"sample_rate: 1 in {handler.SAMPLE_EVERY}"
    )

    logger.info(
        "mainnet_collection_started",
        symbol=SYMBOL,
        stall_timeout_seconds=STALL_TIMEOUT,
        sample_every=handler.SAMPLE_EVERY,
    )

    summary_task = asyncio.create_task(
        _daily_summary_loop(start_time),
        name="daily_summary",
    )

    try:
        await _run_stream(handler, trades_received_ref)
    except asyncio.CancelledError:
        pass
    finally:
        summary_task.cancel()
        try:
            await summary_task
        except asyncio.CancelledError:
            pass

        stats = get_queue_stats()
        row_count = await _get_row_count()

        await _tg_send(
            f"\U0001f534 quant_engine stopped\n"
            f"trades_received: {trades_received_ref[0]:,}\n"
            f"total_rows_db: {row_count:,}\n"
            f"queue_flushed: {stats['flushed_events']:,}\n"
            f"dropped: {stats['dropped_events']:,}"
        )

        logger.info(
            "mainnet_collection_ended",
            trades_received=trades_received_ref[0],
            queue_stats=stats,
        )

        await record_disconnect(
            session_id,
            trades_received_ref[0],
            reason="mainnet_session_ended",
            is_clean=True,
        )
        await stop_write_worker()
        await close_db_pool()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
