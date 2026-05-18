import asyncio
from core.logging_setup import configure_logging
from core.write_queue import start_write_worker, stop_write_worker, get_queue_stats
from db.connection import init_db_pool, close_db_pool
from db.session_tracker import record_connect, record_disconnect
from cache.redis_client import init_redis, close_redis
from services.market_data.binance_ws_client import connect_trade_stream
from services.market_data.mainnet_handler import MainnetTradeHandler
from config.settings import settings
import structlog

configure_logging()
logger = structlog.get_logger()

SYMBOL = settings.ws_symbol
MAINNET_URL = settings.binance_ws_mainnet_url


async def main() -> None:
    await init_db_pool()
    await init_redis()
    await start_write_worker(max_size=10_000)

    session_id = await record_connect(f"MAINNET_{SYMBOL}")
    handler = MainnetTradeHandler()

    logger.info(
        "mainnet_collection_started",
        symbol=SYMBOL,
        url=MAINNET_URL,
        note="passive collection only — no trading",
    )
    try:
        from notifications.telegram_notifier import notifier
        await notifier._send(
            f"\U0001f7e2 quant_engine\n"
            f"mainnet collection started\n"
            f"symbol={SYMBOL}\n"
            f"queue_capacity=10,000"
        )
    except Exception:
        pass

    trades_received = 0
    try:
        # Override the URL for mainnet
        import services.market_data.binance_ws_client as ws_module
        original_url_getter = None

        # Connect directly using mainnet URL
        stream = SYMBOL.lower() + "@trade"
        url = f"{MAINNET_URL}/{stream}"

        import json
        import websockets

        INITIAL_RETRY_DELAY = 1.0
        MAX_RETRY_DELAY = 60.0
        BACKOFF_MULTIPLIER = 2.0
        retry_delay = INITIAL_RETRY_DELAY

        while True:
            try:
                logger.info("mainnet_ws_connecting", url=url)
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    logger.info("mainnet_ws_connected", symbol=SYMBOL)
                    retry_delay = INITIAL_RETRY_DELAY

                    async for raw_message in ws:
                        try:
                            data = json.loads(raw_message)
                            await handler.handle(data)
                            trades_received += 1
                        except json.JSONDecodeError as e:
                            logger.warning("mainnet_invalid_json", error=str(e))
                        except Exception as e:
                            logger.error("mainnet_handler_error", error=str(e))

            except websockets.exceptions.ConnectionClosedOK:
                logger.info("mainnet_ws_closed_cleanly")
                break

            except asyncio.CancelledError:
                logger.info("mainnet_ws_cancelled")
                break

            except Exception as e:
                logger.warning(
                    "mainnet_ws_error",
                    error=str(e),
                    retry_in=retry_delay,
                )

            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * BACKOFF_MULTIPLIER, MAX_RETRY_DELAY)

    finally:
        stats = get_queue_stats()
        logger.info(
            "mainnet_collection_ended",
            trades_received=trades_received,
            queue_stats=stats,
        )
        await record_disconnect(
            session_id,
            trades_received,
            reason="mainnet_session_ended",
            is_clean=True,
        )
        await stop_write_worker()
        await close_db_pool()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
