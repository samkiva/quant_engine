import asyncio
import json
from typing import Callable, Awaitable
import websockets
import structlog
from config.settings import settings
from db.session_tracker import record_connect, record_disconnect

logger = structlog.get_logger()

MessageHandler = Callable[[dict], Awaitable[None]]

INITIAL_RETRY_DELAY: float = 1.0
MAX_RETRY_DELAY: float = settings.ws_reconnect_max_delay
BACKOFF_MULTIPLIER: float = 2.0


async def connect_trade_stream(
    symbol: str,
    on_message: MessageHandler,
) -> None:
    """
    Maintains a persistent WebSocket connection to the Binance trade stream.
    Reconnects with exponential backoff. Records every session for
    data integrity auditing. Uses finally to guarantee session closure
    is always recorded regardless of how the connection ends.
    """
    stream = symbol.lower() + "@trade"
    url = f"{settings.binance_ws_testnet_url}/{stream}"
    retry_delay = INITIAL_RETRY_DELAY

    while True:
        session_id: int | None = None
        trades_this_session: int = 0
        disconnect_reason: str = "unknown"
        is_clean: bool = False

        try:
            logger.info("ws_connecting", url=url, symbol=symbol)
            session_id = await record_connect(symbol)

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                logger.info("ws_connected", symbol=symbol, session_id=session_id)
                retry_delay = INITIAL_RETRY_DELAY

                async for raw_message in ws:
                    try:
                        data = json.loads(raw_message)
                        await on_message(data)
                        trades_this_session += 1
                    except json.JSONDecodeError as e:
                        logger.warning("ws_invalid_json", error=str(e))
                    except Exception as e:
                        logger.error(
                            "ws_handler_error",
                            error=str(e),
                            exc_info=True,
                        )

            # Exited the async with cleanly
            disconnect_reason = "clean_close"
            is_clean = True

        except websockets.exceptions.ConnectionClosedOK:
            disconnect_reason = "clean_close"
            is_clean = True

        except websockets.exceptions.ConnectionClosedError as e:
            disconnect_reason = str(e)
            logger.warning("ws_closed_unexpectedly", reason=disconnect_reason)

        except OSError as e:
            disconnect_reason = str(e)
            logger.error("ws_network_error", error=disconnect_reason)

        except asyncio.CancelledError:
            disconnect_reason = "cancelled"
            is_clean = True
            raise  # Must re-raise CancelledError

        except Exception as e:
            disconnect_reason = str(e)
            logger.error("ws_unexpected_error", error=disconnect_reason)

        finally:
            # Guaranteed to run regardless of how the session ended
            if session_id is not None:
                try:
                    await record_disconnect(
                        session_id,
                        trades_this_session,
                        reason=disconnect_reason,
                        is_clean=is_clean,
                    )
                except Exception as e:
                    logger.error("session_record_failed", error=str(e))

        if is_clean and disconnect_reason in ("clean_close", "cancelled"):
            break

        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * BACKOFF_MULTIPLIER, MAX_RETRY_DELAY)
