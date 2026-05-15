import asyncio
import json
from typing import Callable, Awaitable
import websockets
import structlog
from config.settings import settings

logger = structlog.get_logger()

MessageHandler = Callable[[dict], Awaitable[None]]

INITIAL_RETRY_DELAY: float = 1.0
MAX_RETRY_DELAY: float = 60.0
BACKOFF_MULTIPLIER: float = 2.0


async def connect_trade_stream(
    symbol: str,
    on_message: MessageHandler,
) -> None:
    """
    Maintains a persistent WebSocket connection to the Binance trade stream.
    Reconnects automatically with exponential backoff on any failure.

    Designed to run as a long-lived async task — does not return under
    normal operation.
    """
    stream = symbol.lower() + "@trade"
    url = f"{settings.binance_ws_testnet_url}/{stream}"
    retry_delay = INITIAL_RETRY_DELAY

    while True:
        try:
            logger.info("ws_connecting", url=url, symbol=symbol)

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                logger.info("ws_connected", symbol=symbol)
                retry_delay = INITIAL_RETRY_DELAY

                async for raw_message in ws:
                    try:
                        data = json.loads(raw_message)
                        await on_message(data)
                    except json.JSONDecodeError as e:
                        logger.warning("ws_invalid_json", error=str(e))
                    except Exception as e:
                        logger.error(
                            "ws_handler_error",
                            error=str(e),
                            exc_info=True,
                        )

        except websockets.exceptions.ConnectionClosedOK:
            logger.info("ws_closed_cleanly", symbol=symbol)
            break

        except websockets.exceptions.ConnectionClosedError as e:
            logger.warning(
                "ws_closed_unexpectedly",
                reason=str(e),
                retry_in=retry_delay,
            )

        except OSError as e:
            logger.error(
                "ws_network_error",
                error=str(e),
                retry_in=retry_delay,
            )

        except Exception as e:
            logger.error(
                "ws_unexpected_error",
                error=str(e),
                retry_in=retry_delay,
            )

        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * BACKOFF_MULTIPLIER, MAX_RETRY_DELAY)
