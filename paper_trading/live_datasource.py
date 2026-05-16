import asyncio
import json
from collections.abc import AsyncIterator
from datetime import datetime, timezone
import websockets
import structlog
from backtesting.datasource import DataSource, Tick
from config.settings import settings

logger = structlog.get_logger()

INITIAL_RETRY_DELAY: float = 1.0
MAX_RETRY_DELAY: float = settings.ws_reconnect_max_delay
BACKOFF_MULTIPLIER: float = 2.0


class LiveDataSource(DataSource):
    """
    Implements the DataSource interface backed by a live WebSocket stream.

    This is the key architectural validation: the paper trading engine
    uses the identical DataSource ABC as the backtesting engine.
    Swapping PostgresDataSource for LiveDataSource is the only change
    needed to go from historical replay to live operation.

    Emits reconnect events so the paper engine can notify the risk layer.
    """

    def __init__(self, symbol: str) -> None:
        self._symbol = symbol
        self._reconnect_callbacks: list = []
        self._tick_count: int = 0

    def on_reconnect(self, callback) -> None:
        """Register a callback to be called on every reconnect."""
        self._reconnect_callbacks.append(callback)

    async def _notify_reconnect(self) -> None:
        now = datetime.now(tz=timezone.utc)
        for cb in self._reconnect_callbacks:
            await cb(now)

    async def count(self) -> int:
        """Live source has no fixed count — returns ticks seen so far."""
        return self._tick_count

    async def stream(self) -> AsyncIterator[Tick]:
        stream_name = self._symbol.lower() + "@trade"
        url = f"{settings.binance_ws_testnet_url}/{stream_name}"
        retry_delay = INITIAL_RETRY_DELAY
        first_connect = True

        while True:
            try:
                if not first_connect:
                    logger.info(
                        "live_ds_reconnecting",
                        symbol=self._symbol,
                        retry_delay=retry_delay,
                    )
                    await self._notify_reconnect()

                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    first_connect = False
                    retry_delay = INITIAL_RETRY_DELAY
                    logger.info("live_ds_connected", symbol=self._symbol)

                    async for raw_message in ws:
                        try:
                            data = json.loads(raw_message)
                            tick = self._parse_tick(data)
                            if tick:
                                self._tick_count += 1
                                yield tick
                        except Exception as e:
                            logger.error(
                                "live_ds_parse_error",
                                error=str(e),
                            )

            except websockets.exceptions.ConnectionClosedOK:
                logger.info("live_ds_closed_cleanly")
                return

            except asyncio.CancelledError:
                logger.info("live_ds_cancelled")
                return

            except Exception as e:
                logger.warning(
                    "live_ds_connection_error",
                    error=str(e),
                    retry_in=retry_delay,
                )

            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * BACKOFF_MULTIPLIER, MAX_RETRY_DELAY)

    def _parse_tick(self, msg: dict) -> Tick | None:
        try:
            return Tick(
                timestamp=datetime.fromtimestamp(
                    msg["T"] / 1000, tz=timezone.utc
                ),
                symbol=msg["s"],
                price=float(msg["p"]),
                quantity=float(msg["q"]),
                is_buyer_maker=msg["m"],
            )
        except KeyError:
            return None
