from collections import deque
from models.market_data import TradeEvent
import pandas as pd
import structlog

logger = structlog.get_logger()

MAX_BUFFER_SIZE = 10_000


class TradeBuffer:
    """
    Bounded in-memory buffer for raw trade events.

    Uses a deque with maxlen to prevent unbounded memory growth.
    Pandas DataFrame export is available for research use only.

    In Phase 2 this interface stays identical — only the backing
    store changes to PostgreSQL/Redis.
    """

    def __init__(self, maxlen: int = MAX_BUFFER_SIZE) -> None:
        self._buffer: deque[TradeEvent] = deque(maxlen=maxlen)

    def append(self, event: TradeEvent) -> None:
        self._buffer.append(event)

    def to_dataframe(self) -> pd.DataFrame:
        if not self._buffer:
            return pd.DataFrame()
        return pd.DataFrame([vars(e) for e in self._buffer])

    def size(self) -> int:
        return len(self._buffer)

    def clear(self) -> None:
        self._buffer.clear()
        logger.info("buffer_cleared")
