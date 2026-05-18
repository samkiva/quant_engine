from datetime import datetime, timezone
import structlog
from backtesting.strategy import StrategySignal
from core.write_queue import enqueue

logger = structlog.get_logger()


def log_signal(
    signal: StrategySignal,
    strategy_name: str,
    generated_at: datetime,
    session_id: int | None = None,
    post_reconnect: bool = False,
    risk_blocked: bool = False,
    block_reason: str | None = None,
) -> None:
    """
    Enqueues a signal record. Uses received_at for latency measurement
    to eliminate clock skew between local clock and Binance server clock.

    processing_latency = generated_at - received_at (both local clock)
    clock_skew = received_at - tick.timestamp (local vs server)
    """
    tick = signal.tick

    # True processing latency — local clock only, immune to clock skew
    processing_latency_ms = (
        (generated_at - tick.received_at).total_seconds() * 1000
    )

    # Clock skew measurement — informational only
    clock_skew_ms = (
        (tick.received_at - tick.timestamp).total_seconds() * 1000
    )

    enqueue("signal", {
        "strategy_name": strategy_name,
        "symbol": tick.symbol,
        "signal": signal.signal.value,
        "tick_price": tick.price,
        "tick_timestamp": tick.timestamp,
        "generated_at": generated_at,
        "latency_ms": round(processing_latency_ms, 3),
        "reason": signal.reason,
        "risk_blocked": risk_blocked,
        "block_reason": block_reason,
        "session_id": session_id,
        "post_reconnect": post_reconnect,
    })

    logger.debug(
        "signal_enqueued",
        strategy=strategy_name,
        signal=signal.signal.value,
        price=tick.price,
        processing_latency_ms=round(processing_latency_ms, 1),
        clock_skew_ms=round(clock_skew_ms, 1),
        risk_blocked=risk_blocked,
    )
