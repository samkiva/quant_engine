from datetime import datetime, timezone
from backtesting.portfolio import Portfolio
from core.write_queue import enqueue


def record_portfolio_state(
    portfolio: Portfolio,
    current_price: float,
    session_id: int | None,
    cause: str,
) -> None:
    """
    Enqueues a portfolio snapshot to the write queue.
    Called after every signal execution — not awaited, never blocks.

    cause: human-readable description of what triggered this snapshot
           e.g. "buy_signal_executed", "sell_signal_executed", "startup"
    """
    position = portfolio.open_position
    total_pnl = sum(
        t.pnl for t in portfolio.closed_trades if t.pnl is not None
    )

    enqueue("portfolio_state", {
        "session_id": session_id,
        "recorded_at": datetime.now(tz=timezone.utc),
        "cash": portfolio.cash,
        "position_side": position.side if position else None,
        "position_price": position.entry_price if position else None,
        "position_qty": position.quantity if position else None,
        "portfolio_value": portfolio.current_value(current_price),
        "total_pnl": total_pnl,
        "cause": cause,
    })
