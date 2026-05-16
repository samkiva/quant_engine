import math
from dataclasses import dataclass
from backtesting.portfolio import Portfolio


@dataclass
class BacktestMetrics:
    total_trades:     int
    winning_trades:   int
    losing_trades:    int
    win_rate:         float   # 0.0 to 1.0
    total_pnl:        float
    total_return_pct: float
    max_drawdown_pct: float
    sharpe_ratio:     float
    avg_win:          float
    avg_loss:         float
    profit_factor:    float   # gross profit / gross loss


def compute_metrics(portfolio: Portfolio, equity_curve: list[float]) -> BacktestMetrics:
    """
    Computes standard backtesting performance metrics.

    equity_curve: list of portfolio values at each tick,
                  used for drawdown and Sharpe computation.
    """
    trades = portfolio.closed_trades

    if not trades:
        return BacktestMetrics(
            total_trades=0, winning_trades=0, losing_trades=0,
            win_rate=0.0, total_pnl=0.0, total_return_pct=0.0,
            max_drawdown_pct=0.0, sharpe_ratio=0.0,
            avg_win=0.0, avg_loss=0.0, profit_factor=0.0,
        )

    pnls = [t.pnl for t in trades if t.pnl is not None]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    total_pnl = sum(pnls)
    win_rate = len(wins) / len(pnls) if pnls else 0.0
    avg_win = sum(wins) / len(wins) if wins else 0.0
    avg_loss = sum(losses) / len(losses) if losses else 0.0

    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    total_return_pct = (total_pnl / portfolio.initial_cash) * 100

    # Max drawdown
    max_drawdown_pct = _compute_max_drawdown(equity_curve)

    # Sharpe ratio — using per-tick returns, no annualisation
    # (tick-time, not calendar-time, so annualisation would be misleading)
    sharpe = _compute_sharpe(equity_curve)

    return BacktestMetrics(
        total_trades=len(pnls),
        winning_trades=len(wins),
        losing_trades=len(losses),
        win_rate=win_rate,
        total_pnl=total_pnl,
        total_return_pct=total_return_pct,
        max_drawdown_pct=max_drawdown_pct,
        sharpe_ratio=sharpe,
        avg_win=avg_win,
        avg_loss=avg_loss,
        profit_factor=profit_factor,
    )


def _compute_max_drawdown(equity_curve: list[float]) -> float:
    """
    Maximum peak-to-trough decline as a percentage.
    This is the most important risk metric — it tells you
    the worst loss you would have experienced holding this strategy.
    """
    if len(equity_curve) < 2:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for value in equity_curve:
        if value > peak:
            peak = value
        drawdown = (peak - value) / peak * 100
        if drawdown > max_dd:
            max_dd = drawdown
    return max_dd


def _compute_sharpe(equity_curve: list[float], risk_free_rate: float = 0.0) -> float:
    """
    Sharpe ratio: mean return / std of returns.
    Higher is better. Above 1.0 is acceptable. Above 2.0 is good.
    Below 0 means the strategy loses money on average.

    Note: computed in tick-time, not annualised. Use for relative
    comparison only, not as an absolute benchmark against other assets.
    """
    if len(equity_curve) < 2:
        return 0.0
    returns = [
        (equity_curve[i] - equity_curve[i - 1]) / equity_curve[i - 1]
        for i in range(1, len(equity_curve))
    ]
    mean_return = sum(returns) / len(returns)
    if len(returns) < 2:
        return 0.0
    variance = sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)
    std_return = math.sqrt(variance)
    if std_return == 0:
        return 0.0
    return (mean_return - risk_free_rate) / std_return
