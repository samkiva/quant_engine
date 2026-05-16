import structlog
from backtesting.datasource import DataSource
from backtesting.strategy import Strategy
from backtesting.portfolio import Portfolio
from backtesting.metrics import BacktestMetrics, compute_metrics

logger = structlog.get_logger()


class BacktestEngine:
    """
    Feeds ticks to a strategy one at a time in chronological order.
    Executes signals through the portfolio.
    Records the equity curve for metrics computation.

    The engine knows nothing about:
    - What the strategy does internally
    - Where the data comes from
    - How the portfolio sizes positions

    This separation makes every component independently testable.
    """

    def __init__(
        self,
        datasource: DataSource,
        strategy: Strategy,
        portfolio: Portfolio,
    ) -> None:
        self._datasource = datasource
        self._strategy = strategy
        self._portfolio = portfolio
        self._equity_curve: list[float] = []
        self._tick_count: int = 0

    async def run(self) -> BacktestMetrics:
        """
        Runs the full backtest. Returns computed metrics.
        """
        logger.info("backtest_started")
        self._equity_curve = []
        self._tick_count = 0

        async for tick in self._datasource.stream():
            signal = self._strategy.on_tick(tick)
            self._portfolio.on_signal(signal)
            portfolio_value = self._portfolio.current_value(tick.price)
            self._equity_curve.append(portfolio_value)
            self._tick_count += 1

            if self._tick_count % 500 == 0:
                logger.debug(
                    "backtest_progress",
                    ticks=self._tick_count,
                    portfolio_value=round(portfolio_value, 2),
                    trades=len(self._portfolio.closed_trades),
                )

        metrics = compute_metrics(self._portfolio, self._equity_curve)
        logger.info(
            "backtest_complete",
            ticks=self._tick_count,
            total_trades=metrics.total_trades,
            total_pnl=round(metrics.total_pnl, 4),
            win_rate=round(metrics.win_rate, 3),
            sharpe=round(metrics.sharpe_ratio, 3),
            max_drawdown_pct=round(metrics.max_drawdown_pct, 2),
        )
        return metrics

    @property
    def equity_curve(self) -> list[float]:
        return self._equity_curve.copy()
