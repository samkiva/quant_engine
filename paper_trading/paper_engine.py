from datetime import datetime, timezone
import structlog
from backtesting.datasource import DataSource
from backtesting.strategy import Strategy, Signal
from backtesting.portfolio import Portfolio
from backtesting.metrics import BacktestMetrics, compute_metrics
from paper_trading.signal_logger import log_signal
from paper_trading.risk_layer import RiskLayer

logger = structlog.get_logger()


class PaperEngine:
    """
    Live paper trading engine. Processes ticks from any DataSource,
    applies risk controls, logs every signal, and tracks simulated P&L.

    Differences from BacktestEngine:
    - Every signal is logged to PostgreSQL with latency measurement
    - Risk layer sits between strategy and portfolio
    - Reconnect events are forwarded to the risk layer
    - Post-reconnect signals are flagged in the signal log
    - Periodic P&L reporting to logs for observability
    """

    def __init__(
        self,
        datasource: DataSource,
        strategy: Strategy,
        portfolio: Portfolio,
        risk_layer: RiskLayer,
        strategy_name: str,
        session_id: int | None = None,
        report_interval: int = 100,
    ) -> None:
        self._datasource = datasource
        self._strategy = strategy
        self._portfolio = portfolio
        self._risk = risk_layer
        self._strategy_name = strategy_name
        self._session_id = session_id
        self._report_interval = report_interval
        self._equity_curve: list[float] = []
        self._tick_count: int = 0
        self._post_reconnect: bool = False

        # Register reconnect callback if datasource supports it
        if hasattr(datasource, "on_reconnect"):
            datasource.on_reconnect(self._on_reconnect)

    async def _on_reconnect(self, at: datetime) -> None:
        self._risk.notify_reconnect(at)
        self._post_reconnect = True
        logger.info("paper_engine_reconnect_noted", at=at.isoformat())

    async def run(self) -> BacktestMetrics:
        logger.info(
            "paper_engine_started",
            strategy=self._strategy_name,
            session_id=self._session_id,
        )

        async for tick in self._datasource.stream():
            generated_at = datetime.now(tz=timezone.utc)
            signal = self._strategy.on_tick(tick)

            # Risk check
            allowed, check_reason = self._risk.check(signal, tick.price)
            risk_blocked = not allowed

            # Only log non-HOLD signals or blocked signals
            if signal.signal != Signal.HOLD or risk_blocked:
                await log_signal(
                    signal=signal,
                    strategy_name=self._strategy_name,
                    generated_at=generated_at,
                    session_id=self._session_id,
                    post_reconnect=self._post_reconnect,
                    risk_blocked=risk_blocked,
                    block_reason=check_reason if risk_blocked else None,
                )

            # Execute signal if risk-approved
            if allowed:
                self._portfolio.on_signal(signal)

            # Reset post-reconnect flag after first tick processed
            self._post_reconnect = False

            portfolio_value = self._portfolio.current_value(tick.price)
            self._equity_curve.append(portfolio_value)
            self._tick_count += 1

            if self._tick_count % self._report_interval == 0:
                trades = self._portfolio.closed_trades
                total_pnl = sum(
                    t.pnl for t in trades if t.pnl is not None
                )
                logger.info(
                    "paper_engine_status",
                    ticks=self._tick_count,
                    portfolio_value=round(portfolio_value, 2),
                    closed_trades=len(trades),
                    total_pnl=round(total_pnl, 4),
                    kill_switch=self._risk.kill_switch_active,
                )

        return compute_metrics(self._portfolio, self._equity_curve)

    @property
    def equity_curve(self) -> list[float]:
        return self._equity_curve.copy()

    @property
    def tick_count(self) -> int:
        return self._tick_count
