from dataclasses import dataclass
from datetime import datetime, timezone
import structlog
from backtesting.strategy import Signal, StrategySignal
from backtesting.portfolio import Portfolio

logger = structlog.get_logger()


@dataclass
class RiskConfig:
    max_daily_loss_pct: float = 2.0       # Halt if daily P&L drops below -2%
    max_position_pct: float = 10.0        # Max 10% of portfolio in one position
    max_signals_per_minute: int = 10      # Prevent runaway signal generation
    post_reconnect_hold_secs: float = 5.0 # Hold signals for N secs after reconnect


class RiskLayer:
    """
    Pre-execution risk controls. Every signal passes through here
    before reaching the portfolio. Both paper and live trading use
    the same risk layer — identical controls in both modes.

    Design principle: the risk layer is conservative by default.
    When uncertain, it blocks. A missed trade is recoverable.
    A risk failure may not be.
    """

    def __init__(
        self,
        portfolio: Portfolio,
        config: RiskConfig | None = None,
    ) -> None:
        self._portfolio = portfolio
        self._config = config or RiskConfig()
        self._kill_switch: bool = False
        self._daily_start_value: float = portfolio.initial_cash
        self._signal_times: list[datetime] = []
        self._last_reconnect_at: datetime | None = None

    def notify_reconnect(self, at: datetime) -> None:
        """Called by the engine on every WebSocket reconnect."""
        self._last_reconnect_at = at
        logger.info("risk_reconnect_noted", at=at.isoformat())

    def activate_kill_switch(self) -> None:
        """Manually halts all signal execution immediately."""
        self._kill_switch = True
        logger.warning("kill_switch_activated")

    def deactivate_kill_switch(self) -> None:
        self._kill_switch = False
        logger.info("kill_switch_deactivated")

    @property
    def kill_switch_active(self) -> bool:
        return self._kill_switch

    def check(
        self,
        signal: StrategySignal,
        current_price: float,
    ) -> tuple[bool, str]:
        """
        Evaluates whether a signal should be executed.
        Returns (allowed: bool, reason: str).

        HOLD signals always pass — they require no action.
        Only BUY and SELL signals are risk-checked.
        """
        if signal.signal == Signal.HOLD:
            return True, "hold_no_check_required"

        # Kill switch — hard stop
        if self._kill_switch:
            return False, "kill_switch_active"

        # Post-reconnect hold — strategy state may be stale
        if self._last_reconnect_at is not None:
            now = datetime.now(tz=timezone.utc)
            secs_since_reconnect = (
                now - self._last_reconnect_at
            ).total_seconds()
            if secs_since_reconnect < self._config.post_reconnect_hold_secs:
                return False, (
                    f"post_reconnect_hold: {secs_since_reconnect:.1f}s "
                    f"< {self._config.post_reconnect_hold_secs}s threshold"
                )

        # Daily loss limit
        current_value = self._portfolio.current_value(current_price)
        daily_pnl_pct = (
            (current_value - self._daily_start_value)
            / self._daily_start_value * 100
        )
        if daily_pnl_pct < -self._config.max_daily_loss_pct:
            self.activate_kill_switch()
            return False, (
                f"daily_loss_limit: {daily_pnl_pct:.2f}% "
                f"< -{self._config.max_daily_loss_pct}%"
            )

        # Signal frequency limit
        now = datetime.now(tz=timezone.utc)
        self._signal_times = [
            t for t in self._signal_times
            if (now - t).total_seconds() < 60
        ]
        if len(self._signal_times) >= self._config.max_signals_per_minute:
            return False, (
                f"signal_frequency: {len(self._signal_times)} signals "
                f"in last 60s >= limit {self._config.max_signals_per_minute}"
            )

        self._signal_times.append(now)
        return True, "all_checks_passed"
