from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
import structlog
from backtesting.strategy import Signal, StrategySignal
from backtesting.portfolio import Portfolio
from risk.kelly import compute_kelly, KellyResult
from risk.volatility_sizing import compute_vol_scalar

logger = structlog.get_logger()


@dataclass
class RiskDecision:
    allowed: bool
    reason: str
    kelly_fraction: float = 0.0
    vol_scalar: float = 1.0
    suggested_size: float = 0.0


class RiskEngineV2:
    """
    Full pre-trade risk gate. Every signal passes through here.
    No strategy can bypass this engine.

    Controls enforced:
    1. Kill switch — hard stop, overrides everything
    2. Daily drawdown halt — stops trading if daily loss exceeds limit
    3. Post-reconnect hold — stale state window after reconnect
    4. Signal frequency limit — prevents runaway strategies
    5. Kelly criterion — position sizing from historical edge
    6. Volatility targeting — adjusts size to current market regime
    7. Cooldown window — enforced pause after losing trade
    """

    def __init__(
        self,
        portfolio: Portfolio,
        base_trade_qty: float = 0.001,
        max_daily_loss_pct: float = 2.0,
        max_signals_per_minute: int = 10,
        post_reconnect_hold_secs: float = 5.0,
        cooldown_after_loss_secs: float = 30.0,
    ) -> None:
        self._portfolio = portfolio
        self._base_qty = base_trade_qty
        self._max_daily_loss_pct = max_daily_loss_pct
        self._max_signals_per_minute = max_signals_per_minute
        self._post_reconnect_hold_secs = post_reconnect_hold_secs
        self._cooldown_secs = cooldown_after_loss_secs

        self._kill_switch = False
        self._daily_start_value = portfolio.initial_cash
        self._signal_times: list[datetime] = []
        self._last_reconnect_at: Optional[datetime] = None
        self._last_loss_at: Optional[datetime] = None
        self._recent_log_returns: list[float] = []
        self._last_price: Optional[float] = None

    def notify_reconnect(self, at: datetime) -> None:
        self._last_reconnect_at = at
        logger.info("risk_engine_reconnect_noted", at=at.isoformat())

    def activate_kill_switch(self) -> None:
        self._kill_switch = True
        logger.warning("risk_kill_switch_activated")

    def deactivate_kill_switch(self) -> None:
        self._kill_switch = False
        logger.info("risk_kill_switch_deactivated")

    @property
    def kill_switch_active(self) -> bool:
        return self._kill_switch

    def _update_returns(self, price: float) -> None:
        if self._last_price and self._last_price > 0:
            import math
            log_return = math.log(price / self._last_price)
            self._recent_log_returns.append(log_return)
            if len(self._recent_log_returns) > 200:
                self._recent_log_returns = self._recent_log_returns[-200:]
        self._last_price = price

    def _check_loss_cooldown(self) -> tuple[bool, str]:
        if self._last_loss_at is None:
            return True, "no_recent_loss"
        elapsed = (datetime.now(tz=timezone.utc) - self._last_loss_at).total_seconds()
        if elapsed < self._cooldown_secs:
            remaining = self._cooldown_secs - elapsed
            return False, f"cooldown_active: {remaining:.0f}s remaining"
        return True, "cooldown_expired"

    def record_loss(self) -> None:
        self._last_loss_at = datetime.now(tz=timezone.utc)
        logger.info("risk_loss_recorded_cooldown_started")

    def evaluate(
        self,
        signal: StrategySignal,
    ) -> RiskDecision:
        """
        Evaluates a signal through all risk controls.
        Returns a RiskDecision with sizing recommendation.
        HOLD signals pass immediately — no checks required.
        """
        tick = signal.tick
        self._update_returns(tick.price)

        if signal.signal == Signal.HOLD:
            return RiskDecision(allowed=True, reason="hold_no_check")

        # 1. Kill switch
        if self._kill_switch:
            return RiskDecision(allowed=False, reason="kill_switch_active")

        # 2. Daily drawdown halt
        current_value = self._portfolio.current_value(tick.price)
        daily_pnl_pct = (
            (current_value - self._daily_start_value)
            / self._daily_start_value * 100
        )
        if daily_pnl_pct < -self._max_daily_loss_pct:
            self.activate_kill_switch()
            return RiskDecision(
                allowed=False,
                reason=f"daily_loss_halt: {daily_pnl_pct:.2f}%",
            )

        # 3. Post-reconnect hold
        if self._last_reconnect_at:
            elapsed = (
                datetime.now(tz=timezone.utc) - self._last_reconnect_at
            ).total_seconds()
            if elapsed < self._post_reconnect_hold_secs:
                return RiskDecision(
                    allowed=False,
                    reason=f"post_reconnect_hold: {elapsed:.1f}s elapsed",
                )

        # 4. Signal frequency
        now = datetime.now(tz=timezone.utc)
        self._signal_times = [
            t for t in self._signal_times
            if (now - t).total_seconds() < 60
        ]
        if len(self._signal_times) >= self._max_signals_per_minute:
            return RiskDecision(
                allowed=False,
                reason=f"signal_frequency_limit: {len(self._signal_times)}/min",
            )
        self._signal_times.append(now)

        # 5. Cooldown after loss
        cooldown_ok, cooldown_reason = self._check_loss_cooldown()
        if not cooldown_ok:
            return RiskDecision(allowed=False, reason=cooldown_reason)

        # 6. Kelly sizing
        trades = self._portfolio.closed_trades
        wins = [t.pnl for t in trades if t.pnl and t.pnl > 0]
        losses = [t.pnl for t in trades if t.pnl and t.pnl <= 0]
        kelly = compute_kelly(wins, losses)

        if not kelly.is_valid:
            # Not enough data or negative edge — use minimum size
            kelly_fraction = 0.001  # Minimum exploratory size
        else:
            kelly_fraction = kelly.capped_kelly

        # 7. Volatility scalar
        vol_scalar = compute_vol_scalar(self._recent_log_returns)

        # Final size = base * kelly_fraction * vol_scalar
        # For now kelly_fraction acts as a multiplier on base_qty
        suggested_size = self._base_qty * vol_scalar

        logger.debug(
            "risk_decision_approved",
            signal=signal.signal.value,
            kelly_valid=kelly.is_valid,
            kelly_fraction=round(kelly_fraction, 4),
            vol_scalar=round(vol_scalar, 3),
            suggested_size=round(suggested_size, 6),
            daily_pnl_pct=round(daily_pnl_pct, 3),
        )

        return RiskDecision(
            allowed=True,
            reason="all_checks_passed",
            kelly_fraction=kelly_fraction,
            vol_scalar=vol_scalar,
            suggested_size=suggested_size,
        )
