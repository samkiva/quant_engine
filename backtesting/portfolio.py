from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from backtesting.datasource import Tick
from backtesting.strategy import Signal, StrategySignal


@dataclass
class Trade:
    """Records a single executed trade."""
    entry_time:  datetime
    exit_time:   Optional[datetime]
    entry_price: float
    exit_price:  Optional[float]
    quantity:    float
    side:        str  # "LONG" only for now
    pnl:         Optional[float] = None


class Portfolio:
    """
    Tracks cash, position, and trade history during a backtest.

    Design decisions:
    - Long-only (no short selling) in Phase 4
    - Fixed quantity per trade (no position sizing yet)
    - No leverage, no margin
    - Transaction costs ignored (Phase 5 will add slippage/fees)

    These simplifications are intentional. Adding complexity before
    the framework is validated produces unreliable results.
    """

    def __init__(
        self,
        initial_cash: float = 10_000.0,
        trade_quantity: float = 0.001,  # BTC per trade
    ) -> None:
        self._cash = initial_cash
        self._initial_cash = initial_cash
        self._trade_quantity = trade_quantity
        self._position: Optional[Trade] = None
        self._closed_trades: list[Trade] = []

    def on_signal(self, signal: StrategySignal) -> None:
        tick = signal.tick

        if signal.signal == Signal.BUY and self._position is None:
            cost = tick.price * self._trade_quantity
            if cost > self._cash:
                return  # Insufficient funds — skip
            self._cash -= cost
            self._position = Trade(
                entry_time=tick.timestamp,
                exit_time=None,
                entry_price=tick.price,
                exit_price=None,
                quantity=self._trade_quantity,
                side="LONG",
            )

        elif signal.signal == Signal.SELL and self._position is not None:
            proceeds = tick.price * self._position.quantity
            pnl = proceeds - (self._position.entry_price * self._position.quantity)
            self._cash += proceeds
            self._position.exit_time = tick.timestamp
            self._position.exit_price = tick.price
            self._position.pnl = pnl
            self._closed_trades.append(self._position)
            self._position = None

    def current_value(self, current_price: float) -> float:
        """Total portfolio value: cash + open position mark-to-market."""
        position_value = 0.0
        if self._position is not None:
            position_value = current_price * self._position.quantity
        return self._cash + position_value

    @property
    def closed_trades(self) -> list[Trade]:
        return self._closed_trades.copy()

    @property
    def initial_cash(self) -> float:
        return self._initial_cash

    @property
    def cash(self) -> float:
        return self._cash

    @property
    def open_position(self) -> Optional[Trade]:
        return self._position
