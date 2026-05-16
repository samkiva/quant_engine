from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Optional
from backtesting.datasource import Tick


class Signal(Enum):
    BUY  = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass(frozen=True)
class StrategySignal:
    """
    The output of a strategy for a given tick.
    Immutable — a signal is a decision made at a point in time.
    """
    signal: Signal
    tick: Tick
    reason: str  # Human-readable explanation — forces you to articulate why


class Strategy(ABC):
    """
    Base class for all trading strategies.

    Subclasses implement on_tick() and return a StrategySignal.
    The strategy has no knowledge of portfolio state, cash, or positions.
    It only looks at price/volume data and returns a signal.

    This separation is deliberate:
    - Strategy logic stays pure and testable
    - Position sizing and risk management live in Portfolio
    - The engine coordinates between them
    """

    @abstractmethod
    def on_tick(self, tick: Tick) -> StrategySignal:
        """
        Called for every tick in chronological order.
        Must return a StrategySignal — never raises exceptions.
        """
        ...

    @abstractmethod
    def reset(self) -> None:
        """
        Resets internal state for walk-forward validation.
        Called between train and test windows.
        """
        ...


class VWAPCrossStrategy(Strategy):
    """
    Simple VWAP crossover strategy.

    Signal logic:
    - BUY  when price crosses above VWAP from below
    - SELL when price crosses below VWAP from above
    - HOLD otherwise

    This is a mean-reversion/trend-following hybrid.
    On a short window it tends toward mean reversion;
    on a longer window it can trend-follow.

    IMPORTANT: This strategy is for framework demonstration only.
    Do not draw conclusions about its profitability from testnet data.
    """

    def __init__(self) -> None:
        self._cumulative_notional: float = 0.0
        self._cumulative_volume: float = 0.0
        self._prev_above_vwap: Optional[bool] = None

    @property
    def vwap(self) -> Optional[float]:
        if self._cumulative_volume == 0:
            return None
        return self._cumulative_notional / self._cumulative_volume

    def on_tick(self, tick: Tick) -> StrategySignal:
        # Update VWAP state
        self._cumulative_notional += tick.price * tick.quantity
        self._cumulative_volume += tick.quantity
        current_vwap = self.vwap

        if current_vwap is None:
            return StrategySignal(Signal.HOLD, tick, "insufficient data")

        currently_above = tick.price > current_vwap

        # Detect crossover
        if self._prev_above_vwap is None:
            self._prev_above_vwap = currently_above
            return StrategySignal(Signal.HOLD, tick, "initialising")

        if not self._prev_above_vwap and currently_above:
            self._prev_above_vwap = currently_above
            return StrategySignal(
                Signal.BUY, tick,
                f"price {tick.price:.2f} crossed above VWAP {current_vwap:.2f}"
            )

        if self._prev_above_vwap and not currently_above:
            self._prev_above_vwap = currently_above
            return StrategySignal(
                Signal.SELL, tick,
                f"price {tick.price:.2f} crossed below VWAP {current_vwap:.2f}"
            )

        self._prev_above_vwap = currently_above
        return StrategySignal(Signal.HOLD, tick, "no crossover")

    def reset(self) -> None:
        self._cumulative_notional = 0.0
        self._cumulative_volume = 0.0
        self._prev_above_vwap = None
