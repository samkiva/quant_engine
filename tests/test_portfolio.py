import pytest
from datetime import datetime, timezone
from backtesting.datasource import Tick
from backtesting.strategy import Signal, StrategySignal
from backtesting.portfolio import Portfolio


def make_tick(price: float, ts: datetime = None) -> Tick:
    return Tick(
        timestamp=ts or datetime.now(tz=timezone.utc),
        symbol="BTCUSDT",
        price=price,
        quantity=0.001,
        is_buyer_maker=False,
    )


def make_signal(signal: Signal, price: float) -> StrategySignal:
    return StrategySignal(signal=signal, tick=make_tick(price), reason="test")


def test_initial_state():
    p = Portfolio(initial_cash=10_000.0, trade_quantity=0.001)
    assert p.cash == 10_000.0
    assert p.open_position is None
    assert p.closed_trades == []


def test_buy_reduces_cash():
    p = Portfolio(initial_cash=10_000.0, trade_quantity=0.001)
    p.on_signal(make_signal(Signal.BUY, 80_000.0))
    assert p.cash == pytest.approx(10_000.0 - 80.0)


def test_buy_opens_position():
    p = Portfolio(initial_cash=10_000.0, trade_quantity=0.001)
    p.on_signal(make_signal(Signal.BUY, 80_000.0))
    assert p.open_position is not None
    assert p.open_position.entry_price == 80_000.0


def test_sell_closes_position():
    p = Portfolio(initial_cash=10_000.0, trade_quantity=0.001)
    p.on_signal(make_signal(Signal.BUY, 80_000.0))
    p.on_signal(make_signal(Signal.SELL, 81_000.0))
    assert p.open_position is None
    assert len(p.closed_trades) == 1


def test_profitable_trade_pnl():
    p = Portfolio(initial_cash=10_000.0, trade_quantity=0.001)
    p.on_signal(make_signal(Signal.BUY, 80_000.0))
    p.on_signal(make_signal(Signal.SELL, 81_000.0))
    trade = p.closed_trades[0]
    assert trade.pnl == pytest.approx(1.0)  # (81000 - 80000) * 0.001


def test_losing_trade_pnl():
    p = Portfolio(initial_cash=10_000.0, trade_quantity=0.001)
    p.on_signal(make_signal(Signal.BUY, 80_000.0))
    p.on_signal(make_signal(Signal.SELL, 79_000.0))
    trade = p.closed_trades[0]
    assert trade.pnl == pytest.approx(-1.0)


def test_double_buy_ignored():
    p = Portfolio(initial_cash=10_000.0, trade_quantity=0.001)
    p.on_signal(make_signal(Signal.BUY, 80_000.0))
    cash_after_first = p.cash
    p.on_signal(make_signal(Signal.BUY, 80_000.0))
    assert p.cash == cash_after_first


def test_sell_without_position_ignored():
    p = Portfolio(initial_cash=10_000.0, trade_quantity=0.001)
    p.on_signal(make_signal(Signal.SELL, 80_000.0))
    assert p.closed_trades == []


def test_current_value_with_open_position():
    p = Portfolio(initial_cash=10_000.0, trade_quantity=0.001)
    p.on_signal(make_signal(Signal.BUY, 80_000.0))
    value = p.current_value(current_price=82_000.0)
    # cash after buy + position at current price
    expected = (10_000.0 - 80.0) + (82_000.0 * 0.001)
    assert value == pytest.approx(expected)


def test_insufficient_funds_skips_buy():
    p = Portfolio(initial_cash=10.0, trade_quantity=0.001)
    p.on_signal(make_signal(Signal.BUY, 80_000.0))
    assert p.open_position is None
    assert p.cash == 10.0
