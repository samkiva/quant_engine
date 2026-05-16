import pytest
from datetime import datetime, timezone
from backtesting.portfolio import Portfolio, Trade
from backtesting.metrics import compute_metrics, _compute_max_drawdown, _compute_sharpe


def make_trade(entry: float, exit: float, qty: float = 0.001) -> Trade:
    now = datetime.now(tz=timezone.utc)
    pnl = (exit - entry) * qty
    return Trade(
        entry_time=now, exit_time=now,
        entry_price=entry, exit_price=exit,
        quantity=qty, side="LONG", pnl=pnl,
    )


def test_max_drawdown_flat():
    assert _compute_max_drawdown([100.0, 100.0, 100.0]) == 0.0


def test_max_drawdown_declining():
    dd = _compute_max_drawdown([100.0, 90.0, 80.0])
    assert dd == pytest.approx(20.0)


def test_max_drawdown_recovery():
    dd = _compute_max_drawdown([100.0, 80.0, 110.0])
    assert dd == pytest.approx(20.0)


def test_sharpe_flat_returns_zero():
    assert _compute_sharpe([100.0, 100.0, 100.0]) == 0.0


def test_sharpe_positive_trend():
    curve = [100.0 + i for i in range(20)]
    sharpe = _compute_sharpe(curve)
    assert sharpe > 0


def test_no_trades_returns_empty_metrics():
    p = Portfolio()
    m = compute_metrics(p, [])
    assert m.total_trades == 0
    assert m.win_rate == 0.0
    assert m.total_pnl == 0.0


def test_win_rate_all_wins():
    p = Portfolio()
    p._closed_trades = [make_trade(80000, 81000), make_trade(80000, 81000)]
    m = compute_metrics(p, [10000.0, 10001.0, 10002.0])
    assert m.win_rate == pytest.approx(1.0)
    assert m.winning_trades == 2
    assert m.losing_trades == 0


def test_profit_factor():
    p = Portfolio()
    p._closed_trades = [
        make_trade(80000, 81000),  # +1.0
        make_trade(80000, 79000),  # -1.0
    ]
    m = compute_metrics(p, [10000.0, 10001.0, 10000.0])
    assert m.profit_factor == pytest.approx(1.0)


def test_total_pnl():
    p = Portfolio()
    p._closed_trades = [
        make_trade(80000, 81000),  # +1.0
        make_trade(80000, 82000),  # +2.0
    ]
    m = compute_metrics(p, [10000.0, 10001.0, 10003.0])
    assert m.total_pnl == pytest.approx(3.0)
