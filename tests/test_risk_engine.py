import pytest
from datetime import datetime, timezone
from backtesting.datasource import Tick
from backtesting.strategy import Signal, StrategySignal
from backtesting.portfolio import Portfolio
from risk.engine import RiskEngineV2
from risk.kelly import compute_kelly
from risk.volatility_sizing import compute_vol_scalar


def make_tick(price: float = 80000.0) -> Tick:
    return Tick(
        timestamp=datetime.now(tz=timezone.utc),
        symbol="BTCUSDT",
        price=price,
        quantity=0.001,
        is_buyer_maker=False,
    )


def make_signal(sig: Signal, price: float = 80000.0) -> StrategySignal:
    return StrategySignal(signal=sig, tick=make_tick(price), reason="test")


def make_engine() -> tuple[RiskEngineV2, Portfolio]:
    p = Portfolio(initial_cash=10_000.0, trade_quantity=0.001)
    e = RiskEngineV2(portfolio=p)
    return e, p


# Kelly tests
def test_kelly_insufficient_trades():
    result = compute_kelly([1.0], [-0.5])
    assert not result.is_valid
    assert "insufficient" in result.reason


def test_kelly_negative_edge():
    # 20% win rate, 1:1 win/loss — negative edge
    wins = [1.0] * 6
    losses = [-1.0] * 24
    result = compute_kelly(wins, losses)
    assert not result.is_valid
    assert result.raw_kelly < 0


def test_kelly_positive_edge():
    # 60% win rate, 1.5:1 win/loss — positive edge
    wins = [1.5] * 18 + [1.5] * 0
    losses = [-1.0] * 12
    wins = [1.5] * 18
    losses = [-1.0] * 12
    result = compute_kelly(wins, losses)
    assert result.is_valid
    assert result.capped_kelly > 0
    assert result.capped_kelly <= 0.02  # Always capped at 2%


def test_kelly_capped_at_max():
    # Very high win rate — Kelly would say bet everything
    wins = [2.0] * 28
    losses = [-0.5] * 2
    result = compute_kelly(wins, losses)
    assert result.capped_kelly <= 0.02


# Volatility sizing tests
def test_vol_scalar_insufficient_data():
    assert compute_vol_scalar([0.001] * 5) == 1.0


def test_vol_scalar_high_vol_reduces_size():
    high_vol_returns = [0.05, -0.04, 0.06, -0.05] * 10
    scalar = compute_vol_scalar(high_vol_returns)
    assert scalar < 1.0


def test_vol_scalar_low_vol_increases_size():
    low_vol_returns = [0.0001, -0.0001] * 20
    scalar = compute_vol_scalar(low_vol_returns)
    assert scalar > 1.0


def test_vol_scalar_bounded():
    extreme_returns = [1.0, -1.0] * 20
    scalar = compute_vol_scalar(extreme_returns)
    assert scalar >= 0.1
    assert scalar <= 2.0


# Risk engine tests
def test_hold_always_passes():
    engine, _ = make_engine()
    decision = engine.evaluate(make_signal(Signal.HOLD))
    assert decision.allowed
    assert decision.reason == "hold_no_check"


def test_kill_switch_blocks_all():
    engine, _ = make_engine()
    engine.activate_kill_switch()
    decision = engine.evaluate(make_signal(Signal.BUY))
    assert not decision.allowed
    assert "kill_switch" in decision.reason


def test_kill_switch_deactivate():
    engine, _ = make_engine()
    engine.activate_kill_switch()
    engine.deactivate_kill_switch()
    decision = engine.evaluate(make_signal(Signal.BUY))
    assert decision.allowed


def test_daily_loss_triggers_kill_switch():
    engine, portfolio = make_engine()
    # Simulate massive loss — portfolio value near zero
    engine._daily_start_value = 10_000.0
    portfolio._cash = 100.0  # 99% loss
    decision = engine.evaluate(make_signal(Signal.BUY, price=80000.0))
    assert not decision.allowed
    assert engine.kill_switch_active


def test_post_reconnect_hold():
    engine, _ = make_engine()
    engine.notify_reconnect(datetime.now(tz=timezone.utc))
    decision = engine.evaluate(make_signal(Signal.BUY))
    assert not decision.allowed
    assert "post_reconnect" in decision.reason


def test_signal_approved_no_history():
    engine, _ = make_engine()
    decision = engine.evaluate(make_signal(Signal.BUY))
    assert decision.allowed
    assert decision.reason == "all_checks_passed"


def test_cooldown_after_loss():
    engine, _ = make_engine()
    engine.record_loss()
    decision = engine.evaluate(make_signal(Signal.BUY))
    assert not decision.allowed
    assert "cooldown" in decision.reason
