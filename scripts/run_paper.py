import asyncio
from core.logging_setup import configure_logging
from db.connection import init_db_pool, close_db_pool
from db.session_tracker import record_connect, record_disconnect
from cache.redis_client import init_redis, close_redis
from backtesting.strategy import VWAPCrossStrategy
from backtesting.portfolio import Portfolio
from paper_trading.live_datasource import LiveDataSource
from paper_trading.risk_layer import RiskLayer, RiskConfig
from paper_trading.paper_engine import PaperEngine
from config.settings import settings

configure_logging()

SYMBOL = settings.ws_symbol
STRATEGY_NAME = "vwap_cross_v1"


async def main() -> None:
    await init_db_pool()
    await init_redis()

    session_id = await record_connect(SYMBOL)

    datasource = LiveDataSource(symbol=SYMBOL)
    strategy = VWAPCrossStrategy()
    portfolio = Portfolio(initial_cash=10_000.0, trade_quantity=0.001)
    risk = RiskLayer(
        portfolio=portfolio,
        config=RiskConfig(
            max_daily_loss_pct=2.0,
            max_position_pct=10.0,
            max_signals_per_minute=10,
            post_reconnect_hold_secs=5.0,
        ),
    )

    engine = PaperEngine(
        datasource=datasource,
        strategy=strategy,
        portfolio=portfolio,
        risk_layer=risk,
        strategy_name=STRATEGY_NAME,
        session_id=session_id,
        report_interval=100,
    )

    trades_received = 0
    try:
        metrics = await engine.run()
        trades_received = engine.tick_count

        print("\n=== Paper Trading Session Results ===")
        print(f"Ticks processed:   {engine.tick_count}")
        print(f"Total trades:      {metrics.total_trades}")
        print(f"Win rate:          {metrics.win_rate:.1%}")
        print(f"Total P&L:         ${metrics.total_pnl:.4f}")
        print(f"Max drawdown:      {metrics.max_drawdown_pct:.2f}%")
        print(f"Sharpe ratio:      {metrics.sharpe_ratio:.4f}")

    except KeyboardInterrupt:
        trades_received = engine.tick_count
    finally:
        await record_disconnect(
            session_id,
            trades_received,
            reason="paper_session_ended",
            is_clean=True,
        )
        await close_db_pool()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
