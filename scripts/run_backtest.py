import asyncio
from core.logging_setup import configure_logging
from db.connection import init_db_pool, close_db_pool
from backtesting.datasource import PostgresDataSource
from backtesting.strategy import VWAPCrossStrategy
from backtesting.portfolio import Portfolio
from backtesting.engine import BacktestEngine

configure_logging()


async def main() -> None:
    await init_db_pool()

    datasource = PostgresDataSource(symbol="BTCUSDT")
    strategy = VWAPCrossStrategy()
    portfolio = Portfolio(initial_cash=10_000.0, trade_quantity=0.001)
    engine = BacktestEngine(datasource, strategy, portfolio)

    metrics = await engine.run()

    print("\n=== Backtest Results ===")
    print(f"Total ticks processed: {engine._tick_count}")
    print(f"Total trades:          {metrics.total_trades}")
    print(f"Winning trades:        {metrics.winning_trades}")
    print(f"Losing trades:         {metrics.losing_trades}")
    print(f"Win rate:              {metrics.win_rate:.1%}")
    print(f"Total P&L:             ${metrics.total_pnl:.4f}")
    print(f"Total return:          {metrics.total_return_pct:.4f}%")
    print(f"Max drawdown:          {metrics.max_drawdown_pct:.2f}%")
    print(f"Sharpe ratio:          {metrics.sharpe_ratio:.4f}")
    print(f"Avg win:               ${metrics.avg_win:.4f}")
    print(f"Avg loss:              ${metrics.avg_loss:.4f}")
    print(f"Profit factor:         {metrics.profit_factor:.2f}")

    await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
