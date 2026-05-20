import asyncio
from core.logging_setup import configure_logging
from db.connection import init_db_pool, close_db_pool
from research.validation.walk_forward import generate_windows
from research.validation.fingerprint import fingerprint_dataset
from research.validation.hypothesis import HYPOTHESIS_REGISTRY
from backtesting.datasource import PostgresDataSource
from backtesting.strategy import VWAPCrossStrategy
from backtesting.portfolio import Portfolio
from backtesting.engine import BacktestEngine
from backtesting.metrics import BacktestMetrics

configure_logging()

TABLE = "mainnet_trades"
SYMBOL = "BTCUSDT"
HYPOTHESIS_KEY = "vwap_cross_v1"


async def run_window_backtest(
    window,
    strategy_class,
) -> BacktestMetrics:
    strategy = strategy_class()
    portfolio = Portfolio(initial_cash=10_000.0, trade_quantity=0.001)
    datasource = PostgresDataSource(
        symbol=SYMBOL,
        start_time=window.test_start,
        end_time=window.test_end,
        table_name=TABLE,
    )
    engine = BacktestEngine(datasource, strategy, portfolio)
    return await engine.run()


async def main() -> None:
    await init_db_pool()

    hypothesis = HYPOTHESIS_REGISTRY[HYPOTHESIS_KEY]
    print("\n" + "="*60)
    print(hypothesis.summary())
    print("="*60)

    # Fingerprint the full dataset for reproducibility
    fp = await fingerprint_dataset(TABLE, SYMBOL)
    print(f"\nDataset fingerprint: {fp['fingerprint']}")
    print(f"Total trades:        {fp['trade_count']:,}")

    # Generate walk-forward windows
    windows = await generate_windows(
        table=TABLE,
        symbol=SYMBOL,
        train_hours=0.5,    # 30-min train
        test_hours=0.25,    # 15-min test
        min_train_trades=50,
        min_test_trades=25,
    )

    if not windows:
        print("\nInsufficient data for walk-forward windows.")
        print("Continue collecting mainnet data and retry.")
        await close_db_pool()
        return

    print(f"\nWalk-forward windows: {len(windows)}")
    print("-"*60)

    results = []
    for w in windows:
        metrics = await run_window_backtest(w, VWAPCrossStrategy)
        results.append(metrics)
        status = "PASS" if (
            metrics.sharpe_ratio >= hypothesis.min_sharpe and
            metrics.win_rate >= hypothesis.min_win_rate
        ) else "FAIL"
        print(
            f"Window {w.window_id:2d} | "
            f"trades={metrics.total_trades:3d} | "
            f"win={metrics.win_rate:.0%} | "
            f"sharpe={metrics.sharpe_ratio:6.3f} | "
            f"pnl=${metrics.total_pnl:7.4f} | "
            f"{status}"
        )

    # Summary
    if results:
        passed = sum(1 for m in results
                     if m.sharpe_ratio >= hypothesis.min_sharpe
                     and m.win_rate >= hypothesis.min_win_rate)
        print("-"*60)
        print(f"Windows passed: {passed}/{len(results)}")
        avg_sharpe = sum(m.sharpe_ratio for m in results) / len(results)
        avg_winrate = sum(m.win_rate for m in results) / len(results)
        print(f"Avg Sharpe:     {avg_sharpe:.4f}")
        print(f"Avg Win Rate:   {avg_winrate:.1%}")
        verdict = "HYPOTHESIS SUPPORTED" if passed > len(results) * 0.6 else "HYPOTHESIS REJECTED"
        print(f"\nVerdict: {verdict}")

    await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
