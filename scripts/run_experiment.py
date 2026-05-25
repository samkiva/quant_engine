import asyncio
from core.logging_setup import configure_logging
from db.connection import init_db_pool, close_db_pool

configure_logging()


async def main() -> None:
    await init_db_pool()

    from research.experiments.vol_clustering import run
    result = await run()

    ks = result["ks_test"]
    high = result["high_vol_distribution"]
    normal = result["normal_vol_distribution"]
    acf = result["vol_autocorrelation"]

    print("\n" + "="*60)
    print("EXPERIMENT: Volatility Clustering")
    print("="*60)

    print(f"\nConclusion: {result['conclusion']}")
    print(f"\nKS Test:")
    print(f"  Statistic:   {ks.statistic:.6f}")
    print(f"  P-value:     {ks.p_value:.6f}")
    print(f"  Significant: {ks.significant} (threshold: {ks.significance_level})")
    print(f"  {ks.interpretation}")

    print(f"\nHigh-Vol Regime future volatility (n={high['n']:,}):")
    print(f"  Mean:   {high['mean']:.6f}")
    print(f"  Median: {high['median']:.6f}")
    print(f"  P90:    {high['p90']:.6f}")

    print(f"\nNormal-Vol Regime future volatility (n={normal['n']:,}):")
    print(f"  Mean:   {normal['mean']:.6f}")
    print(f"  Median: {normal['median']:.6f}")
    print(f"  P90:    {normal['p90']:.6f}")

    print(f"\nVolatility Autocorrelation (lag 1-5):")
    for lag in range(1, 6):
        bar = "█" * int(abs(acf[lag]) * 40)
        sign = "+" if acf[lag] > 0 else "-"
        print(f"  Lag {lag}: {sign}{abs(acf[lag]):.4f} {bar}")

    await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
