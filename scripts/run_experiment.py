import asyncio
import argparse
from core.logging_setup import configure_logging
from db.connection import init_db_pool, close_db_pool

configure_logging()

EXPERIMENTS = {
    "vol_clustering": "research.experiments.vol_clustering",
    "intensity_lead": "research.experiments.intensity_lead",
}


def _print_vol_clustering(result: dict) -> None:
    ks = result["ks_test"]
    high = result["high_vol_distribution"]
    normal = result["normal_vol_distribution"]
    acf = result["vol_autocorrelation"]

    print(f"\nConclusion: {result['conclusion']}")
    print(f"\nKS Test:")
    print(f"  Statistic:   {ks.statistic:.6f}")
    print(f"  P-value:     {ks.p_value:.6f}")
    print(f"  Significant: {ks.significant} (threshold: {ks.significance_level})")
    print(f"  {ks.interpretation}")
    print(f"\nHigh-Vol future vol (n={high['n']:,}):   mean={high['mean']:.6f}  p90={high['p90']:.6f}")
    print(f"Normal-Vol future vol (n={normal['n']:,}): mean={normal['mean']:.6f}  p90={normal['p90']:.6f}")
    print(f"\nVolatility Autocorrelation:")
    for lag in range(1, 6):
        bar = "█" * int(abs(acf[lag]) * 40)
        print(f"  Lag {lag}: {acf[lag]:+.4f} {bar}")


def _print_intensity_lead(result: dict) -> None:
    ccf = result["ccf"]
    partial = result["partial_correlation"]
    regime = result["regime_analysis"]
    stability = result["temporal_stability"]

    print(f"\nInterpretation: {result['interpretation']}")
    print(f"Usable rows: {result['n']:,}")

    print(f"\nCross-Correlation (intensity_spike → vol_expansion):")
    print(f"  Peak lag:      {ccf.get('peak_lag')}")
    print(f"  Peak corr:     {ccf.get('peak_corr')}")
    print(f"  Bartlett SE:   {ccf.get('bartlett_se')}")
    sig_lags = ccf.get("significant_lags", {})
    print(f"  Significant lags (>3 SE): {sorted(sig_lags.keys()) if sig_lags else 'none'}")
    print(f"\n  CCF at key lags:")
    ccf_vals = ccf.get("ccf", {})
    for lag in [1, 2, 5, 10, 20, 50]:
        val = ccf_vals.get(lag, float("nan"))
        if val != float("nan"):
            bar = "█" * int(abs(val) * 200)
            sign = "+" if val >= 0 else "-"
            print(f"    Lag {lag:2d}: {sign}{abs(val):.4f} {bar}")

    print(f"\nPartial Correlation (controlling for current rolling_vol):")
    print(f"  Partial corr:  {partial.get('partial_corr')}")
    print(f"  Significant:   {partial.get('significant')}")
    print(f"  Bartlett SE:   {partial.get('bartlett_se')}")

    print(f"\nRegime-Conditional Analysis:")
    for regime_name in ["high_vol", "normal_vol"]:
        r = regime.get(regime_name, {})
        if r.get("skipped"):
            print(f"  {regime_name}: skipped (n={r.get('n')})")
        else:
            print(f"  {regime_name}: n={r.get('n'):,}  peak_lag={r.get('peak_lag')}  "
                  f"peak_corr={r.get('peak_corr')}  ccf_lag1={r.get('ccf_lag1')}")

    print(f"\nTemporal Stability:")
    for half in ["first_half", "second_half"]:
        s = stability.get(half, {})
        if s.get("skipped"):
            print(f"  {half}: skipped (n={s.get('n')})")
        else:
            print(f"  {half}: n={s.get('n'):,}  peak_lag={s.get('peak_lag')}  "
                  f"peak_corr={s.get('peak_corr')}  ccf_lag1={s.get('ccf_lag1')}")
    print(f"  Stable: {stability.get('stable')}")


async def main(experiment: str) -> None:
    await init_db_pool()

    import importlib
    module_path = EXPERIMENTS[experiment]
    module = importlib.import_module(module_path)

    print("\n" + "="*60)
    print(f"EXPERIMENT: {experiment}")
    print("="*60)

    result = await module.run()

    if experiment == "vol_clustering":
        _print_vol_clustering(result)
    elif experiment == "intensity_lead":
        _print_intensity_lead(result)

    await close_db_pool()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a research experiment")
    parser.add_argument(
        "--experiment",
        choices=list(EXPERIMENTS.keys()),
        default="vol_clustering",
        help="Experiment to run",
    )
    args = parser.parse_args()
    asyncio.run(main(args.experiment))
