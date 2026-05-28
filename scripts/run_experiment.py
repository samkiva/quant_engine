import argparse
import asyncio
import importlib

from db.connection import get_pool, init_db_pool


EXPERIMENTS = {
    "vol_clustering": "research.experiments.vol_clustering",
    "intensity_lead": "research.experiments.intensity_lead",
    "regime_direction": "research.experiments.regime_direction",
}


def _print_vol_clustering(result: dict) -> None:
    if "error" in result:
        print(f"\nError: {result['error']}")
        return

    print(f"\nInterpretation: {result['interpretation']}")
    print(f"Usable rows: {result['usable_rows']}")

    print("\nKolmogorov-Smirnov Test:")

    ks = result["ks_test"]

    print(f"  Statistic:    {ks.statistic}")
    print(f"  P-value:      {ks.p_value}")
    print(f"  Significant:  {ks.significant}")

    print("\nDistribution A:")

    for k, v in result["distribution_a"].items():
        print(f"  {k}: {v}")

    print("\nDistribution B:")

    for k, v in result["distribution_b"].items():
        print(f"  {k}: {v}")


def _print_intensity_lead(result: dict) -> None:
    if "error" in result:
        print(f"\nError: {result['error']}")
        return

    print(f"\nInterpretation: {result['interpretation']}")
    print(f"Usable rows: {result['usable_rows']}")

    print("\nCross-Correlation (intensity_spike → vol_expansion):")

    ccf = result["cross_correlation"]

    print(f"  Peak lag:      {ccf['peak_lag']}")
    print(f"  Peak corr:     {ccf['peak_corr']}")
    print(f"  Bartlett SE:   {ccf['bartlett_se']}")

    print(
        f"  Significant lags (>3 SE): "
        f"{ccf['significant_lags']}"
    )

    print("\n  CCF at key lags:")

    for lag, val in ccf["selected_lags"].items():
        bar = "█" * int(abs(val) * 100)

        print(f"    Lag {lag:>2}: {val:.4f} {bar}")

    print(
        "\nPartial Correlation "
        "(controlling for current rolling_vol):"
    )

    pc = result["partial_correlation"]

    print(f"  Partial corr:  {pc['partial_corr']}")
    print(f"  Significant:   {pc['significant']}")
    print(f"  Bartlett SE:   {pc['bartlett_se']}")

    print("\nRegime-Conditional Analysis:")

    for regime, data in result["regime_analysis"].items():
        print(
            f"  {regime}: "
            f"n={data['n']}  "
            f"peak_lag={data['peak_lag']}  "
            f"peak_corr={data['peak_corr']}  "
            f"ccf_lag1={data['ccf_lag1']}"
        )

    print("\nTemporal Stability:")

    for split, data in result["stability_analysis"].items():
        print(
            f"  {split}: "
            f"n={data['n']}  "
            f"peak_lag={data['peak_lag']}  "
            f"peak_corr={data['peak_corr']}  "
            f"ccf_lag1={data['ccf_lag1']}"
        )

    print(f"  Stable: {result['stable']}")


def _print_regime_direction(result: dict) -> None:
    if "error" in result:
        print(f"\nError: {result['error']}")
        return

    print(
        "\nHypothesis: "
        "H2 — Mean Reversion after high-vol regime entry"
    )

    print(
        f"Forward window: "
        f"{result['forward_window']} ticks (pre-registered)"
    )

    print(f"Regime entries: {result['n_entries']}")

    print(f"\nConclusion: {result['conclusion']}")

    print("\nCriteria:")

    for k, v in result["criteria"].items():
        status = "PASS" if v else "FAIL"
        print(f"  {status}  {k}")

    fs = result["full_sample"]

    print(f"\nFull Sample (n={fs['n']}):")

    print(
        f"  Mean forward return: "
        f"{fs['mean_forward_return']:.6f}"
    )

    print(
        f"  Round-trip cost: "
        f"{fs['round_trip_cost']:.4f}"
    )

    print(
        f"  Economic edge: "
        f"{fs['economically_significant']}"
    )

    print(
        f"  T-test ({fs['ttest'].test_name}): "
        f"p={fs['ttest'].p_value:.4f}  "
        f"significant={fs['ttest'].significant}"
    )

    print(f"  {fs['ttest'].interpretation}")

    sp = fs["sign_persistence"]

    print(
        f"  Sign persistence: "
        f"{sp.get('persistence')}  "
        f"p={sp.get('p_value')}  "
        f"{sp.get('interpretation')}"
    )

    print(
        f"\nTemporal Stability: "
        f"{'STABLE' if result['stable'] else 'UNSTABLE'}"
    )

    for half in ["first_half", "second_half"]:
        h = result[half]

        print(
            f"  {half}: "
            f"n={h['n']}  "
            f"mean={h['mean_forward_return']:.6f}  "
            f"ttest_sig={h['ttest'].significant}"
        )

    for label in ["up_entry", "down_entry"]:
        r = result.get(label)

        if r:
            print(f"\n{label} (n={r['n']}):")

            print(
                f"  Mean forward return: "
                f"{r['mean_forward_return']:.6f}"
            )

            print(
                f"  Sign persistence: "
                f"{r['sign_persistence'].get('persistence')}"
            )


async def main(experiment: str) -> None:
    if experiment not in EXPERIMENTS:
        raise ValueError(
            f"Unknown experiment '{experiment}'. "
            f"Available: {list(EXPERIMENTS.keys())}"
        )

    # Initialize database pool FIRST
    await init_db_pool()

    print("\n" + "=" * 60)
    print(f"EXPERIMENT: {experiment}")
    print("=" * 60)

    module = importlib.import_module(EXPERIMENTS[experiment])

    result = await module.run()

    if experiment == "vol_clustering":
        _print_vol_clustering(result)

    elif experiment == "intensity_lead":
        _print_intensity_lead(result)

    elif experiment == "regime_direction":
        _print_regime_direction(result)

    # Graceful async cleanup
    pool = get_pool()

    if pool is not None:
        await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--experiment",
        required=True,
        choices=EXPERIMENTS.keys(),
    )

    args = parser.parse_args()

    asyncio.run(main(args.experiment))
