import asyncio
import argparse
import importlib
from core.logging_setup import configure_logging
from db.connection import init_db_pool, get_pool

configure_logging()

EXPERIMENTS = {
    "vol_clustering": "research.experiments.vol_clustering",
    "intensity_lead": "research.experiments.intensity_lead",
    "regime_direction": "research.experiments.regime_direction",
    "order_flow": "research.experiments.order_flow",
}


def print_vol_clustering(result):
    ks = result["ks_test"]
    high = result["high_vol_distribution"]
    normal = result["normal_vol_distribution"]
    acf = result["vol_autocorrelation"]
    print("Conclusion: " + result["conclusion"])
    print("KS statistic=" + str(ks.statistic) + " p=" + str(ks.p_value) + " significant=" + str(ks.significant))
    print("High-vol mean=" + str(high["mean"]) + " n=" + str(high["n"]))
    print("Normal-vol mean=" + str(normal["mean"]) + " n=" + str(normal["n"]))
    for lag in range(1, 6):
        print("  Lag " + str(lag) + ": " + str(acf[lag]))


def print_intensity_lead(result):
    ccf = result["ccf"]
    partial = result["partial_correlation"]
    print("Interpretation: " + result["interpretation"])
    print("Peak lag=" + str(ccf.get("peak_lag")) + " peak_corr=" + str(ccf.get("peak_corr")))
    print("Partial corr=" + str(partial.get("partial_corr")) + " significant=" + str(partial.get("significant")))


def print_regime_direction(result):
    if "error" in result:
        print("Error: " + str(result["error"]))
        return
    print("Hypothesis: " + result["hypothesis"])
    print("Forward window: " + str(result["forward_window"]) + " ticks (pre-registered)")
    print("Regime entries: " + str(result["n_entries"]) + "  Baseline: " + str(result["n_baseline"]))
    print("Conclusion: " + result["conclusion"])
    print("Criteria:")
    for k, v in result["criteria"].items():
        print(("  PASS  " if v else "  FAIL  ") + k)
    fs = result["full_sample"]
    if fs.get("skipped"):
        print("Full sample skipped")
        return
    print("Full Sample n_entry=" + str(fs["n_entry"]) + "  n_baseline=" + str(fs["n_baseline"]))
    print("  Mean entry:    " + str(round(fs["mean_entry"], 6)))
    print("  Mean baseline: " + str(round(fs["mean_baseline"], 6)))
    print("  Mean excess:   " + str(round(fs["mean_excess"], 6)))
    print("  Economic edge: " + str(fs["economically_significant"]))
    print("  " + fs["ttest"].interpretation)
    sp = fs["sign_persistence"]
    print("  Sign persistence: " + str(sp.get("persistence")) + "  p=" + str(sp.get("p_value")))
    print("Temporal Stability: " + ("STABLE" if result["stable"] else "UNSTABLE"))
    for half in ["first_half", "second_half"]:
        h = result[half]
        if h.get("skipped"):
            print("  " + half + ": skipped")
            continue
        print("  " + half + ": n=" + str(h["n_entry"]) + "  excess=" + str(round(h["mean_excess"], 6)) + "  ttest_sig=" + str(h["ttest"].significant))
    for label in ["up_entry", "down_entry"]:
        r = result.get(label)
        if r and not r.get("skipped"):
            print(label + " n=" + str(r["n_entry"]) + "  excess=" + str(round(r["mean_excess"], 6)))
            print("  Sign persistence: " + str(r["sign_persistence"].get("persistence")))



def print_order_flow(result):
    if "error" in result:
        print("Error: " + str(result["error"]))
        return
    print("Hypothesis: " + result["hypothesis"])
    print("Signals: " + str(result["n_signals"]))
    meta = result.get("metadata", {})
    print("Metadata: forward_window=" + str(meta.get("forward_window")) + " purge_gap=" + str(meta.get("purge_gap")))
    print("Conclusion: " + result["conclusion"])
    print("Criteria:")
    for k, v in result["criteria"].items():
        print(("  PASS  " if v else "  FAIL  ") + k)
    fs = result["full_sample"]
    cm = fs.get("cost_model", {})
    print("Full Sample:")
    print("  n_signals=" + str(fs["n_signals"]) + "  n_baseline=" + str(fs["n_baseline"]))
    print("  cost_adjusted_sharpe=" + str(cm.get("cost_adjusted_sharpe")))
    print("  mean_raw=" + str(cm.get("mean_raw_return")))
    print("  mean_adjusted=" + str(cm.get("mean_cost_adjusted_return")))
    print("  viable=" + str(cm.get("economically_viable")))
    print("  ks_significant=" + str(fs["ks_test_significant"]))
    print("  sign_persistence=" + str(fs["sign_persistence"]))
    pv = result["purged_validation"]
    print("Purged Validation:")
    print("  n_windows=" + str(pv.get("n_windows")))
    print("  pass_rate=" + str(pv.get("pass_rate")))
    print("  mean_window_sharpe=" + str(pv.get("mean_window_sharpe")))
    agg = pv.get("aggregate_cost_model", {})
    print("  aggregate_sharpe=" + str(agg.get("cost_adjusted_sharpe")))
    st = result["rolling_stability"]
    print("Rolling Stability:")
    print("  " + st["sign_persistence"]["summary"])
    print("  " + st["cost_adjusted_return"]["summary"])

PRINTERS = {
    "vol_clustering": print_vol_clustering,
    "intensity_lead": print_intensity_lead,
    "regime_direction": print_regime_direction,
    "order_flow": print_order_flow,
}


async def main(experiment):
    await init_db_pool()
    pool = None
    try:
        module = importlib.import_module(EXPERIMENTS[experiment])
        print("=" * 60)
        print("EXPERIMENT: " + experiment)
        print("=" * 60)
        result = await module.run()
        PRINTERS[experiment](result)
    finally:
        try:
            pool = get_pool()
            await pool.close()
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", choices=list(EXPERIMENTS.keys()), required=True)
    args = parser.parse_args()
    asyncio.run(main(args.experiment))
