"""
Run Exhaustion and Mean Reversion Experiment — v1

Pre-registered hypothesis:
Extended one-sided order flow runs (>= 20 consecutive same-side trades)
produce statistically significant mean reversion upon termination.
The reversion direction opposes the run direction.

Baseline: Option B — mid-run ticks only.
Isolates the termination effect from run-membership effect.

Threshold = 20 ticks. Fixed. Not optimized post-result.
Forward window = 50 ticks. Pre-registered.
"""

import numpy as np
import pandas as pd
import structlog
from scipy import stats as scipy_stats
from db.connection import get_pool
from research.features.tick_features import (
    build_feature_dataframe,
    compute_run_length,
    compute_signed_flow,
    compute_flow_zscore,
    compute_forward_return,
)
from research.stats.cost_model import CostParams, evaluate, build_experiment_metadata
from research.stats.tests import two_sample_ttest
from research.validation.purged_walk_forward import generate_purged_windows
from research.validation.rolling_stability import (
    rolling_sign_persistence,
    rolling_cost_adjusted_return,
)

logger = structlog.get_logger()

HYPOTHESIS = "run_exhaustion_mean_reversion_v1"
MIN_RUN_LENGTH = 20         # Fixed. Not optimized.
FORWARD_WINDOW = 50         # Pre-registered.
FLOW_WINDOW = 50
ZSCORE_BASELINE = 200
TRAIN_SIZE = 5000
TEST_SIZE = 1000
PURGE_GAP = 50              # Must equal FORWARD_WINDOW.
MIN_SIGNALS = 30

COST_PARAMS = CostParams(
    entry_cost_pct=0.001,
    exit_cost_pct=0.001,
    slippage_pct=0.0,
)


async def _load_ticks(limit: int = 50000, batch_size: int = 5000) -> pd.DataFrame:
    pool = get_pool()
    all_rows = []
    offset = 0
    while offset < limit:
        current_batch = min(batch_size, limit - offset)
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT trade_time, price, quantity, is_buyer_maker
                FROM mainnet_trades
                WHERE symbol = 'BTCUSDT'
                ORDER BY trade_time ASC
                LIMIT $1 OFFSET $2
            """, current_batch, offset)
        if not rows:
            break
        all_rows.extend(list(rows))
        offset += current_batch
        logger.info("batch_loaded", offset=offset, total=len(all_rows))

    df = pd.DataFrame(all_rows, columns=[
        "trade_time", "price", "quantity", "is_buyer_maker"
    ])
    df["price"] = df["price"].astype(float)
    df["quantity"] = df["quantity"].astype(float)
    df = df.set_index("trade_time")
    df.index = pd.to_datetime(df.index, utc=True)
    return df


def _build_features(df_raw: pd.DataFrame) -> pd.DataFrame:
    features = build_feature_dataframe(df_raw, vol_window=50)
    features["streak"] = compute_run_length(features["is_buyer_maker"])
    flow = compute_signed_flow(
        features["quantity"], features["is_buyer_maker"], window=FLOW_WINDOW
    )
    features["signed_flow"] = flow
    features["flow_zscore"] = compute_flow_zscore(flow, baseline_window=ZSCORE_BASELINE)
    features["forward_return"] = compute_forward_return(
        features["price"], forward_window=FORWARD_WINDOW
    )
    features["side"] = features["is_buyer_maker"].map(
        {False: 1.0, True: -1.0}
    )
    return features


def _detect_events(features: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Detects run termination events and mid-run baseline ticks.

    Termination tick: first contra-side trade after a run >= MIN_RUN_LENGTH.
    Detected at tick t where side[t] != side[t-1] and streak[t-1] >= MIN_RUN_LENGTH.
    No lookahead: streak[t-1] is known at tick t.

    Mid-run baseline: ticks strictly inside a qualifying run.
    tick t qualifies if: streak[t] >= 2 AND the run containing t
    has total length >= MIN_RUN_LENGTH AND t is not the last tick of the run.
    Identified retrospectively — acceptable for research labeling.

    Sample attrition is reported at every stage.
    """
    n = len(features)
    side = features["side"].values
    streak = features["streak"].values

    # --- Termination events ---
    term_indices = []
    for i in range(1, n):
        if side[i] != side[i - 1] and streak[i - 1] >= MIN_RUN_LENGTH:
            term_indices.append(i)

    n_raw_terminations = len(term_indices)
    logger.info("raw_terminations_detected", n=n_raw_terminations)

    # Enforce non-overlapping forward windows
    non_overlapping = []
    last_accepted = -FORWARD_WINDOW
    for idx in term_indices:
        if idx - last_accepted >= FORWARD_WINDOW:
            non_overlapping.append(idx)
            last_accepted = idx

    n_non_overlapping = len(non_overlapping)
    logger.info("non_overlapping_terminations", n=n_non_overlapping)

    # Build termination DataFrame
    term_rows = []
    for idx in non_overlapping:
        if idx + FORWARD_WINDOW >= n:
            continue
        fwd = features["forward_return"].iloc[idx]
        if pd.isna(fwd):
            continue
        completed_run_direction = side[idx - 1]
        adjusted_return = fwd * (-completed_run_direction)
        term_rows.append({
            "idx": idx,
            "completed_run_length": streak[idx - 1],
            "run_direction": completed_run_direction,
            "forward_return": fwd,
            "adjusted_return": adjusted_return,
            "flow_zscore_at_term": features["flow_zscore"].iloc[idx],
            "rolling_vol_at_term": features["rolling_vol"].iloc[idx],
        })

    term_df = pd.DataFrame(term_rows)
    n_valid_terminations = len(term_df)
    logger.info("valid_terminations", n=n_valid_terminations)

    # --- Mid-run baseline ---
    # Identify all runs >= MIN_RUN_LENGTH and mark their middle ticks
    mid_indices = []
    i = 0
    while i < n:
        run_start = i
        run_side = side[i]
        j = i
        while j < n and side[j] == run_side:
            j += 1
        run_length = j - run_start
        if run_length >= MIN_RUN_LENGTH:
            # Middle ticks: index 1 through run_length-2 (0-indexed within run)
            for k in range(run_start + 1, j - 1):
                if k + FORWARD_WINDOW < n:
                    fwd = features["forward_return"].iloc[k]
                    if not pd.isna(fwd):
                        adjusted = fwd * (-run_side)
                        mid_indices.append({
                            "idx": k,
                            "run_length": run_length,
                            "run_direction": run_side,
                            "forward_return": fwd,
                            "adjusted_return": adjusted,
                        })
        i = j

    mid_df = pd.DataFrame(mid_indices) if mid_indices else pd.DataFrame()
    n_mid = len(mid_df)
    logger.info("mid_run_baseline_ticks", n=n_mid)

    attrition = {
        "n_raw_terminations": n_raw_terminations,
        "n_non_overlapping": n_non_overlapping,
        "n_valid_terminations": n_valid_terminations,
        "n_mid_run_baseline": n_mid,
        "attrition_pct": round(
            (1 - n_valid_terminations / n_raw_terminations) * 100, 1
        ) if n_raw_terminations > 0 else 100.0,
    }

    return term_df, mid_df, attrition


def _run_full_sample_tests(term_df: pd.DataFrame, mid_df: pd.DataFrame) -> dict:
    """
    Full-sample statistical tests.
    Baseline = mid-run ticks (Option B).
    Primary metric = adjusted_return (sign-normalized against run direction).
    """
    adj = term_df["adjusted_return"].dropna().values
    fwd_raw = term_df["forward_return"].dropna().values
    base_adj = mid_df["adjusted_return"].dropna().values if len(mid_df) > 0 else np.array([])

    # Reversal rate = P(adjusted_return > 0)
    reversal_rate = float(np.mean(adj > 0))

    # Two-sample Welch t-test vs mid-run baseline
    if len(base_adj) >= 10:
        ttest = two_sample_ttest(
            adj, base_adj,
            alternative="greater",
            label_a="termination", label_b="mid_run",
        )
    else:
        ttest = None

    cost_result = evaluate(fwd_raw, COST_PARAMS)
    cost_adjusted_adj = evaluate(
        adj - COST_PARAMS.round_trip_cost, COST_PARAMS,
        metadata={"note": "adjusted_return_minus_cost"}
    )

    # Sign persistence on adjusted returns
    n = len(adj)
    n_positive = int(np.sum(adj > 0))
    if n >= 10:
        binom = scipy_stats.binomtest(n_positive, n, p=0.5, alternative="greater")
        reversal_p = binom.pvalue
        reversal_significant = reversal_p < 0.01
    else:
        reversal_p = float("nan")
        reversal_significant = False

    return {
        "n_terminations": len(adj),
        "n_mid_run_baseline": len(base_adj),
        "reversal_rate": round(reversal_rate, 4),
        "reversal_p_value": round(float(reversal_p), 6) if not np.isnan(reversal_p) else None,
        "reversal_significant": reversal_significant,
        "mean_adjusted_return": round(float(np.mean(adj)), 8),
        "mean_raw_forward_return": round(float(np.mean(fwd_raw)), 8),
        "mean_baseline_adjusted": round(float(np.mean(base_adj)), 8) if len(base_adj) > 0 else None,
        "ttest": ttest,
        "cost_model_raw": cost_result.to_dict(),
        "run_length_distribution": {
            "mean": round(float(term_df["completed_run_length"].mean()), 1),
            "median": round(float(term_df["completed_run_length"].median()), 1),
            "p90": round(float(term_df["completed_run_length"].quantile(0.90)), 1),
            "max": int(term_df["completed_run_length"].max()),
        },
    }


def _run_directional_conditioning(term_df: pd.DataFrame) -> dict:
    """
    Tests whether buyer-run terminations and seller-run terminations
    produce consistent adjusted returns.
    Both must show reversal_rate > 0.5 for criterion 4 to pass.
    """
    results = {}
    for direction, label in [(1.0, "buyer_run"), (-1.0, "seller_run")]:
        subset = term_df[term_df["run_direction"] == direction]
        adj = subset["adjusted_return"].dropna().values
        if len(adj) < 10:
            results[label] = {"n": len(adj), "skipped": True}
            continue
        reversal_rate = float(np.mean(adj > 0))
        mean_adj = float(np.mean(adj))
        results[label] = {
            "n": len(adj),
            "reversal_rate": round(reversal_rate, 4),
            "mean_adjusted_return": round(mean_adj, 8),
        }
        logger.info(
            "directional_conditioning",
            label=label, n=len(adj),
            reversal_rate=round(reversal_rate, 4),
            mean_adj=round(mean_adj, 8),
        )
    return results


def _run_purged_validation(features: pd.DataFrame) -> dict:
    """
    Purged walk-forward on adjusted returns at termination events.
    Each test window uses only its own termination events.
    Threshold fixed at MIN_RUN_LENGTH — not refit per window.
    """
    n = len(features)
    windows = generate_purged_windows(
        n_ticks=n,
        train_size=TRAIN_SIZE,
        test_size=TEST_SIZE,
        forward_window=PURGE_GAP,
    )
    if not windows:
        return {"error": "insufficient data", "n_windows": 0}

    side = features["side"].values
    streak = features["streak"].values
    all_adj_returns = []
    window_results = []

    for w in windows:
        test_slice_indices = range(w.test_start_idx, w.test_end_idx)
        adj_returns = []
        for i in test_slice_indices:
            if i == 0 or i + FORWARD_WINDOW >= n:
                continue
            if side[i] != side[i - 1] and streak[i - 1] >= MIN_RUN_LENGTH:
                fwd = features["forward_return"].iloc[i]
                if pd.isna(fwd):
                    continue
                completed_dir = side[i - 1]
                adj = fwd * (-completed_dir)
                adj_returns.append(adj)

        if len(adj_returns) < 3:
            window_results.append({
                "window_id": w.window_id, "n": 0,
                "viable": False, "sharpe": 0.0, "skipped": True,
            })
            continue

        adj_arr = np.array(adj_returns)
        raw_for_cost = features["forward_return"].iloc[
            [i for i in test_slice_indices
             if i > 0 and i + FORWARD_WINDOW < n
             and side[i] != side[i-1]
             and streak[i-1] >= MIN_RUN_LENGTH
             and not pd.isna(features["forward_return"].iloc[i])]
        ].values

        cost_result = evaluate(raw_for_cost, COST_PARAMS)
        all_adj_returns.extend(adj_returns)

        window_results.append({
            "window_id": w.window_id,
            "n": len(adj_returns),
            "reversal_rate": round(float(np.mean(adj_arr > 0)), 4),
            "mean_adj": round(float(np.mean(adj_arr)), 8),
            "viable": cost_result.economically_viable,
            "sharpe": cost_result.cost_adjusted_sharpe,
            "skipped": False,
        })

    if not all_adj_returns:
        return {"error": "no valid observations across windows", "n_windows": len(windows)}

    agg_raw = []
    for w in windows:
        for i in range(w.test_start_idx, w.test_end_idx):
            if i == 0 or i + FORWARD_WINDOW >= n:
                continue
            if side[i] != side[i-1] and streak[i-1] >= MIN_RUN_LENGTH:
                fwd = features["forward_return"].iloc[i]
                if not pd.isna(fwd):
                    agg_raw.append(fwd)

    agg_cost = evaluate(np.array(agg_raw), COST_PARAMS)
    valid = [r for r in window_results if not r.get("skipped")]
    pass_rate = float(np.mean([r["viable"] for r in valid])) if valid else 0.0

    return {
        "n_windows": len(windows),
        "n_valid_windows": len(valid),
        "pass_rate": round(pass_rate, 4),
        "aggregate_sharpe": agg_cost.cost_adjusted_sharpe,
        "aggregate_viable": agg_cost.economically_viable,
        "mean_reversal_rate_across_windows": round(
            float(np.mean([r["reversal_rate"] for r in valid])), 4
        ) if valid else None,
        "window_results": window_results,
        "passed": pass_rate > 0.55 and agg_cost.cost_adjusted_sharpe > 0.5,
    }


def _run_stability(term_df: pd.DataFrame) -> dict:
    """Rolling stability on termination events."""
    adj = term_df["adjusted_return"].dropna().values
    dummy_entry = np.ones(len(adj))  # direction already encoded in adjusted_return

    persistence = rolling_sign_persistence(
        dummy_entry, adj,
        window_size=min(100, len(adj) // 3),
        min_threshold=0.55,
        min_stable_score=0.60,
    )
    cost_stab = rolling_cost_adjusted_return(
        term_df["forward_return"].dropna().values,
        cost_per_trade=COST_PARAMS.round_trip_cost,
        window_size=min(100, len(adj) // 3),
        min_threshold=0.0,
        min_stable_score=0.60,
    )
    return {
        "sign_persistence": {
            "stability_score": persistence.stability_score,
            "mean": persistence.mean_metric,
            "is_stable": persistence.is_stable,
            "summary": persistence.summary(),
        },
        "cost_adjusted": {
            "stability_score": cost_stab.stability_score,
            "mean": cost_stab.mean_metric,
            "is_stable": cost_stab.is_stable,
            "summary": cost_stab.summary(),
        },
    }


async def run() -> dict:
    logger.info("experiment_started", hypothesis=HYPOTHESIS)

    df_raw = await _load_ticks()
    features = _build_features(df_raw)
    features = features.dropna(subset=["rolling_vol", "flow_zscore"])

    term_df, mid_df, attrition = _detect_events(features)

    logger.info("attrition_summary", **attrition)

    if len(term_df) < MIN_SIGNALS:
        return {
            "hypothesis": HYPOTHESIS,
            "attrition": attrition,
            "error": "insufficient termination events: " + str(len(term_df)),
        }

    full = _run_full_sample_tests(term_df, mid_df)
    directional = _run_directional_conditioning(term_df)
    purged = _run_purged_validation(features)
    stability = _run_stability(term_df)

    criteria = {
        "ttest_significant": (
            full["ttest"].significant if full["ttest"] else False
        ),
        "cost_adjusted_sharpe_gt_half": (
            purged.get("aggregate_sharpe", 0) > 0.5
        ),
        "rolling_stability_gt_60pct": (
            stability["sign_persistence"]["is_stable"]
        ),
        "consistent_across_directions": (
            all(
                not v.get("skipped") and v["reversal_rate"] > 0.5
                for v in directional.values()
            )
        ),
    }

    passed = sum(criteria.values())
    conclusion = (
        "RUN EXHAUSTION CONFIRMED — mean reversion passes full validation"
        if passed == 4 else
        "RUN EXHAUSTION NOT CONFIRMED — " + str(passed) + "/4 criteria met"
    )

    meta = build_experiment_metadata(
        in_sample_rows=TRAIN_SIZE,
        out_of_sample_rows=TEST_SIZE,
        purge_gap=PURGE_GAP,
        forward_window=FORWARD_WINDOW,
        cost_params=COST_PARAMS,
    )

    return {
        "hypothesis": HYPOTHESIS,
        "metadata": meta,
        "attrition": attrition,
        "full_sample": full,
        "directional_conditioning": directional,
        "purged_validation": purged,
        "rolling_stability": stability,
        "criteria": criteria,
        "conclusion": conclusion,
    }
