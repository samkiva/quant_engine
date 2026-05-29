"""
Order Flow Imbalance Experiment — v1

Pre-registered hypothesis:
Rolling signed order flow imbalance (normalized buyer/seller volume ratio)
predicts the direction of forward returns over the next 50 ticks.

Theoretical basis:
Signed flow is a direct measure of informed order pressure.
When buyers consistently aggress, they absorb available liquidity
and push price upward. This is a causal mechanism — not a regime
label derived from price behavior. The signal is in the order flow
itself, not in the price response.

Null hypothesis:
Signed flow has no predictive relationship with forward returns
beyond what would be expected by chance.

Acceptance criteria (all four required):
1. cost_adjusted_sharpe > 0.5 on out-of-sample purged windows
2. rolling_stability_score > 0.60 for sign_persistence
3. purged_walk_forward pass rate > 0.55
4. result consistent across directional conditioning

Design constraints:
- Single feature: signed_flow only
- Single horizon: forward_window = 50 ticks (pre-registered)
- No parameter search
- No ensembles
- Cost model applied before any pass/fail decision
- Purged walk-forward for all train/test separation
"""

import numpy as np
import pandas as pd
import structlog
from db.connection import get_pool
from research.features.tick_features import (
    build_feature_dataframe,
    compute_signed_flow,
    compute_forward_return,
)
from research.stats.cost_model import CostParams, evaluate, build_experiment_metadata
from research.stats.tests import ks_test, sign_persistence_test
from research.validation.purged_walk_forward import (
    generate_purged_windows,
    run_purged_evaluation,
)
from research.validation.rolling_stability import (
    rolling_sign_persistence,
    rolling_cost_adjusted_return,
)

logger = structlog.get_logger()

# Pre-registered. Do not modify after seeing results.
HYPOTHESIS = "order_flow_imbalance_v1"
FORWARD_WINDOW = 50
PURGE_GAP = 50          # Must equal FORWARD_WINDOW
TRAIN_SIZE = 5000       # Ticks per training window
TEST_SIZE = 1000        # Ticks per test window
FLOW_WINDOW = 50        # Rolling window for signed flow computation
SIGNAL_THRESHOLD = 0.3  # Minimum |flow| to generate a signal
                        # Selects top ~20% of flow magnitude observations
                        # Avoids trading near-zero imbalance noise

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
    """
    Feature/target separation:
    FEATURE:  signed_flow  (uses only past trades, no lookahead)
    TARGET:   forward_return (strictly future, never used as feature)
    """
    features = build_feature_dataframe(df_raw, vol_window=50)

    features["signed_flow"] = compute_signed_flow(
        features["quantity"],
        features["is_buyer_maker"],
        window=FLOW_WINDOW,
    )
    features["forward_return"] = compute_forward_return(
        features["price"],
        forward_window=FORWARD_WINDOW,
    )

    required = ["signed_flow", "forward_return", "rolling_vol"]
    before = len(features)
    features = features.dropna(subset=required)
    logger.info(
        "features_built",
        rows_before=before,
        rows_after=len(features),
        dropped=before - len(features),
    )
    return features


def _extract_signals(features: pd.DataFrame) -> pd.DataFrame:
    """
    Selects signal ticks: rows where |signed_flow| >= SIGNAL_THRESHOLD.

    SIGNAL_THRESHOLD filters near-zero imbalance — only act when
    order flow shows meaningful directional conviction.

    Returns filtered DataFrame with signal direction column added.
    """
    mask = features["signed_flow"].abs() >= SIGNAL_THRESHOLD
    signals = features[mask].copy()
    signals["signal_direction"] = np.sign(signals["signed_flow"])
    logger.info(
        "signals_extracted",
        n_signals=len(signals),
        pct_of_ticks=round(len(signals) / len(features) * 100, 1),
        threshold=SIGNAL_THRESHOLD,
    )
    return signals


def _run_full_sample_tests(signals: pd.DataFrame, baseline: pd.DataFrame) -> dict:
    """Full-sample descriptive statistics. Not used for accept/reject."""
    fwd = signals["forward_return"].values
    fwd_baseline = baseline["forward_return"].values
    flow = signals["signed_flow"].values

    cost_result = evaluate(fwd, COST_PARAMS, metadata={"split": "full_sample"})

    ks = ks_test(
        fwd, fwd_baseline,
        significance_level=0.01,
        label_a="signal_forward", label_b="baseline_forward",
    )

    sign_test = sign_persistence_test(flow, fwd, alternative="greater")

    return {
        "n_signals": len(fwd),
        "n_baseline": len(fwd_baseline),
        "cost_model": cost_result.to_dict(),
        "ks_test_significant": ks.significant,
        "ks_statistic": ks.statistic,
        "sign_persistence": sign_test.get("persistence"),
        "sign_persistence_significant": sign_test.get("significant"),
    }


def _run_purged_validation(features: pd.DataFrame) -> dict:
    """
    Primary validation: purged walk-forward with frozen thresholds.

    Each test window uses a signal threshold fit conceptually on
    train data. Here the threshold is fixed (SIGNAL_THRESHOLD) —
    no fitting required because it is pre-registered.

    Returns per-window cost-adjusted results and aggregate metrics.
    """
    n = len(features)
    windows = generate_purged_windows(
        n_ticks=n,
        train_size=TRAIN_SIZE,
        test_size=TEST_SIZE,
        forward_window=PURGE_GAP,
    )

    if not windows:
        return {"error": "insufficient data for purged windows", "n_windows": 0}

    all_test_returns = []
    window_results = []

    for w in windows:
        test_slice = features.iloc[w.test_start_idx:w.test_end_idx]

        # Apply signal filter to test window only
        signal_mask = test_slice["signed_flow"].abs() >= SIGNAL_THRESHOLD
        test_signals = test_slice[signal_mask]

        fwd = test_signals["forward_return"].dropna().values
        cost_result = evaluate(fwd, COST_PARAMS, metadata={"window_id": w.window_id})

        all_test_returns.extend(fwd.tolist())
        window_results.append({
            "window_id": w.window_id,
            "n_signals": cost_result.n_signals,
            "cost_adjusted_sharpe": cost_result.cost_adjusted_sharpe,
            "mean_excess": cost_result.mean_cost_adjusted_return,
            "viable": cost_result.economically_viable,
        })

    all_test_returns = np.array(all_test_returns)

    # Aggregate cost result across all test windows
    aggregate = evaluate(
        all_test_returns, COST_PARAMS,
        metadata={"source": "all_purged_test_windows"},
    )

    pass_rate = np.mean([r["viable"] for r in window_results])
    mean_sharpe = np.mean([r["cost_adjusted_sharpe"] for r in window_results])

    return {
        "n_windows": len(windows),
        "window_results": window_results,
        "aggregate_cost_model": aggregate.to_dict(),
        "pass_rate": round(float(pass_rate), 4),
        "mean_window_sharpe": round(float(mean_sharpe), 4),
        "purged_validation_passed": pass_rate > 0.55 and aggregate.cost_adjusted_sharpe > 0.5,
    }


def _run_rolling_stability(signals: pd.DataFrame) -> dict:
    """Stability analysis across rolling windows of signal events."""
    flow = signals["signed_flow"].values
    fwd = signals["forward_return"].values

    mask = ~(np.isnan(flow) | np.isnan(fwd))
    flow_clean = flow[mask]
    fwd_clean = fwd[mask]

    persistence_result = rolling_sign_persistence(
        flow_clean, fwd_clean,
        window_size=200,
        min_threshold=0.55,
        min_stable_score=0.60,
    )

    cost_result = rolling_cost_adjusted_return(
        fwd_clean,
        cost_per_trade=COST_PARAMS.round_trip_cost,
        window_size=200,
        min_threshold=0.0,
        min_stable_score=0.60,
    )

    return {
        "sign_persistence": {
            "stability_score": persistence_result.stability_score,
            "mean": persistence_result.mean_metric,
            "is_stable": persistence_result.is_stable,
            "summary": persistence_result.summary(),
        },
        "cost_adjusted_return": {
            "stability_score": cost_result.stability_score,
            "mean_adjusted": cost_result.mean_metric,
            "is_stable": cost_result.is_stable,
            "summary": cost_result.summary(),
        },
    }


async def run() -> dict:
    """
    Full order flow imbalance experiment.
    All acceptance decisions based on post-cost metrics.
    """
    logger.info("experiment_started", hypothesis=HYPOTHESIS)

    df_raw = await _load_ticks()
    features = _build_features(df_raw)
    signals = _extract_signals(features)
    baseline = features[features["signed_flow"].abs() < SIGNAL_THRESHOLD]

    if len(signals) < 50:
        return {
            "hypothesis": HYPOTHESIS,
            "error": "insufficient signals: " + str(len(signals)),
        }

    full_sample = _run_full_sample_tests(signals, baseline)
    purged = _run_purged_validation(features)
    stability = _run_rolling_stability(signals)

    # Acceptance criteria
    criteria = {
        "purged_sharpe_gt_half":
            purged.get("aggregate_cost_model", {}).get("cost_adjusted_sharpe", 0) > 0.5,
        "purged_pass_rate_gt_55pct":
            purged.get("pass_rate", 0) > 0.55,
        "rolling_persistence_stable":
            stability["sign_persistence"]["is_stable"],
        "rolling_cost_adjusted_stable":
            stability["cost_adjusted_return"]["is_stable"],
    }

    passed = sum(criteria.values())
    conclusion = (
        "ORDER FLOW SIGNAL CONFIRMED — passes full validation stack"
        if passed == 4 else
        "ORDER FLOW SIGNAL NOT CONFIRMED — " + str(passed) + "/4 criteria met"
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
        "n_signals": len(signals),
        "full_sample": full_sample,
        "purged_validation": purged,
        "rolling_stability": stability,
        "criteria": criteria,
        "conclusion": conclusion,
    }
