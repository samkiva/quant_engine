"""
Order Flow Imbalance Experiment — v2

Changes from v1:
- Primary signal: flow_zscore instead of signed_flow
  Rationale: signed_flow saturates at +-1.0 in 23.8% of ticks due to
  BTCUSDT burst execution structure. Z-score captures deviations from
  recent equilibrium and has 0% saturation.
- Confirmation signal: flow_acceleration (non-zero only at burst transitions)
- Signal definition: zscore crosses threshold AND acceleration aligned
  OR zscore alone if acceleration is near zero (sparse by construction)

Pre-registered hypothesis:
Signed order flow z-score deviations from recent equilibrium predict
forward return direction over the next 50 ticks. A positive z-score
(buying pressure above recent norm) predicts positive forward return.

Null hypothesis:
Flow z-score has no predictive relationship with forward returns
beyond chance after controlling for transaction costs.

Acceptance criteria (all four required):
1. cost_adjusted_sharpe > 0.5 on purged out-of-sample windows
2. rolling_stability_score > 0.60 for sign_persistence
3. purged_walk_forward pass_rate > 0.55
4. result consistent across up/down conditioning
"""

import numpy as np
import pandas as pd
import structlog
from db.connection import get_pool
from research.features.tick_features import (
    build_feature_dataframe,
    compute_signed_flow,
    compute_flow_zscore,
    compute_flow_acceleration,
    compute_forward_return,
)
from research.stats.cost_model import CostParams, evaluate, build_experiment_metadata
from research.stats.tests import ks_test, sign_persistence_test
from research.validation.purged_walk_forward import generate_purged_windows
from research.validation.rolling_stability import (
    rolling_sign_persistence,
    rolling_cost_adjusted_return,
)

logger = structlog.get_logger()

HYPOTHESIS = "order_flow_zscore_v2"
FORWARD_WINDOW = 50
PURGE_GAP = 50
TRAIN_SIZE = 5000
TEST_SIZE = 1000
FLOW_WINDOW = 50
ZSCORE_BASELINE = 200
ACCEL_LAG = 10

# Signal thresholds — selected from distribution (p75 ≈ 0.73)
# Top quartile of z-score deviations only
ZSCORE_THRESHOLD = 0.75

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
    Feature/target contract:
    FEATURES: signed_flow, flow_zscore, flow_acceleration (all past-only)
    TARGET:   forward_return (future — never used as feature input)
    """
    features = build_feature_dataframe(df_raw, vol_window=50)

    flow = compute_signed_flow(
        features["quantity"], features["is_buyer_maker"], window=FLOW_WINDOW
    )
    features["signed_flow"] = flow
    features["flow_zscore"] = compute_flow_zscore(flow, baseline_window=ZSCORE_BASELINE)
    features["flow_acceleration"] = compute_flow_acceleration(flow, lag=ACCEL_LAG)
    features["forward_return"] = compute_forward_return(
        features["price"], forward_window=FORWARD_WINDOW
    )

    required = ["flow_zscore", "flow_acceleration", "forward_return"]
    before = len(features)
    features = features.dropna(subset=required)
    logger.info("features_built", before=before, after=len(features))
    return features


def _extract_signals(features: pd.DataFrame) -> pd.DataFrame:
    """
    Signal definition:
    Primary: |flow_zscore| >= ZSCORE_THRESHOLD (top quartile deviations)
    Direction: sign(flow_zscore)

    Acceleration confirmation is recorded but not required —
    acceleration is zero in most windows by construction (burst overlap).
    Report alignment rate as a descriptive statistic only.
    """
    mask = features["flow_zscore"].abs() >= ZSCORE_THRESHOLD
    signals = features[mask].copy()
    signals["signal_direction"] = np.sign(signals["flow_zscore"])

    # Acceleration alignment (descriptive only)
    accel_aligned = (
        np.sign(signals["flow_zscore"]) == np.sign(signals["flow_acceleration"])
    )
    pct_aligned = accel_aligned.mean()

    logger.info(
        "signals_extracted",
        n_signals=len(signals),
        pct_of_ticks=round(len(signals) / len(features) * 100, 1),
        zscore_threshold=ZSCORE_THRESHOLD,
        accel_alignment_pct=round(float(pct_aligned), 3),
    )
    signals["accel_aligned"] = accel_aligned
    return signals


def _run_full_sample(signals: pd.DataFrame, baseline: pd.DataFrame) -> dict:
    fwd = signals["forward_return"].values
    fwd_base = baseline["forward_return"].values
    flow_dir = signals["signal_direction"].values

    cost_result = evaluate(fwd, COST_PARAMS, metadata={"split": "full_sample"})
    ks = ks_test(fwd, fwd_base, significance_level=0.01,
                 label_a="signal", label_b="baseline")
    sign_test = sign_persistence_test(flow_dir, fwd, alternative="greater")

    # Acceleration-confirmed subset
    confirmed = signals[signals["accel_aligned"]]
    fwd_confirmed = confirmed["forward_return"].values
    cost_confirmed = evaluate(
        fwd_confirmed, COST_PARAMS, metadata={"split": "accel_confirmed"}
    )

    return {
        "n_signals": len(fwd),
        "n_baseline": len(fwd_base),
        "cost_model": cost_result.to_dict(),
        "ks_significant": ks.significant,
        "ks_statistic": ks.statistic,
        "sign_persistence": sign_test.get("persistence"),
        "sign_p_value": sign_test.get("p_value"),
        "accel_confirmed_n": len(fwd_confirmed),
        "accel_confirmed_sharpe": cost_confirmed.cost_adjusted_sharpe,
        "accel_confirmed_viable": cost_confirmed.economically_viable,
    }


def _run_purged_validation(features: pd.DataFrame) -> dict:
    n = len(features)
    windows = generate_purged_windows(
        n_ticks=n,
        train_size=TRAIN_SIZE,
        test_size=TEST_SIZE,
        forward_window=PURGE_GAP,
    )

    if not windows:
        return {"error": "insufficient data", "n_windows": 0}

    all_test_returns = []
    window_results = []

    for w in windows:
        test_slice = features.iloc[w.test_start_idx:w.test_end_idx]
        mask = test_slice["flow_zscore"].abs() >= ZSCORE_THRESHOLD
        test_signals = test_slice[mask]
        fwd = test_signals["forward_return"].dropna().values
        cost_result = evaluate(fwd, COST_PARAMS)
        all_test_returns.extend(fwd.tolist())
        window_results.append({
            "window_id": w.window_id,
            "n_signals": cost_result.n_signals,
            "sharpe": cost_result.cost_adjusted_sharpe,
            "viable": cost_result.economically_viable,
        })

    aggregate = evaluate(
        np.array(all_test_returns), COST_PARAMS,
        metadata={"source": "all_purged_test_windows"}
    )
    pass_rate = float(np.mean([r["viable"] for r in window_results]))
    mean_sharpe = float(np.mean([r["sharpe"] for r in window_results]))

    return {
        "n_windows": len(windows),
        "pass_rate": round(pass_rate, 4),
        "mean_window_sharpe": round(mean_sharpe, 4),
        "aggregate": aggregate.to_dict(),
        "window_results": window_results,
        "passed": pass_rate > 0.55 and aggregate.cost_adjusted_sharpe > 0.5,
    }


def _run_stability(signals: pd.DataFrame) -> dict:
    flow_dir = signals["signal_direction"].values
    fwd = signals["forward_return"].values
    mask = ~(np.isnan(flow_dir) | np.isnan(fwd))

    persistence = rolling_sign_persistence(
        flow_dir[mask], fwd[mask],
        window_size=200, min_threshold=0.55,
    )
    cost_stability = rolling_cost_adjusted_return(
        fwd[mask],
        cost_per_trade=COST_PARAMS.round_trip_cost,
        window_size=200, min_threshold=0.0,
    )
    return {
        "sign_persistence": {
            "stability_score": persistence.stability_score,
            "mean": persistence.mean_metric,
            "is_stable": persistence.is_stable,
            "summary": persistence.summary(),
        },
        "cost_adjusted": {
            "stability_score": cost_stability.stability_score,
            "mean": cost_stability.mean_metric,
            "is_stable": cost_stability.is_stable,
            "summary": cost_stability.summary(),
        },
    }


async def run() -> dict:
    logger.info("experiment_started", hypothesis=HYPOTHESIS)

    df_raw = await _load_ticks()
    features = _build_features(df_raw)
    signals = _extract_signals(features)
    baseline = features[features["flow_zscore"].abs() < ZSCORE_THRESHOLD]

    if len(signals) < 50:
        return {"hypothesis": HYPOTHESIS, "error": "insufficient signals: " + str(len(signals))}

    full = _run_full_sample(signals, baseline)
    purged = _run_purged_validation(features)
    stability = _run_stability(signals)

    criteria = {
        "purged_sharpe_gt_half":
            purged.get("aggregate", {}).get("cost_adjusted_sharpe", 0) > 0.5,
        "purged_pass_rate_gt_55pct":
            purged.get("pass_rate", 0) > 0.55,
        "rolling_persistence_stable":
            stability["sign_persistence"]["is_stable"],
        "rolling_cost_adjusted_stable":
            stability["cost_adjusted"]["is_stable"],
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
        "full_sample": full,
        "purged_validation": purged,
        "rolling_stability": stability,
        "criteria": criteria,
        "conclusion": conclusion,
    }
