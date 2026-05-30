"""
Horizon Sweep Experiment

Research question:
Is the observed directional persistence (sign_persistence 0.894 at H=50)
economically meaningful at longer horizons, or is it a short-lived
microstructure effect that decays before costs can be recovered?

Method:
For each horizon H in HORIZONS:
1. Detect signal events using regime_direction (high-vol entry) signal
2. Enforce minimum spacing of H ticks between accepted signals
   to guarantee non-overlapping forward return windows
3. Compute forward_return over exactly H ticks at each signal
4. Apply cost model — same round-trip cost regardless of horizon
5. Record decay curve metrics

The non-overlapping constraint reduces sample size as H grows.
Sample size is reported explicitly for every horizon.
Do not interpret results at H >= 1000 without noting n < 200.

No new features. No parameter search. No optimization.
Existing cost model, signal definition, and statistical tests only.
"""

import numpy as np
import pandas as pd
import structlog
from scipy import stats as scipy_stats
from db.connection import get_pool
from research.features.tick_features import (
    build_feature_dataframe,
    compute_signed_flow,
    compute_flow_zscore,
)
from research.stats.cost_model import CostParams, evaluate, build_experiment_metadata

logger = structlog.get_logger()

HORIZONS = [50, 100, 250, 500, 1000, 2000]

COST_PARAMS = CostParams(
    entry_cost_pct=0.001,
    exit_cost_pct=0.001,
    slippage_pct=0.0,
)

# Regime signal parameters — unchanged from regime_direction experiment
VOL_WINDOW = 50
VOL_PERCENTILE = 90

# Order flow signal parameters — unchanged from order_flow experiment
FLOW_WINDOW = 50
ZSCORE_BASELINE = 200
ZSCORE_THRESHOLD = 0.75

MIN_SIGNALS_FOR_ANALYSIS = 30


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


def _detect_regime_entries(features: pd.DataFrame) -> np.ndarray:
    """
    Returns integer array of tick indices where high-vol regime entry occurs.
    Uses expanding percentile — identical to regime_direction experiment.
    No lookahead.
    """
    vols = features["rolling_vol"].values
    high_vol = np.zeros(len(vols), dtype=int)

    for i in range(VOL_WINDOW - 1, len(vols)):
        if np.isnan(vols[i]):
            continue
        hist = vols[:i + 1]
        hist = hist[~np.isnan(hist)]
        threshold = np.percentile(hist, VOL_PERCENTILE)
        if vols[i] >= threshold:
            high_vol[i] = 1

    raw_entries = []
    for i in range(1, len(high_vol)):
        if high_vol[i] == 1 and high_vol[i - 1] == 0:
            raw_entries.append(i)

    return np.array(raw_entries, dtype=int)


def _detect_flow_entries(features: pd.DataFrame) -> np.ndarray:
    """
    Returns integer array of tick indices where |flow_zscore| >= threshold.
    Direction encoded as sign(zscore).
    """
    zscore = features["flow_zscore"].values
    mask = np.abs(zscore) >= ZSCORE_THRESHOLD
    return np.where(mask)[0].astype(int)


def _enforce_spacing(entry_indices: np.ndarray, spacing: int) -> np.ndarray:
    """
    Filters entry indices to guarantee minimum spacing between accepted entries.
    Greedy forward pass: accept first valid entry, then skip until spacing is met.

    This ensures forward return windows of length H do not overlap,
    preserving statistical independence between observations.
    """
    if len(entry_indices) == 0:
        return entry_indices

    accepted = [entry_indices[0]]
    for idx in entry_indices[1:]:
        if idx - accepted[-1] >= spacing:
            accepted.append(idx)

    return np.array(accepted, dtype=int)


def _compute_forward_returns_at(
    prices: np.ndarray,
    entry_indices: np.ndarray,
    horizon: int,
) -> np.ndarray:
    """
    Computes log forward return over exactly H ticks at each entry index.
    Entries within H ticks of end of series are excluded (no future data).
    """
    n = len(prices)
    returns = []
    for idx in entry_indices:
        if idx + horizon >= n:
            continue
        if prices[idx] <= 0 or prices[idx + horizon] <= 0:
            continue
        ret = np.log(prices[idx + horizon] / prices[idx])
        returns.append(ret)
    return np.array(returns, dtype=float)


def _compute_entry_returns_at(
    prices: np.ndarray,
    entry_indices: np.ndarray,
    lookback: int,
) -> np.ndarray:
    """
    Computes log return over last H ticks at each entry index.
    Used for sign_persistence computation.
    """
    returns = []
    for idx in entry_indices:
        if idx < lookback:
            returns.append(float("nan"))
            continue
        if prices[idx] <= 0 or prices[idx - lookback] <= 0:
            returns.append(float("nan"))
            continue
        ret = np.log(prices[idx] / prices[idx - lookback])
        returns.append(ret)
    return np.array(returns, dtype=float)


def _compute_baseline_returns(
    prices: np.ndarray,
    entry_indices: np.ndarray,
    horizon: int,
) -> np.ndarray:
    """
    Computes forward returns at all non-signal ticks.
    Used for excess return computation.
    Signal ticks excluded from baseline.
    """
    n = len(prices)
    signal_set = set(entry_indices.tolist())
    baseline = []
    for idx in range(0, n - horizon, horizon):  # non-overlapping
        if idx in signal_set:
            continue
        if prices[idx] <= 0 or prices[idx + horizon] <= 0:
            continue
        ret = np.log(prices[idx + horizon] / prices[idx])
        baseline.append(ret)
    return np.array(baseline, dtype=float)


def _confidence_interval(returns: np.ndarray, confidence: float = 0.95) -> tuple:
    """Two-sided t-interval for mean return."""
    n = len(returns)
    if n < 2:
        return (float("nan"), float("nan"))
    mean = np.mean(returns)
    se = scipy_stats.sem(returns)
    t_crit = scipy_stats.t.ppf((1 + confidence) / 2, df=n - 1)
    return (round(mean - t_crit * se, 8), round(mean + t_crit * se, 8))


def _sweep_one_horizon(
    prices: np.ndarray,
    entry_indices: np.ndarray,
    entry_returns_h50: np.ndarray,
    horizon: int,
    signal_name: str,
) -> dict:
    """
    Runs full metric suite for one horizon value.
    entry_returns_h50: the 50-tick entry return used for sign_persistence baseline.
    """
    spaced = _enforce_spacing(entry_indices, spacing=horizon)
    n_raw = len(entry_indices)
    n_spaced = len(spaced)

    if n_spaced < MIN_SIGNALS_FOR_ANALYSIS:
        logger.info(
            "horizon_skipped",
            H=horizon, signal=signal_name,
            n_raw=n_raw, n_spaced=n_spaced,
            reason="below_min_signals",
        )
        return {
            "H": horizon,
            "signal": signal_name,
            "n_raw": n_raw,
            "n_spaced": n_spaced,
            "skipped": True,
            "reason": "n < " + str(MIN_SIGNALS_FOR_ANALYSIS),
        }

    fwd_returns = _compute_forward_returns_at(prices, spaced, horizon)
    entry_r = _compute_entry_returns_at(prices, spaced, min(horizon, 50))
    baseline_r = _compute_baseline_returns(prices, spaced, horizon)

    if len(fwd_returns) < MIN_SIGNALS_FOR_ANALYSIS:
        return {
            "H": horizon, "signal": signal_name,
            "n_spaced": n_spaced, "n_valid": len(fwd_returns),
            "skipped": True, "reason": "insufficient valid returns",
        }

    cost_result = evaluate(fwd_returns, COST_PARAMS)

    mean_raw = float(np.mean(fwd_returns))
    mean_baseline = float(np.mean(baseline_r)) if len(baseline_r) > 0 else 0.0
    mean_excess = mean_raw - mean_baseline

    # Sign persistence — align by spaced entry indices that produced valid returns
    # fwd_returns may be shorter than entry_r if tail entries lack future data
    min_len = min(len(entry_r), len(fwd_returns))
    entry_r_aligned = entry_r[:min_len]
    fwd_aligned = fwd_returns[:min_len]
    mask = ~(np.isnan(entry_r_aligned) | np.isnan(fwd_aligned))
    e_clean = entry_r_aligned[mask]
    f_clean = fwd_aligned[mask]
    if len(e_clean) >= 10:
        nonzero = (e_clean != 0) & (f_clean != 0)
        sign_pers = float(np.mean(np.sign(e_clean[nonzero]) == np.sign(f_clean[nonzero])))
        binom = scipy_stats.binomtest(
            int(np.sum(np.sign(e_clean[nonzero]) == np.sign(f_clean[nonzero]))),
            int(np.sum(nonzero)), p=0.5, alternative="greater"
        )
        sign_p = binom.pvalue
    else:
        sign_pers = float("nan")
        sign_p = float("nan")

    ci = _confidence_interval(fwd_returns)

    result = {
        "H": horizon,
        "signal": signal_name,
        "n_raw_entries": n_raw,
        "n_spaced_entries": n_spaced,
        "n_valid_returns": len(fwd_returns),
        "mean_raw": round(mean_raw, 8),
        "mean_baseline": round(mean_baseline, 8),
        "mean_excess": round(mean_excess, 8),
        "ci_95_low": ci[0],
        "ci_95_high": ci[1],
        "cost_adjusted_sharpe": cost_result.cost_adjusted_sharpe,
        "cost_adjusted_mean": cost_result.mean_cost_adjusted_return,
        "economically_viable": cost_result.economically_viable,
        "breakeven_cost": cost_result.breakeven_cost_pct,
        "sign_persistence": round(sign_pers, 4) if not np.isnan(sign_pers) else None,
        "sign_p_value": round(sign_p, 6) if not np.isnan(sign_p) else None,
        "skipped": False,
    }

    logger.info(
        "horizon_complete",
        H=horizon,
        signal=signal_name,
        n=len(fwd_returns),
        mean_raw=round(mean_raw, 8),
        mean_excess=round(mean_excess, 8),
        sharpe=cost_result.cost_adjusted_sharpe,
        viable=cost_result.economically_viable,
        sign_pers=round(sign_pers, 4) if not np.isnan(sign_pers) else None,
        breakeven=round(cost_result.breakeven_cost_pct, 8),
    )

    return result


async def run() -> dict:
    logger.info("experiment_started", horizons=HORIZONS)

    df_raw = await _load_ticks()
    features = build_feature_dataframe(df_raw, vol_window=VOL_WINDOW)

    flow = compute_signed_flow(
        features["quantity"], features["is_buyer_maker"], window=FLOW_WINDOW
    )
    features["signed_flow"] = flow
    features["flow_zscore"] = compute_flow_zscore(flow, baseline_window=ZSCORE_BASELINE)

    features = features.dropna(subset=["rolling_vol", "flow_zscore"])
    prices = features["price"].values

    logger.info("data_ready", n_ticks=len(features))

    regime_entries = _detect_regime_entries(features)
    flow_entries = _detect_flow_entries(features)

    logger.info(
        "entries_detected",
        regime_raw=len(regime_entries),
        flow_raw=len(flow_entries),
    )

    regime_results = []
    flow_results = []

    for H in HORIZONS:
        logger.info("sweeping_horizon", H=H)

        r_result = _sweep_one_horizon(
            prices, regime_entries,
            entry_returns_h50=np.array([]),
            horizon=H,
            signal_name="regime",
        )
        regime_results.append(r_result)

        f_result = _sweep_one_horizon(
            prices, flow_entries,
            entry_returns_h50=np.array([]),
            horizon=H,
            signal_name="flow_zscore",
        )
        flow_results.append(f_result)

    meta = build_experiment_metadata(
        in_sample_rows=0,
        out_of_sample_rows=len(features),
        purge_gap=0,
        forward_window=max(HORIZONS),
        cost_params=COST_PARAMS,
    )

    return {
        "experiment": "horizon_sweep_v1",
        "metadata": meta,
        "horizons": HORIZONS,
        "regime_signal": regime_results,
        "flow_signal": flow_results,
    }
