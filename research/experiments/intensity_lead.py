import numpy as np
import pandas as pd
import structlog
from db.connection import get_pool
from research.features.tick_features import (
    build_feature_dataframe,
    compute_intensity_spike,
    compute_vol_expansion,
)
from research.stats.tests import (
    cross_correlation,
    partial_correlation,
    compute_distribution_stats,
)

logger = structlog.get_logger()

VOL_WINDOW = 50
INTENSITY_BASELINE_WINDOW = 200
FORWARD_WINDOW = 50
CCF_MAX_LAG = 50
MIN_SAMPLES = 30


async def load_ticks(limit: int = 50000, batch_size: int = 5000) -> pd.DataFrame:
    """Batched loader — reuses identical logic from vol_clustering."""
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


def _build_aligned_arrays(features: pd.DataFrame) -> pd.DataFrame:
    """
    Adds intensity_spike and vol_expansion to feature DataFrame.
    Returns only rows where all required columns are non-NaN.

    Critical: vol_expansion uses shift(-FORWARD_WINDOW) — it is
    forward-looking by construction and must NEVER be used as a
    feature. It is the prediction target only.

    Alignment contract:
    - intensity_spike[i] uses data up to and including tick i
    - vol_expansion[i] uses rolling_vol[i+FORWARD_WINDOW]
    - No row in the returned DataFrame uses future information
      as a feature input
    """
    df = features.copy()

    df["intensity_spike"] = compute_intensity_spike(
        df["trade_intensity"],
        baseline_window=INTENSITY_BASELINE_WINDOW,
    )
    df["vol_expansion"] = compute_vol_expansion(
        df["rolling_vol"],
        forward_window=FORWARD_WINDOW,
    )

    required = [
        "rolling_vol",
        "trade_intensity",
        "intensity_spike",
        "vol_expansion",
    ]
    before = len(df)
    df = df.dropna(subset=required)
    after = len(df)

    logger.info(
        "alignment_complete",
        rows_before=before,
        rows_after=after,
        dropped=before - after,
    )

    return df


def _run_ccf(df: pd.DataFrame) -> dict:
    """
    Cross-correlation of intensity_spike with vol_expansion at each lag.

    Note: vol_expansion[i] is already the forward-looking ratio
    vol[i+FORWARD_WINDOW] / vol[i]. Running CCF at additional lag k
    means we are asking: does intensity at tick i predict vol expansion
    starting FORWARD_WINDOW + k ticks later.

    For practical signal timing, we focus on whether CCF at lag=1
    (one tick ahead of the expansion window) is significant.
    """
    x = df["intensity_spike"].values
    y = df["vol_expansion"].values

    ccf = cross_correlation(x, y, max_lag=CCF_MAX_LAG)

    valid = {k: v for k, v in ccf.items() if not np.isnan(v)}
    if not valid:
        return {"peak_lag": None, "peak_corr": None, "ccf": ccf}

    peak_lag = max(valid, key=lambda k: abs(valid[k]))
    peak_corr = valid[peak_lag]

    se = 1.0 / np.sqrt(len(x))
    significant_lags = {
        k: v for k, v in valid.items() if abs(v) > 3 * se
    }

    logger.info(
        "ccf_complete",
        peak_lag=peak_lag,
        peak_corr=round(peak_corr, 6),
        significant_lags=len(significant_lags),
        bartlett_se=round(se, 6),
    )

    return {
        "peak_lag": peak_lag,
        "peak_corr": round(peak_corr, 6),
        "significant_lags": significant_lags,
        "bartlett_se": round(se, 6),
        "ccf": {k: round(v, 6) for k, v in valid.items()},
    }


def _run_partial_corr(df: pd.DataFrame) -> dict:
    """
    Partial correlation of intensity_spike with vol_expansion,
    controlling for current rolling_vol.

    If partial_corr ≈ 0 after controlling for rolling_vol,
    intensity adds no incremental predictive power.
    If partial_corr remains significant, intensity is an
    independent predictor of vol expansion.
    """
    pcorr = partial_correlation(
        df["intensity_spike"].values,
        df["vol_expansion"].values,
        df["rolling_vol"].values,
    )

    se = 1.0 / np.sqrt(len(df))
    significant = not np.isnan(pcorr) and abs(pcorr) > 3 * se

    logger.info(
        "partial_corr_complete",
        partial_corr=pcorr,
        significant=significant,
        n=len(df),
    )

    return {
        "partial_corr": pcorr,
        "significant": significant,
        "bartlett_se": round(se, 6),
    }


def _run_regime_analysis(df: pd.DataFrame) -> dict:
    """
    Tests whether intensity predicts vol expansion differently
    in high-vol vs normal-vol regimes.

    Uses expanding percentile for regime classification —
    identical to vol_clustering experiment, no lookahead.
    """
    vols = df["rolling_vol"].values
    high_mask = np.zeros(len(vols), dtype=bool)

    for i in range(49, len(vols)):
        threshold = np.percentile(vols[:i+1], 90)
        if vols[i] >= threshold:
            high_mask[i] = True

    normal_mask = ~high_mask

    results = {}
    for label, mask in [("high_vol", high_mask), ("normal_vol", normal_mask)]:
        subset = df[mask]
        if len(subset) < MIN_SAMPLES:
            results[label] = {"n": len(subset), "skipped": True}
            continue

        ccf_subset = cross_correlation(
            subset["intensity_spike"].values,
            subset["vol_expansion"].values,
            max_lag=20,
        )
        valid = {k: v for k, v in ccf_subset.items() if not np.isnan(v)}
        peak_lag = max(valid, key=lambda k: abs(valid[k])) if valid else None
        peak_corr = valid[peak_lag] if peak_lag else None

        results[label] = {
            "n": len(subset),
            "peak_lag": peak_lag,
            "peak_corr": round(peak_corr, 6) if peak_corr else None,
            "ccf_lag1": round(valid.get(1, float("nan")), 6),
        }
        logger.info("regime_ccf_complete", regime=label, **results[label])

    return results


def _run_temporal_stability(df: pd.DataFrame) -> dict:
    """
    Splits dataset in half chronologically. Runs CCF on each half.
    Consistent results across halves → signal is stable.
    Divergent results → likely data-mined or session-specific noise.

    No shuffling. Temporal order is preserved. This is the correct
    split for time-series data — random splits would introduce
    lookahead bias at the split boundary.
    """
    mid = len(df) // 2
    first_half = df.iloc[:mid]
    second_half = df.iloc[mid:]

    results = {}
    for label, subset in [("first_half", first_half), ("second_half", second_half)]:
        if len(subset) < MIN_SAMPLES:
            results[label] = {"n": len(subset), "skipped": True}
            continue

        ccf_subset = cross_correlation(
            subset["intensity_spike"].values,
            subset["vol_expansion"].values,
            max_lag=20,
        )
        valid = {k: v for k, v in ccf_subset.items() if not np.isnan(v)}
        peak_lag = max(valid, key=lambda k: abs(valid[k])) if valid else None
        peak_corr = valid[peak_lag] if peak_lag else None

        results[label] = {
            "n": len(subset),
            "peak_lag": peak_lag,
            "peak_corr": round(peak_corr, 6) if peak_corr else None,
            "ccf_lag1": round(valid.get(1, float("nan")), 6),
        }
        logger.info("stability_split_complete", split=label, **results[label])

    # Stability verdict: peaks within 5 lags and same sign
    if all("peak_lag" in results.get(h, {}) for h in ["first_half", "second_half"]):
        pl1 = results["first_half"].get("peak_lag")
        pl2 = results["second_half"].get("peak_lag")
        pc1 = results["first_half"].get("peak_corr") or 0
        pc2 = results["second_half"].get("peak_corr") or 0
        stable = (
            pl1 is not None and pl2 is not None and
            abs(pl1 - pl2) <= 5 and
            np.sign(pc1) == np.sign(pc2)
        )
        results["stable"] = stable
    else:
        results["stable"] = False

    return results


async def run() -> dict:
    """
    Full intensity lead experiment.
    Returns structured result dict — no side effects.
    """
    logger.info("experiment_started", hypothesis="intensity_lead_v1")

    df_raw = await load_ticks()
    features = build_feature_dataframe(df_raw, vol_window=VOL_WINDOW)
    df = _build_aligned_arrays(features)

    logger.info("experiment_data_ready", usable_rows=len(df))

    ccf_result = _run_ccf(df)
    partial_result = _run_partial_corr(df)
    regime_result = _run_regime_analysis(df)
    stability_result = _run_temporal_stability(df)

    # Practical interpretation
    peak_corr = ccf_result.get("peak_corr") or 0.0
    partial_corr = partial_result.get("partial_corr") or 0.0
    stable = stability_result.get("stable", False)
    n = len(df)
    se = 1.0 / np.sqrt(n)

    has_signal = abs(partial_corr) > 3 * se and stable
    interpretation = (
        "INTENSITY LEADS VOLATILITY EXPANSION — incremental signal confirmed"
        if has_signal else
        "INTENSITY DOES NOT INDEPENDENTLY LEAD VOL EXPANSION — "
        "no actionable edge beyond current vol clustering"
    )

    return {
        "hypothesis": "intensity_lead_v1",
        "n": n,
        "ccf": ccf_result,
        "partial_correlation": partial_result,
        "regime_analysis": regime_result,
        "temporal_stability": stability_result,
        "interpretation": interpretation,
    }
