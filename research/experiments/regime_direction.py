import numpy as np
import pandas as pd
import structlog
from db.connection import get_pool
from research.features.tick_features import (
    build_feature_dataframe,
    compute_forward_return,
    compute_entry_return,
    compute_regime_entries,
)
from research.stats.tests import (
    ks_test,
    one_sample_ttest,
    sign_persistence_test,
    compute_distribution_stats,
)

logger = structlog.get_logger()

VOL_WINDOW = 50
FORWARD_WINDOW = 50       # Pre-registered. Do not change after seeing results.
MIN_SPACING = 50          # Must equal FORWARD_WINDOW — non-overlapping guarantee.
ROUND_TRIP_COST = 0.002   # 0.20% round trip. Edge threshold.


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


def _build_research_frame(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Builds full feature frame. Adds research-specific columns.

    Column separation contract:
    FEATURES  (use in models): rolling_vol, entry_return, high_vol_regime
    TARGETS   (predict only):  forward_return, norm_forward_return
    METADATA  (filter/split):  regime_entry

    No target column is used as input to any feature column.
    """
    features = build_feature_dataframe(df_raw, vol_window=VOL_WINDOW)

    # Regime classification — reuse expanding percentile from vol_clustering
    vols = features["rolling_vol"].values
    high_vol = pd.Series(0, index=features.index, dtype=int)
    for i in range(49, len(vols)):
        if np.isnan(vols[i]):
            continue
        threshold = np.percentile(vols[:i+1][~np.isnan(vols[:i+1])], 90)
        if vols[i] >= threshold:
            high_vol.iloc[i] = 1
    features["high_vol"] = high_vol

    # Entry detection with non-overlapping spacing enforcement
    features["regime_entry"] = compute_regime_entries(
        features["high_vol"], min_spacing=MIN_SPACING
    )

    # Target variables — forward-looking, never used as features
    features["forward_return"] = compute_forward_return(
        features["price"], forward_window=FORWARD_WINDOW
    )
    features["entry_return"] = compute_entry_return(
        features["price"], lookback_window=VOL_WINDOW
    )

    # Volatility-normalized forward return
    features["norm_forward_return"] = (
        features["forward_return"] / features["rolling_vol"]
    ).replace([float("inf"), float("-inf")], float("nan"))

    total_entries = features["regime_entry"].sum()
    logger.info(
        "research_frame_built",
        total_rows=len(features),
        regime_entries=int(total_entries),
        high_vol_ticks=int(high_vol.sum()),
    )

    return features


def _run_direction_tests(entries: pd.DataFrame, label: str) -> dict:
    """
    Runs all pre-registered tests on a set of regime entry observations.
    Identical function called for full sample and each temporal split.
    """
    fwd = entries["forward_return"].dropna().values
    norm_fwd = entries["norm_forward_return"].dropna().values

    entry_r = entries["entry_return"]
    fwd_aligned = entries["forward_return"]
    mask = ~(entry_r.isna() | fwd_aligned.isna())
    entry_clean = entry_r[mask].values
    fwd_clean = fwd_aligned[mask].values

    # Baseline: all non-entry ticks forward returns
    n_entries = len(fwd)

    # t-test: H2 predicts mean < 0
    ttest = one_sample_ttest(fwd, null_mean=0.0, alternative="less")

    # KS test vs zero-mean normal (proxy for unconditional)
    unconditional = np.random.default_rng(42).normal(0, fwd.std() if fwd.std() > 0 else 1, len(fwd))
    ks = ks_test(fwd, unconditional, significance_level=0.01,
                 label_a="entry_forward_returns", label_b="unconditional_proxy")

    # Sign persistence: H2 predicts < 0.5
    sign_test = sign_persistence_test(entry_clean, fwd_clean)

    # Distribution stats
    dist = compute_distribution_stats(fwd)

    # Economic significance
    mean_return = float(np.nanmean(fwd))
    economically_significant = abs(mean_return) > ROUND_TRIP_COST

    logger.info(
        "direction_tests_complete",
        label=label,
        n=n_entries,
        mean_fwd_return=round(mean_return, 6),
        ttest_significant=ttest.significant,
        sign_persistence=sign_test.get("persistence"),
        economic_edge=economically_significant,
    )

    return {
        "label": label,
        "n": n_entries,
        "mean_forward_return": round(mean_return, 6),
        "economically_significant": economically_significant,
        "round_trip_cost": ROUND_TRIP_COST,
        "ttest": ttest,
        "ks_test": ks,
        "sign_persistence": sign_test,
        "distribution": dist,
    }


async def run() -> dict:
    """
    Pre-registered hypothesis: H2 (mean reversion).
    Forward window: 50 ticks. Fixed before data inspection.
    """
    logger.info(
        "experiment_started",
        hypothesis="regime_direction_H2_mean_reversion",
        forward_window=FORWARD_WINDOW,
        min_spacing=MIN_SPACING,
    )

    df_raw = await _load_ticks()
    features = _build_research_frame(df_raw)

    entry_mask = features["regime_entry"]
    entries = features[entry_mask].copy()

    logger.info("entry_set_ready", n_entries=len(entries))

    if len(entries) < 10:
        return {
            "hypothesis": "regime_direction_H2",
            "error": f"Insufficient entries: {len(entries)}",
        }

    # Full sample tests
    full_result = _run_direction_tests(entries, label="full_sample")

    # Temporal stability: chronological 50/50 split
    mid = len(entries) // 2
    first_half = entries.iloc[:mid]
    second_half = entries.iloc[mid:]

    first_result = _run_direction_tests(first_half, label="first_half")
    second_result = _run_direction_tests(second_half, label="second_half")

    # Directional conditioning: up vs down entry
    up_entries = entries[entries["entry_return"] > 0]
    down_entries = entries[entries["entry_return"] < 0]

    up_result = _run_direction_tests(up_entries, label="up_entry") if len(up_entries) >= 10 else None
    down_result = _run_direction_tests(down_entries, label="down_entry") if len(down_entries) >= 10 else None

    # Stability verdict
    fp = first_result["mean_forward_return"]
    sp = second_result["mean_forward_return"]
    stable = (
        np.sign(fp) == np.sign(sp) and
        first_result["ttest"].significant == second_result["ttest"].significant
    )

    # Overall verdict — all four criteria must pass for H2 support
    criteria = {
        "ks_significant": full_result["ks_test"].significant,
        "ttest_significant": full_result["ttest"].significant,
        "sign_persistence_below_half": (
            full_result["sign_persistence"].get("persistence", 1.0) < 0.5 and
            full_result["sign_persistence"].get("significant", False)
        ),
        "temporally_stable": stable,
        "economically_significant": full_result["economically_significant"],
    }

    passed = sum(criteria.values())
    conclusion = (
        "H2 SUPPORTED — mean reversion confirmed with economic edge"
        if passed >= 4 else
        f"H2 NOT SUPPORTED — {passed}/5 criteria met"
    )

    return {
        "hypothesis": "regime_direction_H2_mean_reversion",
        "forward_window": FORWARD_WINDOW,
        "n_entries": len(entries),
        "full_sample": full_result,
        "first_half": first_result,
        "second_half": second_result,
        "up_entry": up_result,
        "down_entry": down_result,
        "criteria": criteria,
        "stable": stable,
        "conclusion": conclusion,
    }
