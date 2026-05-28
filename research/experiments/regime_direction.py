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
    two_sample_ttest,
    sign_persistence_test,
    compute_distribution_stats,
)

logger = structlog.get_logger()

# Pre-registered. Do not modify after seeing results.
HYPOTHESIS = "H1_momentum_continuation"
FORWARD_WINDOW = 50
MIN_SPACING = 50
ROUND_TRIP_COST = 0.002


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
    Feature/target separation contract:
    FEATURES:  rolling_vol, entry_return, high_vol, regime_entry
    TARGETS:   forward_return, norm_forward_return  (never used as features)
    """
    features = build_feature_dataframe(df_raw, vol_window=50)

    # Regime — expanding percentile, no lookahead
    vols = features["rolling_vol"].values
    high_vol = pd.Series(0, index=features.index, dtype=int)
    for i in range(49, len(vols)):
        if np.isnan(vols[i]):
            continue
        hist = vols[:i+1]
        hist = hist[~np.isnan(hist)]
        threshold = np.percentile(hist, 90)
        if vols[i] >= threshold:
            high_vol.iloc[i] = 1
    features["high_vol"] = high_vol

    features["regime_entry"] = compute_regime_entries(
        features["high_vol"], min_spacing=MIN_SPACING
    )
    features["forward_return"] = compute_forward_return(
        features["price"], forward_window=FORWARD_WINDOW
    )
    features["entry_return"] = compute_entry_return(
        features["price"], lookback_window=50
    )
    features["norm_forward_return"] = (
        features["forward_return"] / features["rolling_vol"]
    ).replace([float("inf"), float("-inf")], float("nan"))

    logger.info(
        "research_frame_built",
        rows=len(features),
        entries=int(features["regime_entry"].sum()),
        non_entries=int((~features["regime_entry"]).sum()),
    )
    return features


def _run_tests(
    entries: pd.DataFrame,
    baseline: pd.DataFrame,
    label: str,
) -> dict:
    """
    Runs all pre-registered H1 tests.
    baseline = non-entry ticks — the true unconditional comparison.
    """
    fwd_entry = entries["forward_return"].dropna().values
    fwd_baseline = baseline["forward_return"].dropna().values

    if len(fwd_entry) < 10 or len(fwd_baseline) < 10:
        return {"label": label, "n": len(fwd_entry), "skipped": True}

    # Two-sample Welch t-test: do entry returns exceed baseline?
    ttest = two_sample_ttest(
        fwd_entry, fwd_baseline,
        alternative="greater",
        label_a="entry", label_b="baseline",
    )

    # KS test: real unconditional baseline (not synthetic Gaussian)
    ks = ks_test(
        fwd_entry, fwd_baseline,
        significance_level=0.01,
        label_a="entry_forward", label_b="unconditional_baseline",
    )

    # Sign persistence: H1 predicts > 0.5
    mask = ~(entries["entry_return"].isna() | entries["forward_return"].isna())
    sign_test = sign_persistence_test(
        entries["entry_return"][mask].values,
        entries["forward_return"][mask].values,
        alternative="greater",
    )

    dist = compute_distribution_stats(fwd_entry)
    mean_entry = float(np.nanmean(fwd_entry))
    mean_base = float(np.nanmean(fwd_baseline))
    mean_excess = mean_entry - mean_base

    logger.info(
        "tests_complete",
        label=label,
        n_entry=len(fwd_entry),
        n_baseline=len(fwd_baseline),
        mean_entry=round(mean_entry, 6),
        mean_baseline=round(mean_base, 6),
        mean_excess=round(mean_excess, 6),
        ttest_sig=ttest.significant,
        ks_sig=ks.significant,
        sign_persistence=sign_test.get("persistence"),
    )

    return {
        "label": label,
        "n_entry": len(fwd_entry),
        "n_baseline": len(fwd_baseline),
        "mean_entry": round(mean_entry, 6),
        "mean_baseline": round(mean_base, 6),
        "mean_excess": round(mean_excess, 6),
        "economically_significant": abs(mean_excess) > ROUND_TRIP_COST,
        "ttest": ttest,
        "ks_test": ks,
        "sign_persistence": sign_test,
        "distribution": dist,
    }


async def run() -> dict:
    logger.info("experiment_started", hypothesis=HYPOTHESIS, forward_window=FORWARD_WINDOW)

    df_raw = await _load_ticks()
    features = _build_research_frame(df_raw)

    entry_mask = features["regime_entry"]
    entries = features[entry_mask].copy()
    baseline = features[~entry_mask].copy()

    logger.info("split_ready", entries=len(entries), baseline=len(baseline))

    if len(entries) < 10:
        return {"hypothesis": HYPOTHESIS, "error": f"Insufficient entries: {len(entries)}"}

    full = _run_tests(entries, baseline, label="full_sample")

    # Temporal stability — chronological split of entries only
    mid = len(entries) // 2
    first = _run_tests(entries.iloc[:mid], baseline, label="first_half")
    second = _run_tests(entries.iloc[mid:], baseline, label="second_half")

    # Directional conditioning
    up = entries[entries["entry_return"] > 0]
    dn = entries[entries["entry_return"] < 0]
    up_result = _run_tests(up, baseline, "up_entry") if len(up) >= 10 else None
    dn_result = _run_tests(dn, baseline, "down_entry") if len(dn) >= 10 else None

    # Stability verdict
    stable = (
        not first.get("skipped") and not second.get("skipped") and
        np.sign(first["mean_excess"]) == np.sign(second["mean_excess"]) and
        first["ttest"].significant == second["ttest"].significant
    )

    criteria = {
        "ttest_significant":        full["ttest"].significant,
        "ks_significant":           full["ks_test"].significant,
        "sign_persistence_gt_half": (
            full["sign_persistence"].get("persistence", 0) > 0.5 and
            full["sign_persistence"].get("significant", False)
        ),
        "temporally_stable":        stable,
        "economically_significant": full["economically_significant"],
    }

    passed = sum(criteria.values())
    conclusion = (
        "H1 SUPPORTED — momentum confirmed with economic edge"
        if passed >= 4 else
        f"H1 PARTIAL — {passed}/5 criteria met" if passed >= 2 else
        f"H1 NOT SUPPORTED — {passed}/5 criteria met"
    )

    return {
        "hypothesis": HYPOTHESIS,
        "forward_window": FORWARD_WINDOW,
        "n_entries": len(entries),
        "n_baseline": len(baseline),
        "full_sample": full,
        "first_half": first,
        "second_half": second,
        "up_entry": up_result,
        "down_entry": dn_result,
        "criteria": criteria,
        "stable": stable,
        "conclusion": conclusion,
    }
