import pandas as pd
import numpy as np
import structlog
from db.connection import get_pool
from research.features.tick_features import build_feature_dataframe
from research.stats.tests import ks_test, autocorrelation, compute_distribution_stats
from research.validation.hypothesis import HYPOTHESIS_REGISTRY

logger = structlog.get_logger()

VOL_WINDOW = 50
HIGH_VOL_PERCENTILE = 90


async def load_mainnet_ticks(
    limit: int = 50000,
    batch_size: int = 5000,
) -> pd.DataFrame:
    """Loads ticks in batches to avoid mobile network timeout."""
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


async def run() -> dict:
    logger.info("experiment_started", hypothesis="vol_clustering_v1")

    df = await load_mainnet_ticks()
    logger.info("data_loaded", rows=len(df))

    features = build_feature_dataframe(df, vol_window=VOL_WINDOW)
    features_clean = features.dropna(subset=["rolling_vol", "future_vol_50"])
    logger.info("features_ready", usable_rows=len(features_clean))

    vols = features_clean["rolling_vol"].values
    future_vols = features_clean["future_vol_50"].values

    # Expanding percentile — no lookahead bias
    high_mask = np.zeros(len(vols), dtype=bool)
    for i in range(49, len(vols)):
        threshold = np.percentile(vols[:i+1], HIGH_VOL_PERCENTILE)
        if vols[i] >= threshold:
            high_mask[i] = True

    normal_mask = ~high_mask

    high_future = future_vols[high_mask & ~np.isnan(future_vols)]
    normal_future = future_vols[normal_mask & ~np.isnan(future_vols)]

    logger.info(
        "regime_split",
        high_vol_n=len(high_future),
        normal_vol_n=len(normal_future),
    )

    ks_result = ks_test(
        high_future, normal_future,
        significance_level=0.01,
        label_a="high_vol", label_b="normal_vol",
    )

    acf = autocorrelation(vols, max_lag=10)

    return {
        "hypothesis": "vol_clustering_v1",
        "ks_test": ks_result,
        "high_vol_distribution": compute_distribution_stats(high_future),
        "normal_vol_distribution": compute_distribution_stats(normal_future),
        "vol_autocorrelation": acf,
        "conclusion": (
            "VOLATILITY CLUSTERING CONFIRMED"
            if ks_result.significant
            else "VOLATILITY CLUSTERING NOT DETECTED"
        ),
    }
