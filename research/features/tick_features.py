import math
import pandas as pd
import numpy as np
from dataclasses import dataclass
import structlog

logger = structlog.get_logger()


def compute_log_returns(prices: pd.Series) -> pd.Series:
    """Log returns: log(p_t / p_{t-1}). First value is NaN."""
    return np.log(prices / prices.shift(1))


def compute_rolling_volatility(
    log_returns: pd.Series,
    window: int = 50,
) -> pd.Series:
    """Rolling std of log returns. First (window-1) values are NaN."""
    return log_returns.rolling(window=window).std()


def compute_trade_intensity(
    trade_times: pd.Series,
    window: int = 50,
) -> pd.Series:
    """
    Trades per second over a rolling window of N trades.
    High intensity = many trades arriving rapidly.
    Low intensity = sparse arrivals.
    """
    intervals = trade_times.diff().dt.total_seconds()
    rolling_avg_interval = intervals.rolling(window=window).mean()
    return 1.0 / rolling_avg_interval.replace(0, np.nan)


def compute_inter_arrival_times(trade_times: pd.Series) -> pd.Series:
    """Seconds between consecutive trades. Reveals tick clustering."""
    return trade_times.diff().dt.total_seconds()


def compute_buyer_imbalance(
    is_buyer_maker: pd.Series,
    window: int = 50,
) -> pd.Series:
    """
    Rolling fraction of trades where buyer is maker.
    > 0.5 → sellers are aggressing (bearish pressure)
    < 0.5 → buyers are aggressing (bullish pressure)

    Note: is_buyer_maker=True means the buyer's order was resting
    (maker) and the seller hit it — seller is the aggressor.
    """
    return is_buyer_maker.astype(float).rolling(window=window).mean()


def compute_volatility_regime(
    rolling_vol: pd.Series,
    high_pct: float = 90.0,
    low_pct: float = 25.0,
) -> pd.Series:
    """
    Classifies each tick into volatility regime.
    Uses expanding percentiles to avoid lookahead bias.

    Returns: 1 = high vol, 0 = normal, -1 = low vol
    """
    regimes = pd.Series(0, index=rolling_vol.index, dtype=int)
    vol_clean = rolling_vol.dropna()

    for i in range(len(vol_clean)):
        historical = vol_clean.iloc[:i+1]
        if len(historical) < 50:
            continue
        high_threshold = np.percentile(historical, high_pct)
        low_threshold = np.percentile(historical, low_pct)
        current = historical.iloc[-1]
        if current >= high_threshold:
            regimes.iloc[rolling_vol.index.get_loc(historical.index[-1])] = 1
        elif current <= low_threshold:
            regimes.iloc[rolling_vol.index.get_loc(historical.index[-1])] = -1

    return regimes


def build_feature_dataframe(df: pd.DataFrame, vol_window: int = 50) -> pd.DataFrame:
    """
    Computes all features from a raw tick DataFrame.
    Input df must have columns: trade_time, price, quantity, is_buyer_maker
    trade_time must be the index (datetime, timezone-aware).

    Returns enriched DataFrame. Does NOT mutate input.
    """
    result = df.copy()

    result["log_return"] = compute_log_returns(result["price"])
    result["rolling_vol"] = compute_rolling_volatility(
        result["log_return"], window=vol_window
    )
    result["trade_intensity"] = compute_trade_intensity(
        result.index.to_series(), window=vol_window
    )
    result["inter_arrival_secs"] = compute_inter_arrival_times(
        result.index.to_series()
    )
    result["buyer_imbalance"] = compute_buyer_imbalance(
        result["is_buyer_maker"], window=vol_window
    )

    # Forward-looking volatility — used as prediction TARGET only
    # Never used as a feature (would cause lookahead bias)
    result["future_vol_50"] = result["rolling_vol"].shift(-vol_window)

    logger.info(
        "features_computed",
        rows=len(result),
        vol_window=vol_window,
        nan_pct=round(result["rolling_vol"].isna().mean() * 100, 1),
    )

    return result


def compute_intensity_spike(
    intensity: pd.Series,
    baseline_window: int = 200,
) -> pd.Series:
    """
    Normalizes trade intensity by its rolling median baseline.

    intensity_spike = intensity / rolling_median(intensity, baseline_window)

    Values > 1 indicate above-baseline activity.
    Values < 1 indicate below-baseline activity.

    Rolling median (not mean) is used because intensity distributions
    are right-skewed — median is a more robust baseline estimator.

    First (baseline_window - 1) values are NaN by design.
    No lookahead: baseline uses only past observations.
    """
    baseline = intensity.rolling(window=baseline_window, min_periods=baseline_window).median()
    spike = intensity / baseline
    spike = spike.replace([float("inf"), float("-inf")], float("nan"))
    return spike


def compute_vol_expansion(
    rolling_vol: pd.Series,
    forward_window: int = 50,
) -> pd.Series:
    """
    Ratio of future volatility to current volatility.

    vol_expansion = rolling_vol.shift(-forward_window) / rolling_vol

    Values > 1: volatility expanded in next N ticks.
    Values < 1: volatility contracted.
    Values = 1: no change.

    Last (forward_window) values are NaN — no future data available.
    This is intentionally forward-looking and must ONLY be used
    as a prediction target, never as a feature input.
    Division by zero replaced with NaN explicitly.
    """
    future_vol = rolling_vol.shift(-forward_window)
    expansion = future_vol / rolling_vol.replace(0, float("nan"))
    expansion = expansion.replace([float("inf"), float("-inf")], float("nan"))
    return expansion
