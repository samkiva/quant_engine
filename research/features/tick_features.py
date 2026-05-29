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


def compute_forward_return(
    prices: pd.Series,
    forward_window: int = 50,
) -> pd.Series:
    """
    Log return from current tick to N ticks ahead.

    forward_return_t = log(price_{t+N} / price_t)

    PREDICTION TARGET ONLY. Never use as a feature.
    Last (forward_window) values are NaN — no future data.
    """
    return np.log(prices.shift(-forward_window) / prices)


def compute_entry_return(
    prices: pd.Series,
    lookback_window: int = 50,
) -> pd.Series:
    """
    Log return from N ticks ago to current tick.

    entry_return_t = log(price_t / price_{t-N})

    Uses only past prices — safe as feature input.
    First (lookback_window) values are NaN.
    """
    return np.log(prices / prices.shift(lookback_window))


def compute_regime_entries(
    high_vol: pd.Series,
    min_spacing: int = 50,
) -> pd.Series:
    """
    Detects transitions into high-vol regime with enforced
    minimum spacing to prevent overlapping forward windows.

    Entry at tick t requires:
    1. high_vol_t == 1
    2. high_vol_{t-1} == 0  (transition, not continuation)
    3. At least min_spacing ticks since last accepted entry

    min_spacing must equal forward_window to guarantee
    non-overlapping prediction targets. Caller is responsible
    for passing the correct value.

    Returns boolean Series — True at accepted entry ticks only.
    """
    raw_entries = (high_vol == 1) & (high_vol.shift(1) == 0)
    entries = pd.Series(False, index=high_vol.index)
    last_entry_pos = -min_spacing

    for pos, (idx, is_entry) in enumerate(raw_entries.items()):
        if is_entry and (pos - last_entry_pos) >= min_spacing:
            entries[idx] = True
            last_entry_pos = pos

    return entries


def compute_signed_flow(
    quantity: pd.Series,
    is_buyer_maker: pd.Series,
    window: int = 50,
) -> pd.Series:
    """
    Rolling signed volume imbalance over the last N trades.

    signed_volume_t = quantity_t * side_t
    where:
        side_t = +1 if buyer is aggressor (is_buyer_maker == False)
        side_t = -1 if seller is aggressor (is_buyer_maker == True)

    Note on Binance convention:
        is_buyer_maker == True  → buyer's order was resting (maker)
                                → seller hit the book → seller aggresses
                                → sign = -1 (selling pressure)
        is_buyer_maker == False → seller's order was resting (maker)
                                → buyer lifted the offer → buyer aggresses
                                → sign = +1 (buying pressure)

    signed_flow_t = sum(signed_volume) over last N trades

    Positive values: net buying pressure over window.
    Negative values: net selling pressure over window.

    Normalized by rolling total volume to make comparable
    across windows with different absolute volume levels.
    Returns values in [-1, +1].

    First (window - 1) values are NaN by design.
    No lookahead: uses only past and current trades.
    """
    side = is_buyer_maker.apply(lambda x: -1.0 if x else 1.0)
    signed_vol = quantity * side

    rolling_signed = signed_vol.rolling(window=window).sum()
    rolling_total = quantity.rolling(window=window).sum()

    normalized = rolling_signed / rolling_total.replace(0, float("nan"))
    normalized = normalized.replace(
        [float("inf"), float("-inf")], float("nan")
    )
    return normalized


def compute_trade_count_imbalance(
    is_buyer_maker: pd.Series,
    window: int = 50,
) -> pd.Series:
    """
    Rolling trade count imbalance — not volume weighted.

    imbalance = (n_buyer_aggressed - n_seller_aggressed)
                / (n_buyer_aggressed + n_seller_aggressed)

    Returns values in [-1, +1].
    +1: all trades in window are buyer-aggressed
    -1: all trades in window are seller-aggressed
     0: perfectly balanced

    Differs from compute_signed_flow in that large trades do not
    dominate — each trade counts equally regardless of size.
    This reduces saturation from volume-concentrated bursts.

    Uses same Binance side convention as compute_signed_flow:
        is_buyer_maker == False → buyer aggresses → +1
        is_buyer_maker == True  → seller aggresses → -1
    """
    side = is_buyer_maker.apply(lambda x: -1.0 if x else 1.0)
    n_buy = (side == 1).astype(float).rolling(window=window).sum()
    n_sell = (side == -1).astype(float).rolling(window=window).sum()
    total = n_buy + n_sell
    imbalance = (n_buy - n_sell) / total.replace(0, float("nan"))
    return imbalance.replace([float("inf"), float("-inf")], float("nan"))


def compute_flow_zscore(
    signed_flow: pd.Series,
    baseline_window: int = 200,
    min_std: float = 1e-6,
) -> pd.Series:
    """
    Z-score of signed_flow relative to its rolling baseline.

    zscore_t = (flow_t - mean(flow_{t-N:t})) / std(flow_{t-N:t})

    Solves the saturation problem: even when absolute flow is near ±1,
    the z-score captures deviations from recent equilibrium.
    A flow of +0.70 during a period where mean is +0.90 produces
    a negative z-score — buying pressure is weakening relative
    to recent norm, even though absolute flow is still positive.

    baseline_window: lookback for mean and std estimation.
                     Must be longer than the flow computation window
                     to capture meaningful equilibrium.
                     Recommended: 4x the flow window (200 for flow=50).

    min_std: floor for std to prevent division near zero.
             Applied when market is in extremely low-activity period.

    First (baseline_window - 1) values are NaN.
    No lookahead: mean and std use only past observations.
    """
    rolling_mean = signed_flow.rolling(window=baseline_window, min_periods=baseline_window).mean()
    rolling_std = signed_flow.rolling(window=baseline_window, min_periods=baseline_window).std()
    rolling_std = rolling_std.clip(lower=min_std)
    zscore = (signed_flow - rolling_mean) / rolling_std
    return zscore.replace([float("inf"), float("-inf")], float("nan"))


def compute_flow_acceleration(
    signed_flow: pd.Series,
    lag: int = 10,
) -> pd.Series:
    """
    First difference of signed_flow at a fixed lag.

    acceleration_t = flow_t - flow_{t-lag}

    Measures the rate of change in order flow direction.
    Positive: buying pressure is increasing (flow moving toward +1)
    Negative: selling pressure is increasing (flow moving toward -1)
    Near zero: flow direction is stable

    Dynamic range is good by construction: even in a saturated
    period (flow near ±1), transitions between same-side and
    contra-side bursts produce non-zero acceleration.

    lag: number of ticks between current and reference observation.
         Recommended: same as the flow computation window / 5.
         At flow_window=50, lag=10 measures change over last 10 trades.

    First (lag) values are NaN.
    No lookahead: uses only past values via shift(lag).
    """
    return signed_flow - signed_flow.shift(lag)
