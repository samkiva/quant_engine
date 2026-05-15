import pandas as pd
import numpy as np


def compute_vwap(df: pd.DataFrame) -> pd.Series:
    """
    Cumulative VWAP from the start of the dataset.
    Weights each price observation by its traded volume.
    """
    notional = df["price"] * df["quantity"]  # value of each trade
    cumulative_notional = notional.cumsum()
    cumulative_volume = df["quantity"].cumsum()
    return cumulative_notional / cumulative_volume


def compute_log_returns(df: pd.DataFrame) -> pd.Series:
    """
    Log returns: log(price_t / price_{t-1})
    More mathematically correct than percentage returns for financial data.
    First value is NaN by definition.
    """
    return np.log(df["price"] / df["price"].shift(1))


def compute_rolling_volatility(
    log_returns: pd.Series,
    window: int = 50,
) -> pd.Series:
    """
    Rolling standard deviation of log returns.
    Annualising is skipped here — we work in trade-time, not calendar time.
    """
    return log_returns.rolling(window=window).std()


def compute_momentum(df: pd.DataFrame, window: int = 20) -> pd.Series:
    """
    Price momentum: current price minus price N trades ago.
    Positive = upward pressure. Negative = downward pressure.
    """
    return df["price"] - df["price"].shift(window)


def add_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes all indicators and returns enriched DataFrame.
    Does not mutate the input.
    """
    df = df.copy()
    df["vwap"] = compute_vwap(df)
    df["log_return"] = compute_log_returns(df)
    df["volatility_50"] = compute_rolling_volatility(df["log_return"], window=50)
    df["momentum_20"] = compute_momentum(df, window=20)
    df["above_vwap"] = (df["price"] > df["vwap"]).astype(int)
    return df
