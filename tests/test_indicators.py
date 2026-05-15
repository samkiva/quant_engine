import pytest
import pandas as pd
import numpy as np
from research.indicators import (
    compute_vwap,
    compute_log_returns,
    compute_rolling_volatility,
    compute_momentum,
    add_all_indicators,
)


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "price": [100.0, 101.0, 100.5, 102.0, 101.5,
                  103.0, 102.5, 104.0, 103.5, 105.0],
        "quantity": [1.0, 2.0, 1.5, 3.0, 0.5,
                     2.0, 1.0, 2.5, 1.5, 2.0],
        "is_buyer_maker": [True, False] * 5,
    })


def test_vwap_is_volume_weighted(sample_df):
    vwap = compute_vwap(sample_df)
    assert len(vwap) == len(sample_df)
    assert vwap.iloc[0] == pytest.approx(100.0)


def test_vwap_between_price_extremes(sample_df):
    vwap = compute_vwap(sample_df)
    assert vwap.iloc[-1] >= sample_df["price"].min()
    assert vwap.iloc[-1] <= sample_df["price"].max()


def test_log_returns_first_value_is_nan(sample_df):
    returns = compute_log_returns(sample_df)
    assert pd.isna(returns.iloc[0])


def test_log_returns_correct_formula(sample_df):
    returns = compute_log_returns(sample_df)
    expected = np.log(101.0 / 100.0)
    assert returns.iloc[1] == pytest.approx(expected)


def test_log_returns_length_matches_input(sample_df):
    returns = compute_log_returns(sample_df)
    assert len(returns) == len(sample_df)


def test_rolling_volatility_respects_window(sample_df):
    returns = compute_log_returns(sample_df)
    vol = compute_rolling_volatility(returns, window=5)
    # log_returns[0] is NaN, so rolling(5) needs indices 1-5 to compute index 5
    # indices 0-4 should all be NaN
    for i in range(5):
        assert pd.isna(vol.iloc[i]), f"Expected NaN at index {i}"
    # index 5 is the first computable value
    assert not pd.isna(vol.iloc[5])


def test_rolling_volatility_non_negative(sample_df):
    returns = compute_log_returns(sample_df)
    vol = compute_rolling_volatility(returns, window=3)
    assert (vol.dropna() >= 0).all()


def test_momentum_window(sample_df):
    momentum = compute_momentum(sample_df, window=3)
    assert momentum.iloc[3] == pytest.approx(2.0)


def test_momentum_first_values_nan(sample_df):
    momentum = compute_momentum(sample_df, window=3)
    assert pd.isna(momentum.iloc[0])
    assert pd.isna(momentum.iloc[2])
    assert not pd.isna(momentum.iloc[3])


def test_add_all_indicators_returns_new_dataframe(sample_df):
    result = add_all_indicators(sample_df)
    assert "vwap" not in sample_df.columns
    assert "vwap" in result.columns


def test_add_all_indicators_expected_columns(sample_df):
    result = add_all_indicators(sample_df)
    expected = {"vwap", "log_return", "volatility_50", "momentum_20", "above_vwap"}
    assert expected.issubset(result.columns)


def test_above_vwap_is_binary(sample_df):
    result = add_all_indicators(sample_df)
    assert result["above_vwap"].isin([0, 1]).all()
