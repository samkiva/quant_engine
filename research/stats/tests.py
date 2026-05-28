from dataclasses import dataclass
from typing import Optional
import numpy as np
import structlog

logger = structlog.get_logger()


@dataclass
class StatTestResult:
    test_name: str
    statistic: float
    p_value: float
    significant: bool       # p < significance_level
    significance_level: float
    sample_size_a: int
    sample_size_b: int
    interpretation: str


def ks_test(
    sample_a: np.ndarray,
    sample_b: np.ndarray,
    significance_level: float = 0.01,
    label_a: str = "group_a",
    label_b: str = "group_b",
) -> StatTestResult:
    """
    Two-sample Kolmogorov-Smirnov test.
    Tests whether two samples come from the same distribution.

    p < significance_level → distributions are different → effect exists.
    p >= significance_level → cannot distinguish distributions.

    Using p < 0.01 (not 0.05) to reduce false positives across
    multiple hypothesis tests.
    """
    from scipy import stats

    sample_a = np.array(sample_a, dtype=float)
    sample_b = np.array(sample_b, dtype=float)
    sample_a = sample_a[~np.isnan(sample_a)]
    sample_b = sample_b[~np.isnan(sample_b)]

    statistic, p_value = stats.ks_2samp(sample_a, sample_b)
    significant = p_value < significance_level

    interpretation = (
        f"{label_a} and {label_b} have DIFFERENT distributions "
        f"(p={p_value:.4f} < {significance_level})"
        if significant else
        f"Cannot distinguish {label_a} from {label_b} "
        f"(p={p_value:.4f} >= {significance_level})"
    )

    result = StatTestResult(
        test_name="KS_two_sample",
        statistic=round(statistic, 6),
        p_value=round(p_value, 6),
        significant=significant,
        significance_level=significance_level,
        sample_size_a=len(sample_a),
        sample_size_b=len(sample_b),
        interpretation=interpretation,
    )

    logger.info(
        "ks_test_complete",
        statistic=result.statistic,
        p_value=result.p_value,
        significant=result.significant,
        n_a=result.sample_size_a,
        n_b=result.sample_size_b,
    )

    return result


def autocorrelation(series: np.ndarray, max_lag: int = 20) -> dict[int, float]:
    """
    Computes autocorrelation at each lag from 1 to max_lag.
    If autocorrelation at lag 1 is significantly positive,
    the series has momentum/persistence.
    If negative, it mean-reverts.
    """
    series = np.array(series, dtype=float)
    series = series[~np.isnan(series)]
    n = len(series)
    mean = series.mean()
    var = series.var()

    if var == 0:
        return {lag: 0.0 for lag in range(1, max_lag + 1)}

    acf = {}
    for lag in range(1, max_lag + 1):
        cov = np.mean((series[:n-lag] - mean) * (series[lag:] - mean))
        acf[lag] = round(cov / var, 4)

    return acf


def compute_distribution_stats(sample: np.ndarray) -> dict:
    """Summary statistics for a distribution."""
    sample = np.array(sample, dtype=float)
    sample = sample[~np.isnan(sample)]
    return {
        "mean": round(float(np.mean(sample)), 8),
        "std": round(float(np.std(sample)), 8),
        "median": round(float(np.median(sample)), 8),
        "p10": round(float(np.percentile(sample, 10)), 8),
        "p25": round(float(np.percentile(sample, 25)), 8),
        "p75": round(float(np.percentile(sample, 75)), 8),
        "p90": round(float(np.percentile(sample, 90)), 8),
        "n": len(sample),
    }


def cross_correlation(
    x: np.ndarray,
    y: np.ndarray,
    max_lag: int = 50,
) -> dict[int, float]:
    """
    Pearson correlation between x[t] and y[t+k] for k in 1..max_lag.

    Positive lag k means x leads y — x at time t correlates with
    y k steps in the future. This is the direction of interest for
    testing whether intensity predicts future volatility expansion.

    Assumptions:
    - Both series are approximately stationary over the window used
    - Linear relationship between x and y (Pearson is not rank-based)
    - NaNs removed pairwise per lag — sample size varies across lags

    Returns dict of {lag: correlation}. NaN returned where
    insufficient non-NaN pairs exist (< 30).
    """
    x = np.array(x, dtype=float)
    y = np.array(y, dtype=float)
    results = {}

    for lag in range(1, max_lag + 1):
        x_lead = x[:-lag]
        y_future = y[lag:]
        mask = ~(np.isnan(x_lead) | np.isnan(y_future))
        x_clean = x_lead[mask]
        y_clean = y_future[mask]

        if len(x_clean) < 30:
            results[lag] = float("nan")
            continue

        correlation = float(np.corrcoef(x_clean, y_clean)[0, 1])
        results[lag] = round(correlation, 6)

    return results


def partial_correlation(
    x: np.ndarray,
    y: np.ndarray,
    z: np.ndarray,
) -> float:
    """
    Partial correlation of x and y after removing the linear effect of z.

    Tests whether x predicts y beyond what z already explains.
    In this research context: does intensity_spike predict
    vol_expansion beyond what current rolling_vol already predicts?

    Method: residuals from OLS regression of x on z and y on z.
    Pearson correlation of the two residual series.

    Assumptions:
    - Linear relationships between variables
    - z is a confounder, not a mediator
    - Sufficient sample size after NaN removal (minimum 30)

    Returns NaN if computation fails or sample is insufficient.
    """
    x = np.array(x, dtype=float)
    y = np.array(y, dtype=float)
    z = np.array(z, dtype=float)

    mask = ~(np.isnan(x) | np.isnan(y) | np.isnan(z))
    x, y, z = x[mask], y[mask], z[mask]

    if len(x) < 30:
        return float("nan")

    def residuals(a: np.ndarray, b: np.ndarray) -> np.ndarray:
        """OLS residuals of regressing a on b (with intercept)."""
        b_design = np.column_stack([np.ones(len(b)), b])
        coeffs, _, _, _ = np.linalg.lstsq(b_design, a, rcond=None)
        return a - b_design @ coeffs

    x_resid = residuals(x, z)
    y_resid = residuals(y, z)

    if x_resid.std() == 0 or y_resid.std() == 0:
        return float("nan")

    result = float(np.corrcoef(x_resid, y_resid)[0, 1])
    return round(result, 6)


def one_sample_ttest(
    sample: np.ndarray,
    null_mean: float = 0.0,
    alternative: str = "less",
    significance_level: float = 0.01,
) -> StatTestResult:
    """
    One-sample t-test against a null mean.

    For H2 (mean reversion): alternative='less' tests whether
    mean forward return is significantly below null_mean (0.0).
    A significant result supports mean reversion.

    alternative: 'less', 'greater', or 'two-sided'
    Must be pre-registered before seeing data.

    Assumptions:
    - Observations approximately independent (enforced by
      non-overlapping entry window logic upstream)
    - Approximately normal distribution of means (CLT applies
      for n > 30)
    """
    from scipy import stats

    sample = np.array(sample, dtype=float)
    sample = sample[~np.isnan(sample)]

    statistic, p_two_sided = stats.ttest_1samp(sample, null_mean)

    if alternative == "less":
        p_value = p_two_sided / 2 if statistic < 0 else 1 - p_two_sided / 2
    elif alternative == "greater":
        p_value = p_two_sided / 2 if statistic > 0 else 1 - p_two_sided / 2
    else:
        p_value = p_two_sided

    significant = p_value < significance_level
    mean_val = float(np.mean(sample))

    interpretation = (
        f"Mean={mean_val:.6f} is significantly {alternative} than "
        f"{null_mean} (p={p_value:.4f})"
        if significant else
        f"Cannot reject null: mean={mean_val:.6f} not significantly "
        f"{alternative} than {null_mean} (p={p_value:.4f})"
    )

    return StatTestResult(
        test_name=f"t_test_{alternative}",
        statistic=round(statistic, 6),
        p_value=round(p_value, 6),
        significant=significant,
        significance_level=significance_level,
        sample_size_a=len(sample),
        sample_size_b=0,
        interpretation=interpretation,
    )


def sign_persistence_test(
    entry_returns: np.ndarray,
    forward_returns: np.ndarray,
    significance_level: float = 0.01,
    alternative: str = "two-sided",
) -> dict:
    """
    Tests whether forward return sign matches entry return sign.

    sign_persistence = P(sign(forward) == sign(entry))

    H2 (mean reversion) predicts sign_persistence < 0.5:
    forward return direction opposes the entry direction.

    Uses binomial test against null p=0.5.
    NaN pairs removed before testing.
    """
    from scipy import stats

    entry = np.array(entry_returns, dtype=float)
    forward = np.array(forward_returns, dtype=float)

    mask = ~(np.isnan(entry) | np.isnan(forward))
    entry, forward = entry[mask], forward[mask]

    if len(entry) < 10:
        return {"persistence": float("nan"), "significant": False, "n": len(entry)}

    # Exclude zero returns from sign comparison
    nonzero = (entry != 0) & (forward != 0)
    entry, forward = entry[nonzero], forward[nonzero]

    same_sign = np.sign(entry) == np.sign(forward)
    n_same = int(same_sign.sum())
    n_total = len(same_sign)
    persistence = n_same / n_total

    # One-sided binomial test: alternative='less' for mean reversion
    result = stats.binomtest(n_same, n_total, p=0.5, alternative=alternative)
    p_value = result.pvalue
    significant = p_value < significance_level

    return {
        "persistence": round(persistence, 4),
        "n_same_sign": n_same,
        "n_total": n_total,
        "p_value": round(p_value, 6),
        "significant": significant,
        "interpretation": (
            f"Sign persistence={persistence:.3f} — "
            f"{'mean reversion supported' if persistence < 0.5 else 'momentum supported'} "
            f"(p={p_value:.4f}, {'significant' if significant else 'not significant'})"
        ),
    }


def two_sample_ttest(
    sample_a: np.ndarray,
    sample_b: np.ndarray,
    alternative: str = "greater",
    significance_level: float = 0.01,
    label_a: str = "sample_a",
    label_b: str = "sample_b",
) -> StatTestResult:
    """
    Welch two-sample t-test. Does not assume equal variance.

    Tests whether mean(sample_a) differs from mean(sample_b)
    in the specified direction.

    For H1: alternative='greater' tests whether entry-conditioned
    forward returns exceed unconditional baseline returns.

    This is the correct test for regime conditioning —
    it answers whether regime entry changes expected return
    relative to what the market normally produces.
    """
    from scipy import stats

    a = np.array(sample_a, dtype=float)
    b = np.array(sample_b, dtype=float)
    a = a[~np.isnan(a)]
    b = b[~np.isnan(b)]

    statistic, p_two_sided = stats.ttest_ind(a, b, equal_var=False)

    if alternative == "greater":
        p_value = p_two_sided / 2 if statistic > 0 else 1 - p_two_sided / 2
    elif alternative == "less":
        p_value = p_two_sided / 2 if statistic < 0 else 1 - p_two_sided / 2
    else:
        p_value = p_two_sided

    significant = p_value < significance_level
    mean_a = float(np.mean(a))
    mean_b = float(np.mean(b))
    mean_diff = mean_a - mean_b

    interpretation = (
        f"{label_a} mean={mean_a:.6f} is significantly {alternative} than "
        f"{label_b} mean={mean_b:.6f}, diff={mean_diff:.6f} (p={p_value:.4f})"
        if significant else
        f"Cannot distinguish {label_a} mean={mean_a:.6f} from "
        f"{label_b} mean={mean_b:.6f}, diff={mean_diff:.6f} (p={p_value:.4f})"
    )

    return StatTestResult(
        test_name=f"welch_ttest_{alternative}",
        statistic=round(statistic, 6),
        p_value=round(p_value, 6),
        significant=significant,
        significance_level=significance_level,
        sample_size_a=len(a),
        sample_size_b=len(b),
        interpretation=interpretation,
    )
