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
