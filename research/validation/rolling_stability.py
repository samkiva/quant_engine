from typing import Optional

"""
Rolling stability analysis for research signals.

A signal that passes a single temporal split may still be unstable —
present in one session, absent in another, or trend-dependent.

This module tests whether a signal metric (sign_persistence, KS statistic,
cost-adjusted return) remains above a minimum threshold across multiple
fixed-size rolling windows throughout the dataset.

Stability score = fraction of windows where metric >= threshold.
A signal with stability_score >= 0.60 is present in most conditions.
A signal with stability_score < 0.40 is session-specific noise.

Windows are fixed tick counts (not calendar time or percentages).
Fixed tick counts are regime-neutral — see purged_walk_forward.py.
"""

from dataclasses import dataclass
from typing import Callable
import numpy as np
import structlog

logger = structlog.get_logger()


@dataclass(frozen=True)
class StabilityResult:
    metric_name: str
    window_size: int
    n_windows: int
    n_windows_above_threshold: int
    stability_score: float          # fraction above threshold
    threshold: float
    metric_values: tuple            # per-window metric values
    mean_metric: float
    std_metric: float
    is_stable: bool                 # stability_score >= min_stable_score

    def summary(self) -> str:
        return (
            f"{self.metric_name}: stability={self.stability_score:.2f} "
            f"({self.n_windows_above_threshold}/{self.n_windows} windows >= {self.threshold}) "
            f"mean={self.mean_metric:.4f} std={self.std_metric:.4f} "
            f"{'STABLE' if self.is_stable else 'UNSTABLE'}"
        )


def rolling_sign_persistence(
    entry_returns: np.ndarray,
    forward_returns: np.ndarray,
    window_size: int = 200,
    min_threshold: float = 0.55,
    min_stable_score: float = 0.60,
    step_size: Optional[int] = None,
) -> StabilityResult:
    """
    Computes sign_persistence in fixed-size rolling windows.

    entry_returns and forward_returns must be pre-aligned:
    entry_returns[i] and forward_returns[i] correspond to the
    same signal event. Caller is responsible for alignment.

    window_size: number of signal events per window (not ticks).
                 Using signal events (not ticks) ensures each window
                 contains the same number of observations.

    step_size: advance between windows. Defaults to window_size // 2
               (50% overlap). Overlapping windows increase smoothness
               of the stability curve but are not independent.
               For independence, set step_size = window_size.
    """
    e = np.array(entry_returns, dtype=float)
    f = np.array(forward_returns, dtype=float)

    mask = ~(np.isnan(e) | np.isnan(f)) & (e != 0) & (f != 0)
    e, f = e[mask], f[mask]

    n = len(e)
    if n < window_size:
        logger.warning(
            "insufficient_signals_for_rolling_stability",
            n=n, window_size=window_size,
        )
        return StabilityResult(
            metric_name="sign_persistence",
            window_size=window_size,
            n_windows=0,
            n_windows_above_threshold=0,
            stability_score=0.0,
            threshold=min_threshold,
            metric_values=(),
            mean_metric=float("nan"),
            std_metric=float("nan"),
            is_stable=False,
        )

    step = step_size or max(1, window_size // 2)
    values = []
    start = 0

    while start + window_size <= n:
        e_window = e[start:start + window_size]
        f_window = f[start:start + window_size]
        persistence = float(np.mean(np.sign(e_window) == np.sign(f_window)))
        values.append(persistence)
        start += step

    values = np.array(values)
    n_above = int(np.sum(values >= min_threshold))
    score = n_above / len(values)

    result = StabilityResult(
        metric_name="sign_persistence",
        window_size=window_size,
        n_windows=len(values),
        n_windows_above_threshold=n_above,
        stability_score=round(score, 4),
        threshold=min_threshold,
        metric_values=tuple(round(v, 4) for v in values),
        mean_metric=round(float(np.mean(values)), 4),
        std_metric=round(float(np.std(values)), 4),
        is_stable=score >= min_stable_score,
    )

    logger.info(
        "rolling_sign_persistence_complete",
        n_windows=len(values),
        stability_score=round(score, 4),
        mean=round(float(np.mean(values)), 4),
        is_stable=result.is_stable,
    )

    return result


def rolling_cost_adjusted_return(
    forward_returns: np.ndarray,
    cost_per_trade: float,
    window_size: int = 200,
    min_threshold: float = 0.0,
    min_stable_score: float = 0.60,
    step_size: Optional[int] = None,
) -> StabilityResult:
    """
    Computes mean cost-adjusted return in fixed-size rolling windows.

    min_threshold=0.0 means the window must show positive post-cost
    return on average to count as passing.

    This is the primary stability test for tradeable signals.
    A signal that is statistically real but shows negative cost-adjusted
    returns in most windows is not tradeable regardless of overall mean.
    """
    returns = np.array(forward_returns, dtype=float)
    returns = returns[~np.isnan(returns)]
    n = len(returns)

    if n < window_size:
        logger.warning(
            "insufficient_returns_for_rolling_stability",
            n=n, window_size=window_size,
        )
        return StabilityResult(
            metric_name="cost_adjusted_return",
            window_size=window_size,
            n_windows=0,
            n_windows_above_threshold=0,
            stability_score=0.0,
            threshold=min_threshold,
            metric_values=(),
            mean_metric=float("nan"),
            std_metric=float("nan"),
            is_stable=False,
        )

    step = step_size or max(1, window_size // 2)
    values = []
    start = 0

    while start + window_size <= n:
        window = returns[start:start + window_size]
        adjusted = float(np.mean(window) - cost_per_trade)
        values.append(adjusted)
        start += step

    values = np.array(values)
    n_above = int(np.sum(values >= min_threshold))
    score = n_above / len(values)

    result = StabilityResult(
        metric_name="cost_adjusted_return",
        window_size=window_size,
        n_windows=len(values),
        n_windows_above_threshold=n_above,
        stability_score=round(score, 4),
        threshold=min_threshold,
        metric_values=tuple(round(v, 8) for v in values),
        mean_metric=round(float(np.mean(values)), 8),
        std_metric=round(float(np.std(values)), 8),
        is_stable=score >= min_stable_score,
    )

    logger.info(
        "rolling_cost_adjusted_return_complete",
        n_windows=len(values),
        stability_score=round(score, 4),
        mean_adjusted=round(float(np.mean(values)), 8),
        is_stable=result.is_stable,
    )

    return result


def compute_stability_score(
    metric_values: np.ndarray,
    threshold: float,
    min_stable_score: float = 0.60,
) -> float:
    """
    Generic stability scorer.
    Returns fraction of values >= threshold.
    Used when caller computes metric values externally.
    """
    values = np.array(metric_values, dtype=float)
    values = values[~np.isnan(values)]
    if len(values) == 0:
        return 0.0
    return float(np.mean(values >= threshold))
