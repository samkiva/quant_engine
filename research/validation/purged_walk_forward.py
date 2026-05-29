"""
Purged walk-forward validation.

Standard walk-forward splits data into train/test windows.
Purged walk-forward adds an explicit gap between train end
and test start equal to the forward horizon of the experiment.

Why purging is mandatory:
If forward_window=50 and train ends at tick T, then the label
for tick T uses price at tick T+50. If test starts at T+1,
the test window's early ticks share forward-label data with
the train window's final ticks. This is label contamination.

The purge gap eliminates this: test starts at T + forward_window + 1.
No train label uses any price in the test window.

Threshold freezing:
Regime thresholds (e.g. vol percentiles) are fit on train data only.
The fit threshold is passed as a frozen scalar to test evaluation.
Recalibrating on test data is lookahead bias.

Fixed tick counts:
Windows are sized by tick count, not calendar time or equal percentages.
Tick-count windows are regime-neutral. Calendar windows are not —
a volatile session produces more ticks per hour than a quiet one,
making percentage-based splits regime-dependent.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import numpy as np
import structlog
from db.connection import get_pool

logger = structlog.get_logger()


@dataclass(frozen=True)
class PurgedWindow:
    """
    A single purged walk-forward window.

    train: [train_start_idx, train_end_idx)
    purge: [train_end_idx, test_start_idx)   — excluded entirely
    test:  [test_start_idx, test_end_idx)

    purge_size always equals forward_window.
    No tick in purge zone appears in either train or test.

    frozen_threshold: scalar fit on train data.
    Applied without modification to test data.
    None until set by caller after train-phase fitting.
    """
    window_id: int
    train_start_idx: int
    train_end_idx: int
    test_start_idx: int
    test_end_idx: int
    purge_size: int
    train_size: int
    test_size: int
    frozen_threshold: Optional[float] = None

    def with_threshold(self, threshold: float) -> "PurgedWindow":
        """Returns new window with frozen threshold set."""
        return PurgedWindow(
            window_id=self.window_id,
            train_start_idx=self.train_start_idx,
            train_end_idx=self.train_end_idx,
            test_start_idx=self.test_start_idx,
            test_end_idx=self.test_end_idx,
            purge_size=self.purge_size,
            train_size=self.train_size,
            test_size=self.test_size,
            frozen_threshold=threshold,
        )


def generate_purged_windows(
    n_ticks: int,
    train_size: int,
    test_size: int,
    forward_window: int,
    min_train_size: Optional[int] = None,
) -> list[PurgedWindow]:
    """
    Generates purged walk-forward windows over n_ticks total ticks.

    purge_size = forward_window (mandatory, not configurable).
    Windows are non-overlapping on the test dimension.
    Advancing cursor moves by (test_size + purge_size) per step.

    Parameters:
        n_ticks:        total number of ticks in the dataset
        train_size:     number of ticks in each training window
        test_size:      number of ticks in each test window
        forward_window: prediction horizon — sets purge gap size
        min_train_size: minimum acceptable train window
                        defaults to train_size (no partial windows)

    Returns list of PurgedWindow — empty if insufficient data.
    """
    purge_size = forward_window
    min_train = min_train_size or train_size

    if n_ticks < train_size + purge_size + test_size:
        logger.warning(
            "insufficient_ticks_for_purged_windows",
            n_ticks=n_ticks,
            required=train_size + purge_size + test_size,
        )
        return []

    windows = []
    window_id = 0
    train_start = 0

    while True:
        train_end = train_start + train_size
        test_start = train_end + purge_size
        test_end = test_start + test_size

        if test_end > n_ticks:
            break

        actual_train = train_end - train_start
        if actual_train < min_train:
            train_start += test_size
            continue

        windows.append(PurgedWindow(
            window_id=window_id,
            train_start_idx=train_start,
            train_end_idx=train_end,
            test_start_idx=test_start,
            test_end_idx=test_end,
            purge_size=purge_size,
            train_size=actual_train,
            test_size=test_size,
        ))
        window_id += 1

        # Advance: train expands to include previous test window
        # This is walk-forward (expanding train), not rolling
        train_start = train_start
        train_size = train_size + test_size

    logger.info(
        "purged_windows_generated",
        count=len(windows),
        purge_size=purge_size,
        final_train_size=windows[-1].train_size if windows else 0,
    )

    return windows


def fit_vol_threshold(
    rolling_vol: np.ndarray,
    percentile: float = 90.0,
) -> float:
    """
    Fits a volatility threshold on train data.
    Returns a scalar — the percentile value of rolling_vol.

    This scalar is frozen and applied to test data unchanged.
    Call this on train slice only. Never on test slice.
    """
    clean = rolling_vol[~np.isnan(rolling_vol)]
    if len(clean) == 0:
        return float("nan")
    threshold = float(np.percentile(clean, percentile))
    logger.debug("vol_threshold_fit", percentile=percentile, threshold=round(threshold, 8))
    return threshold


def apply_frozen_threshold(
    rolling_vol: np.ndarray,
    threshold: float,
) -> np.ndarray:
    """
    Applies a frozen threshold to a vol series.
    Returns boolean array: True where vol >= threshold.

    Threshold must come from fit_vol_threshold on train data.
    Never refit on test data.
    """
    if np.isnan(threshold):
        return np.zeros(len(rolling_vol), dtype=bool)
    return rolling_vol >= threshold


def run_purged_evaluation(
    feature_df,
    windows: list[PurgedWindow],
    vol_col: str = "rolling_vol",
    forward_return_col: str = "forward_return",
    vol_percentile: float = 90.0,
    min_entries_per_window: int = 5,
) -> list[dict]:
    """
    Runs purged walk-forward evaluation across all windows.

    For each window:
    1. Fit threshold on train slice only
    2. Apply frozen threshold to test slice
    3. Collect forward returns at regime entry ticks in test slice
    4. Return raw forward returns — caller applies cost model

    Returns list of per-window result dicts.
    Caller passes results to cost_model.evaluate() for final verdict.
    """
    results = []

    for w in windows:
        train_slice = feature_df.iloc[w.train_start_idx:w.train_end_idx]
        test_slice = feature_df.iloc[w.test_start_idx:w.test_end_idx]

        # Fit threshold on train only
        train_vol = train_slice[vol_col].values
        threshold = fit_vol_threshold(train_vol, percentile=vol_percentile)

        # Freeze threshold — store in window for auditability
        frozen_window = w.with_threshold(threshold)

        # Apply frozen threshold to test data
        test_vol = test_slice[vol_col].values
        high_vol_mask = apply_frozen_threshold(test_vol, threshold)

        # Detect regime entries in test window
        entry_mask = np.zeros(len(high_vol_mask), dtype=bool)
        for i in range(1, len(high_vol_mask)):
            if high_vol_mask[i] and not high_vol_mask[i - 1]:
                entry_mask[i] = True

        # Collect forward returns at entry ticks
        fwd_returns = test_slice[forward_return_col].values
        entry_returns = fwd_returns[entry_mask]
        entry_returns = entry_returns[~np.isnan(entry_returns)]

        n_entries = len(entry_returns)
        mean_return = float(np.mean(entry_returns)) if n_entries > 0 else float("nan")

        result = {
            "window_id": w.window_id,
            "train_size": w.train_size,
            "test_size": w.test_size,
            "purge_size": w.purge_size,
            "frozen_threshold": round(threshold, 8),
            "n_entries": n_entries,
            "mean_raw_return": round(mean_return, 8) if not np.isnan(mean_return) else None,
            "forward_returns": entry_returns,
            "skipped": n_entries < min_entries_per_window,
        }

        logger.debug(
            "purged_window_evaluated",
            window_id=w.window_id,
            n_entries=n_entries,
            mean_raw=round(mean_return, 6) if not np.isnan(mean_return) else None,
            threshold=round(threshold, 8),
        )

        results.append(result)

    valid = [r for r in results if not r["skipped"]]
    logger.info(
        "purged_evaluation_complete",
        total_windows=len(results),
        valid_windows=len(valid),
        skipped=len(results) - len(valid),
    )

    return results
