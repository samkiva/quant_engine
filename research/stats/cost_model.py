"""
Canonical cost-adjusted evaluation layer.

Every experiment that claims a tradeable signal must pass through
this module. Raw return statistics are descriptive only.
The primary acceptance criterion is cost_adjusted_sharpe.

Cost model parameters are explicit and fixed per experiment run.
No parameter search. No optimization. Auditable by metadata block.
"""

from dataclasses import dataclass, field, asdict
from typing import Optional
import math
import numpy as np
import structlog

logger = structlog.get_logger()


@dataclass(frozen=True)
class CostParams:
    """
    Explicit cost assumptions for one experiment run.
    Frozen after construction — never modified during evaluation.

    entry_cost_pct: taker fee on entry, as decimal (0.001 = 0.10%)
    exit_cost_pct:  taker fee on exit, as decimal
    slippage_pct:   estimated market impact per side, as decimal
                    Set to 0.0 for tick-level research where slippage
                    is unquantifiable. Flag it explicitly, never omit it.

    BTCUSDT Binance taker defaults: entry=0.001, exit=0.001
    Round-trip total: 0.002 (0.20%)
    """
    entry_cost_pct: float = 0.001
    exit_cost_pct: float = 0.001
    slippage_pct: float = 0.0

    @property
    def round_trip_cost(self) -> float:
        return self.entry_cost_pct + self.exit_cost_pct + 2 * self.slippage_pct

    def to_dict(self) -> dict:
        return {
            "entry_cost_pct": self.entry_cost_pct,
            "exit_cost_pct": self.exit_cost_pct,
            "slippage_pct": self.slippage_pct,
            "round_trip_cost": self.round_trip_cost,
        }


@dataclass
class CostAdjustedResult:
    """
    Primary output of cost model evaluation.
    All fields are post-cost unless explicitly labeled _raw.
    """
    n_signals: int
    mean_raw_return: float
    mean_cost_adjusted_return: float
    total_cost_adjusted_pnl: float
    cost_adjusted_sharpe: float
    win_rate_raw: float
    win_rate_cost_adjusted: float
    breakeven_cost_pct: float        # Round-trip cost at which Sharpe = 0
    cost_drag_pct: float             # Fraction of gross PnL consumed by costs
    economically_viable: bool        # mean_cost_adjusted_return > 0
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "n_signals": self.n_signals,
            "mean_raw_return": round(self.mean_raw_return, 8),
            "mean_cost_adjusted_return": round(self.mean_cost_adjusted_return, 8),
            "total_cost_adjusted_pnl": round(self.total_cost_adjusted_pnl, 8),
            "cost_adjusted_sharpe": round(self.cost_adjusted_sharpe, 4),
            "win_rate_raw": round(self.win_rate_raw, 4),
            "win_rate_cost_adjusted": round(self.win_rate_cost_adjusted, 4),
            "breakeven_cost_pct": round(self.breakeven_cost_pct, 6),
            "cost_drag_pct": round(self.cost_drag_pct, 4),
            "economically_viable": self.economically_viable,
            "metadata": self.metadata,
        }


def evaluate(
    forward_returns: np.ndarray,
    cost_params: CostParams,
    metadata: Optional[dict] = None,
    annualization_factor: float = 1.0,
) -> CostAdjustedResult:
    """
    Applies cost model to a sequence of forward returns.

    Each element of forward_returns represents one complete
    round-trip trade: entry + hold for forward_window + exit.
    Cost is applied once per signal (round-trip).

    forward_returns: log returns from signal generation to exit.
                     Must be aligned to signal timestamps only —
                     not all ticks. Caller is responsible for
                     passing only entry-point returns.

    annualization_factor: multiply Sharpe by this for annualized
                          comparison. Default 1.0 (no annualization)
                          because tick-level horizons are ambiguous.

    NaN values are removed before evaluation. Their count is
    reported in metadata for auditability.
    """
    returns = np.array(forward_returns, dtype=float)
    n_total = len(returns)
    nan_count = int(np.isnan(returns).sum())
    returns = returns[~np.isnan(returns)]
    n_clean = len(returns)

    if n_clean == 0:
        logger.warning("cost_model_empty_input", n_total=n_total)
        return CostAdjustedResult(
            n_signals=0,
            mean_raw_return=0.0,
            mean_cost_adjusted_return=0.0,
            total_cost_adjusted_pnl=0.0,
            cost_adjusted_sharpe=0.0,
            win_rate_raw=0.0,
            win_rate_cost_adjusted=0.0,
            breakeven_cost_pct=0.0,
            cost_drag_pct=0.0,
            economically_viable=False,
            metadata={"error": "empty_input", "n_total": n_total},
        )

    cost_per_trade = cost_params.round_trip_cost
    adjusted_returns = returns - cost_per_trade

    mean_raw = float(np.mean(returns))
    mean_adjusted = float(np.mean(adjusted_returns))
    total_adjusted_pnl = float(np.sum(adjusted_returns))

    std_adjusted = float(np.std(adjusted_returns))
    min_std = abs(mean_adjusted) * 0.01 if mean_adjusted != 0 else 1e-10
    if std_adjusted > min_std:
        sharpe = (mean_adjusted / std_adjusted) * math.sqrt(annualization_factor)
    else:
        sharpe = 0.0  # Insufficient variance — not meaningful

    win_rate_raw = float(np.mean(returns > 0))
    win_rate_adjusted = float(np.mean(adjusted_returns > 0))

    # Breakeven: what round-trip cost would make mean_adjusted = 0
    breakeven = mean_raw if mean_raw > 0 else 0.0

    # Cost drag: fraction of gross PnL consumed by costs
    gross_pnl = float(np.sum(returns))
    total_costs = cost_per_trade * n_clean
    if abs(gross_pnl) > 0:
        cost_drag = total_costs / abs(gross_pnl)
    else:
        cost_drag = float("inf")

    result_metadata = {
        "n_total_input": n_total,
        "n_nan_removed": nan_count,
        "n_clean": n_clean,
        "cost_per_trade": round(cost_per_trade, 6),
        "cost_params": cost_params.to_dict(),
    }
    if metadata:
        result_metadata.update(metadata)

    result = CostAdjustedResult(
        n_signals=n_clean,
        mean_raw_return=mean_raw,
        mean_cost_adjusted_return=mean_adjusted,
        total_cost_adjusted_pnl=total_adjusted_pnl,
        cost_adjusted_sharpe=round(sharpe, 4),
        win_rate_raw=win_rate_raw,
        win_rate_cost_adjusted=win_rate_adjusted,
        breakeven_cost_pct=breakeven,
        cost_drag_pct=min(cost_drag, 999.0),
        economically_viable=mean_adjusted > 0,
        metadata=result_metadata,
    )

    logger.info(
        "cost_model_evaluated",
        n_signals=n_clean,
        mean_raw=round(mean_raw, 8),
        mean_adjusted=round(mean_adjusted, 8),
        sharpe=round(sharpe, 4),
        viable=result.economically_viable,
        breakeven=round(breakeven, 6),
        cost_drag=round(min(cost_drag, 999.0), 2),
    )

    return result


def build_experiment_metadata(
    in_sample_rows: int,
    out_of_sample_rows: int,
    purge_gap: int,
    forward_window: int,
    cost_params: CostParams,
) -> dict:
    """
    Mandatory metadata block for every experiment result.
    Returned as top-level key in all experiment output dicts.
    Prevents ambiguity about validation conditions.
    """
    return {
        "in_sample_rows": in_sample_rows,
        "out_of_sample_rows": out_of_sample_rows,
        "purge_gap": purge_gap,
        "forward_window": forward_window,
        "cost_model": cost_params.to_dict(),
    }
