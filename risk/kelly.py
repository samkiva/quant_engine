from dataclasses import dataclass
from typing import Optional
import structlog

logger = structlog.get_logger()

MAX_KELLY_FRACTION = 0.02   # Never risk more than 2% per trade
HALF_KELLY = 0.5            # Always use half-Kelly to reduce variance
MIN_TRADES_REQUIRED = 30    # Minimum closed trades before Kelly is valid


@dataclass
class KellyResult:
    raw_kelly: float
    half_kelly: float
    capped_kelly: float
    win_rate: float
    win_loss_ratio: float
    is_valid: bool
    reason: str


def compute_kelly(
    wins: list[float],
    losses: list[float],
) -> KellyResult:
    """
    Computes fractional Kelly position size from historical trade outcomes.

    Formula: f* = (p * b - (1-p)) / b
    Where:
        p = win rate
        b = average win / average loss ratio
        f* = fraction of capital to risk

    Returns half-Kelly capped at MAX_KELLY_FRACTION.
    Negative Kelly = no edge = do not trade.
    """
    total = len(wins) + len(losses)

    if total < MIN_TRADES_REQUIRED:
        return KellyResult(
            raw_kelly=0.0, half_kelly=0.0, capped_kelly=0.0,
            win_rate=0.0, win_loss_ratio=0.0,
            is_valid=False,
            reason=f"insufficient_trades: {total} < {MIN_TRADES_REQUIRED}",
        )

    if not losses:
        return KellyResult(
            raw_kelly=MAX_KELLY_FRACTION, half_kelly=MAX_KELLY_FRACTION,
            capped_kelly=MAX_KELLY_FRACTION,
            win_rate=1.0, win_loss_ratio=float("inf"),
            is_valid=True, reason="no_losses",
        )

    win_rate = len(wins) / total
    avg_win = sum(wins) / len(wins)
    avg_loss = abs(sum(losses) / len(losses))

    if avg_loss == 0:
        return KellyResult(
            raw_kelly=0.0, half_kelly=0.0, capped_kelly=0.0,
            win_rate=win_rate, win_loss_ratio=0.0,
            is_valid=False, reason="zero_avg_loss",
        )

    win_loss_ratio = avg_win / avg_loss
    raw_kelly = (win_rate * win_loss_ratio - (1 - win_rate)) / win_loss_ratio
    half_kelly = raw_kelly * HALF_KELLY
    capped_kelly = max(0.0, min(half_kelly, MAX_KELLY_FRACTION))

    is_valid = raw_kelly > 0
    reason = "positive_edge" if is_valid else f"negative_edge: raw_kelly={raw_kelly:.4f}"

    logger.debug(
        "kelly_computed",
        win_rate=round(win_rate, 3),
        win_loss_ratio=round(win_loss_ratio, 3),
        raw_kelly=round(raw_kelly, 4),
        half_kelly=round(half_kelly, 4),
        capped_kelly=round(capped_kelly, 4),
        is_valid=is_valid,
    )

    return KellyResult(
        raw_kelly=raw_kelly,
        half_kelly=half_kelly,
        capped_kelly=capped_kelly,
        win_rate=win_rate,
        win_loss_ratio=win_loss_ratio,
        is_valid=is_valid,
        reason=reason,
    )
