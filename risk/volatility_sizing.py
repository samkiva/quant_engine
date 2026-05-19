import math
import structlog

logger = structlog.get_logger()

TARGET_DAILY_VOL = 0.01      # Target 1% daily portfolio volatility
MIN_VOL_SCALAR = 0.1         # Never size below 10% of base
MAX_VOL_SCALAR = 2.0         # Never size above 200% of base


def compute_vol_scalar(
    recent_log_returns: list[float],
    target_daily_vol: float = TARGET_DAILY_VOL,
) -> float:
    """
    Computes a position size scalar based on current vs target volatility.

    When volatility is high → scalar < 1 → smaller position.
    When volatility is low  → scalar > 1 → larger position (capped).

    This keeps portfolio realized volatility roughly constant
    regardless of market regime — a standard technique in
    systematic funds called volatility targeting.
    """
    if len(recent_log_returns) < 10:
        return 1.0  # Insufficient data — use base size

    n = len(recent_log_returns)
    mean = sum(recent_log_returns) / n
    variance = sum((r - mean) ** 2 for r in recent_log_returns) / (n - 1)
    current_vol = math.sqrt(variance)

    if current_vol == 0:
        return 1.0

    scalar = target_daily_vol / current_vol
    scalar = max(MIN_VOL_SCALAR, min(scalar, MAX_VOL_SCALAR))

    logger.debug(
        "vol_scalar_computed",
        current_vol=round(current_vol, 6),
        target_vol=target_daily_vol,
        scalar=round(scalar, 3),
    )

    return scalar
