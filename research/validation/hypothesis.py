from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class StrategyHypothesis:
    name: str
    version: str
    hypothesis: str
    null_hypothesis: str
    mechanism: str
    failure_conditions: str
    min_sharpe: float = 0.5
    min_win_rate: float = 0.45
    max_drawdown_pct: float = 5.0
    created_at: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    dataset_fingerprint: Optional[str] = None
    notes: str = ""

    def summary(self) -> str:
        return (
            f"Hypothesis: {self.name} v{self.version}\n"
            f"Claim: {self.hypothesis}\n"
            f"Mechanism: {self.mechanism}\n"
            f"Falsifiable if: {self.failure_conditions}\n"
            f"Min Sharpe: {self.min_sharpe} | "
            f"Min Win Rate: {self.min_win_rate:.0%} | "
            f"Max Drawdown: {self.max_drawdown_pct}%"
        )


HYPOTHESIS_REGISTRY: dict[str, StrategyHypothesis] = {

    "vwap_cross_v1": StrategyHypothesis(
        name="VWAP Crossover",
        version="1.0",
        hypothesis=(
            "Price crossing above/below cumulative VWAP predicts "
            "short-term continuation in the crossover direction."
        ),
        null_hypothesis=(
            "VWAP crossover signals have no predictive power beyond "
            "random chance on tick-level data."
        ),
        mechanism=(
            "Institutional traders use VWAP as execution benchmark. "
            "Price returning to VWAP indicates mean reversion pressure; "
            "breaking through indicates trend confirmation."
        ),
        failure_conditions=(
            "Win rate <= 50% on out-of-sample data across multiple "
            "walk-forward windows. Sharpe < 0.5 consistently."
        ),
        notes="Phase 4 backtest: 7.7% win rate. REJECTED on testnet.",
    ),

    "vol_clustering_v1": StrategyHypothesis(
        name="Volatility Clustering Regime",
        version="1.0",
        hypothesis=(
            "Periods where 50-tick rolling volatility exceeds its "
            "90th percentile predict continued elevated volatility "
            "over the subsequent 50 ticks."
        ),
        null_hypothesis=(
            "Current volatility level has no predictive relationship "
            "with near-term future volatility beyond random variation."
        ),
        mechanism=(
            "GARCH effects and volatility clustering are among the most "
            "robust empirical findings in financial markets (Engle 2003). "
            "Volatility exhibits persistence."
        ),
        failure_conditions=(
            "KS test p-value >= 0.05 comparing next-50-tick volatility "
            "distributions in high-vol vs normal-vol regimes."
        ),
        min_sharpe=0.3,
        notes="First mainnet hypothesis. Pure statistical test.",
    ),
}
