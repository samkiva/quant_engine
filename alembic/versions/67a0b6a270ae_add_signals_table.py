"""add signals table

Revision ID: 67a0b6a270ae
Revises: 3feb0c5bc201
Create Date: 2026-05-15

"""
from typing import Sequence, Union
from alembic import op

revision: str = '67a0b6a270ae'
down_revision: Union[str, None] = '3feb0c5bc201'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id              BIGSERIAL PRIMARY KEY,
            symbol          TEXT NOT NULL,
            computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            window_start    TIMESTAMPTZ NOT NULL,
            window_end      TIMESTAMPTZ NOT NULL,
            trade_count     INTEGER NOT NULL,
            vwap            NUMERIC(20, 8),
            volatility_50   NUMERIC(20, 10),
            momentum_20     NUMERIC(20, 8),
            above_vwap      BOOLEAN
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_signals_symbol_time
            ON signals(symbol, computed_at DESC)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_signals_symbol_time")
    op.execute("DROP TABLE IF EXISTS signals")
