"""add signal log table

Revision ID: e2f0990daa2f
Revises: d05ec9f0f8a8
Create Date: 2026-05-16

"""
from typing import Sequence, Union
from alembic import op

revision: str = 'e2f0990daa2f'
down_revision: Union[str, None] = 'd05ec9f0f8a8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS signal_log (
            id              BIGSERIAL PRIMARY KEY,
            strategy_name   TEXT NOT NULL,
            symbol          TEXT NOT NULL,
            signal          TEXT NOT NULL,
            tick_price      NUMERIC(20, 8) NOT NULL,
            tick_timestamp  TIMESTAMPTZ NOT NULL,
            generated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            latency_ms      NUMERIC(10, 3),
            reason          TEXT,
            risk_blocked    BOOLEAN NOT NULL DEFAULT FALSE,
            block_reason    TEXT,
            session_id      BIGINT REFERENCES stream_sessions(id),
            post_reconnect  BOOLEAN NOT NULL DEFAULT FALSE
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_signal_log_symbol_time
            ON signal_log(symbol, generated_at DESC)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_signal_log_strategy
            ON signal_log(strategy_name, generated_at DESC)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_signal_log_strategy")
    op.execute("DROP INDEX IF EXISTS idx_signal_log_symbol_time")
    op.execute("DROP TABLE IF EXISTS signal_log")
