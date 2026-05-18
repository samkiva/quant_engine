"""add portfolio state log table

Revision ID: deb8cb457114
Revises: e2f0990daa2f
Create Date: 2026-05-16

"""
from typing import Sequence, Union
from alembic import op

revision: str = 'deb8cb457114'
down_revision: Union[str, None] = 'e2f0990daa2f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_state_log (
            id              BIGSERIAL PRIMARY KEY,
            session_id      BIGINT REFERENCES stream_sessions(id),
            recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            cash            NUMERIC(20, 8) NOT NULL,
            position_side   TEXT,
            position_price  NUMERIC(20, 8),
            position_qty    NUMERIC(20, 8),
            portfolio_value NUMERIC(20, 8) NOT NULL,
            total_pnl       NUMERIC(20, 8) NOT NULL DEFAULT 0,
            cause           TEXT NOT NULL
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_portfolio_state_session
            ON portfolio_state_log(session_id, recorded_at DESC)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_portfolio_state_session")
    op.execute("DROP TABLE IF EXISTS portfolio_state_log")
