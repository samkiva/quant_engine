"""add stream sessions table

Revision ID: d05ec9f0f8a8
Revises: 67a0b6a270ae
Create Date: 2026-05-16

"""
from typing import Sequence, Union
from alembic import op

revision: str = 'd05ec9f0f8a8'
down_revision: Union[str, None] = '67a0b6a270ae'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS stream_sessions (
            id              BIGSERIAL PRIMARY KEY,
            symbol          TEXT NOT NULL,
            connected_at    TIMESTAMPTZ NOT NULL,
            disconnected_at TIMESTAMPTZ,
            disconnect_reason TEXT,
            trades_received INTEGER NOT NULL DEFAULT 0,
            is_clean_close  BOOLEAN NOT NULL DEFAULT FALSE
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_sessions_symbol_time
            ON stream_sessions(symbol, connected_at DESC)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_sessions_symbol_time")
    op.execute("DROP TABLE IF EXISTS stream_sessions")
