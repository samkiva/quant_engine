"""initial trades table

Revision ID: 3feb0c5bc201
Revises:
Create Date: 2026-05-15

"""
from typing import Sequence, Union
from alembic import op

revision: str = '3feb0c5bc201'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id              BIGSERIAL PRIMARY KEY,
            trade_id        BIGINT NOT NULL,
            symbol          TEXT NOT NULL,
            price           NUMERIC(20, 8) NOT NULL,
            quantity        NUMERIC(20, 8) NOT NULL,
            is_buyer_maker  BOOLEAN NOT NULL,
            trade_time      TIMESTAMPTZ NOT NULL,
            event_time      TIMESTAMPTZ NOT NULL,
            inserted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(symbol, trade_id)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_trades_symbol_time
            ON trades(symbol, trade_time DESC)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_trades_symbol_time")
    op.execute("DROP TABLE IF EXISTS trades")
