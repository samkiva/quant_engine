"""add mainnet trades table

Revision ID: 5cb8eed2e342
Revises: deb8cb457114
Create Date: 2026-05-18

"""
from typing import Sequence, Union
from alembic import op

revision: str = '5cb8eed2e342'
down_revision: Union[str, None] = 'deb8cb457114'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS mainnet_trades (
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
        CREATE INDEX IF NOT EXISTS idx_mainnet_trades_symbol_time
            ON mainnet_trades(symbol, trade_time DESC)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_mainnet_trades_symbol_time")
    op.execute("DROP TABLE IF EXISTS mainnet_trades")
