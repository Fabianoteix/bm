"""create transactions and outbox

Revision ID: 0001
Revises:
Create Date: 2026-09-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

DATETIME = sa.DateTime().with_variant(mysql.DATETIME(fsp=6), "mysql")
BIGINT_PK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
TABLE_OPTS = {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"}


def upgrade() -> None:
    op.create_table(
        "transactions",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("customer_id", sa.String(64), nullable=False),
        sa.Column("value", sa.Numeric(14, 2), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("last_error", sa.String(500), nullable=True),
        sa.Column("idempotency_key", sa.String(128), nullable=True),
        sa.Column("created_at", DATETIME, nullable=False),
        sa.Column("updated_at", DATETIME, nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_transactions"),
        sa.UniqueConstraint("idempotency_key", name="uq_transactions_idempotency_key"),
        **TABLE_OPTS,
    )
    op.create_index("ix_transactions_customer_id", "transactions", ["customer_id"])
    op.create_index("ix_transactions_status_updated_at", "transactions", ["status", "updated_at"])

    op.create_table(
        "outbox",
        sa.Column("id", BIGINT_PK, autoincrement=True, nullable=False),
        sa.Column("event_id", sa.String(36), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("aggregate_id", sa.String(36), nullable=False),
        sa.Column("topic", sa.String(128), nullable=False),
        sa.Column("message_key", sa.String(64), nullable=False),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.Column("headers", sa.Text(), nullable=False),
        sa.Column("created_at", DATETIME, nullable=False),
        sa.Column("available_at", DATETIME, nullable=False),
        sa.Column("published_at", DATETIME, nullable=True),
        sa.Column("publish_attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.String(500), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_outbox"),
        sa.UniqueConstraint("event_id", name="uq_outbox_event_id"),
        **TABLE_OPTS,
    )
    op.create_index("ix_outbox_aggregate_id", "outbox", ["aggregate_id"])
    op.create_index("ix_outbox_pending", "outbox", ["published_at", "available_at", "id"])


def downgrade() -> None:
    op.drop_table("outbox")
    op.drop_table("transactions")
