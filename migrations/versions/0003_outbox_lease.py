"""outbox lease (locked_until / locked_by)

O relay deixa de segurar ``SELECT ... FOR UPDATE`` durante o flush do Kafka.
Ele reserva as linhas com um lease numa transação curta, publica fora de
qualquer transação e marca ``published_at`` em outra transação curta. Se o
relay morrer no meio, o lease expira e outro relay assume.

Só adiciona colunas anuláveis: sem reescrita de dados, compatível com o relay
antigo durante o deploy (ele ignora as colunas).

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

DATETIME = sa.DateTime().with_variant(mysql.DATETIME(fsp=6), "mysql")


def upgrade() -> None:
    with op.batch_alter_table("outbox") as batch:  # batch: compatível com SQLite
        batch.add_column(sa.Column("locked_until", DATETIME, nullable=True))
        batch.add_column(sa.Column("locked_by", sa.String(128), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("outbox") as batch:
        batch.drop_column("locked_by")
        batch.drop_column("locked_until")
