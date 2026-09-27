"""idempotency key unique per customer

A Idempotency-Key deixa de ser única globalmente e passa a ser única por cliente:
dois clientes podem, sem saber, gerar a mesma chave (ex.: "pedido-1") e isso não
pode fazer um receber a transação do outro nem tomar 409.

Ordem segura: cria a constraint nova ANTES de remover a antiga. Como a antiga
(global) é mais restritiva, não existe dado que viole a nova; não há janela em
que a chave fique sem nenhuma proteção.

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-26
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("transactions") as batch:  # batch: compatível com SQLite
        batch.create_unique_constraint(
            "uq_transactions_customer_idempotency_key", ["customer_id", "idempotency_key"]
        )
        batch.drop_constraint("uq_transactions_idempotency_key", type_="unique")


def downgrade() -> None:
    # Só funciona se não houver a mesma chave usada por clientes diferentes;
    # caso haja, o downgrade falha de propósito (não apagamos dados em silêncio).
    with op.batch_alter_table("transactions") as batch:
        batch.create_unique_constraint("uq_transactions_idempotency_key", ["idempotency_key"])
        batch.drop_constraint("uq_transactions_customer_idempotency_key", type_="unique")
