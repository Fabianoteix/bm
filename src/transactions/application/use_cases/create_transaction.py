from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

import structlog

from transactions.application.errors import DuplicateIdempotencyKey, IdempotencyConflict
from transactions.application.ports import Clock, TransactionView, UnitOfWork
from transactions.domain.transaction import Transaction, normalize_customer_id

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class CreateTransactionCommand:
    customer_id: str
    value: Decimal
    idempotency_key: str | None = None


@dataclass(frozen=True)
class CreateTransactionResult:
    transaction: TransactionView
    created: bool  # False => replay idempotente de uma requisição anterior


class CreateTransaction:
    """Persiste a transação e o evento de processamento de forma ATÔMICA.

    Transação + registro no outbox são gravados no mesmo COMMIT do MySQL. Se o
    Kafka estiver fora, nada se perde: o relay publica depois (Cenário 1).
    """

    def __init__(self, uow_factory: Callable[[], UnitOfWork], clock: Clock) -> None:
        self._uow_factory = uow_factory
        self._clock = clock

    def execute(self, cmd: CreateTransactionCommand) -> CreateTransactionResult:
        if cmd.idempotency_key:
            existing = self._find_existing(cmd)
            if existing is not None:
                return existing

        tx = Transaction.create(
            customer_id=cmd.customer_id,
            value=cmd.value,
            now=self._clock.now(),
            idempotency_key=cmd.idempotency_key,
        )
        try:
            with self._uow_factory() as uow:
                uow.transactions.add(tx)
                uow.outbox.add(tx.pull_events())
                uow.commit()
        except DuplicateIdempotencyKey:
            # Corrida: duas requisições com a mesma chave passaram pelo lookup ao
            # mesmo tempo. A constraint UNIQUE decidiu o vencedor; devolvemos o dele.
            existing = self._find_existing(cmd)
            if existing is None:  # pragma: no cover - defensivo
                raise
            return existing

        structlog.contextvars.bind_contextvars(transaction_id=str(tx.id))
        log.info("transaction.created", customer_id=tx.customer_id, value=str(tx.value))
        return CreateTransactionResult(TransactionView.from_entity(tx), created=True)

    def _find_existing(self, cmd: CreateTransactionCommand) -> CreateTransactionResult | None:
        assert cmd.idempotency_key is not None
        with self._uow_factory() as uow:
            existing = uow.transactions.get_by_idempotency_key(
                normalize_customer_id(cmd.customer_id), cmd.idempotency_key
            )
        if existing is None:
            return None
        if not existing.same_request_as(cmd.customer_id, cmd.value):
            raise IdempotencyConflict("Idempotency-Key já utilizada com um payload diferente")
        log.info("transaction.idempotent_replay", transaction_id=str(existing.id))
        return CreateTransactionResult(TransactionView.from_entity(existing), created=False)
