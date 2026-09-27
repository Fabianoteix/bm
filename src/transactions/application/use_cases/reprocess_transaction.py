from __future__ import annotations

import uuid
from collections.abc import Callable

import structlog

from transactions.application.errors import NotReprocessable, TransactionNotFound
from transactions.application.ports import Clock, UnitOfWork
from transactions.domain.transaction import TransactionStatus

log = structlog.get_logger(__name__)


class ReprocessTransaction:
    """Reprocessa uma transação FAILED (ex.: após corrigir o serviço externo).

    O reprocessamento parte do MySQL (fonte da verdade), não dos bytes da DLQ:
    o estado é resetado e um novo ``ProcessingRequested`` sai pelo outbox, com
    as mesmas garantias de atomicidade do fluxo normal.
    """

    def __init__(self, uow_factory: Callable[[], UnitOfWork], clock: Clock) -> None:
        self._uow_factory = uow_factory
        self._clock = clock

    def execute(self, transaction_id: uuid.UUID) -> None:
        with self._uow_factory() as uow:
            tx = uow.transactions.get(transaction_id)
            if tx is None:
                raise TransactionNotFound(str(transaction_id))
            if tx.status is not TransactionStatus.FAILED:
                raise NotReprocessable(f"status atual é {tx.status}, esperado FAILED")
            expected_version = tx.version
            tx.reset_for_reprocessing(self._clock.now())
            uow.transactions.update(tx, expected_version=expected_version)
            uow.outbox.add(tx.pull_events())
            uow.commit()
        log.info("transaction.reprocess_requested", transaction_id=str(transaction_id))

    def execute_all_failed(self, limit: int = 100) -> list[uuid.UUID]:
        with self._uow_factory() as uow:
            ids = uow.transactions.list_ids_by_status(TransactionStatus.FAILED.value, limit)
        for tx_id in ids:
            self.execute(tx_id)
        return ids
