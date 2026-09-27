from __future__ import annotations

import uuid
from collections.abc import Callable

from transactions.application.errors import TransactionNotFound
from transactions.application.ports import TransactionView, UnitOfWork


class GetTransaction:
    """Consulta direta no MySQL (fonte da verdade) por chave primária.

    Sem cache de propósito: o status muda justamente enquanto o cliente faz
    polling, e uma leitura por PK no MySQL é barata. Ver README, decisão D8.
    """

    def __init__(self, uow_factory: Callable[[], UnitOfWork]) -> None:
        self._uow_factory = uow_factory

    def execute(self, transaction_id: uuid.UUID) -> TransactionView:
        with self._uow_factory() as uow:
            tx = uow.transactions.get(transaction_id)
        if tx is None:
            raise TransactionNotFound(str(transaction_id))

        return TransactionView.from_entity(tx)
