"""Eventos de domínio.

São fatos imutáveis produzidos pela entidade. O domínio não sabe em qual tópico
eles vão parar nem como são serializados: isso é responsabilidade dos adapters
(ver ``adapters/messaging/serialization.py``), o que permite versionar o
contrato externo sem tocar na regra de negócio.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True, kw_only=True)
class DomainEvent:
    transaction_id: uuid.UUID
    occurred_at: datetime
    event_id: uuid.UUID = field(default_factory=uuid.uuid4)

    @property
    def event_type(self) -> str:
        return type(self).__name__


@dataclass(frozen=True, kw_only=True)
class TransactionCreated(DomainEvent):
    customer_id: str
    value: Decimal


@dataclass(frozen=True, kw_only=True)
class ProcessingRequested(DomainEvent):
    """Comando interno: 'analise esta transação, tentativa N'.

    ``not_before`` materializa o backoff: o relay do outbox só publica a
    mensagem depois desse instante (retry atrasado sem bloquear partição).
    """

    attempt: int
    not_before: datetime | None = None


@dataclass(frozen=True, kw_only=True)
class TransactionStatusChanged(DomainEvent):
    """Evento de integração para outros sistemas.

    ``version`` é monotônico por transação: consumidores podem descartar
    eventos fora de ordem comparando com a última versão vista.
    """

    customer_id: str
    value: Decimal
    previous_status: str
    status: str
    version: int
    reason: str


@dataclass(frozen=True, kw_only=True)
class TransactionDeadLettered(DomainEvent):
    attempts: int
    error: str
