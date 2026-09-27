"""Portas (interfaces) da aplicação.

A camada de aplicação depende apenas destes contratos. MySQL/SQLAlchemy, Kafka,
o serviço de risco e o FastAPI são adapters que os implementam
(Arquitetura Hexagonal / Ports and Adapters).
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from types import TracebackType
from typing import Protocol, Self

from transactions.domain.events import DomainEvent
from transactions.domain.transaction import RiskDecision, Transaction


class Clock(Protocol):
    def now(self) -> datetime: ...


class TransactionRepository(Protocol):
    def get(self, transaction_id: uuid.UUID) -> Transaction | None: ...

    def get_by_idempotency_key(self, customer_id: str, key: str) -> Transaction | None: ...

    def add(self, transaction: Transaction) -> None: ...

    def update(self, transaction: Transaction, *, expected_version: int) -> None:
        """Persiste com lock otimista. Lança ``ConcurrencyConflict`` se a versão mudou."""
        ...

    def list_ids_by_status(self, status: str, limit: int) -> list[uuid.UUID]: ...


class Outbox(Protocol):
    def add(self, events: Sequence[DomainEvent]) -> None:
        """Grava eventos na MESMA transação de banco da mudança de estado."""
        ...


class UnitOfWork(Protocol):
    @property
    def transactions(self) -> TransactionRepository: ...

    @property
    def outbox(self) -> Outbox: ...

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...


@dataclass(frozen=True)
class RiskAnalysisRequest:
    transaction_id: uuid.UUID
    customer_id: str
    value: Decimal


class RiskAnalysisGateway(Protocol):
    def analyze(self, request: RiskAnalysisRequest) -> RiskDecision:
        """Lança ``RiskServiceUnavailable`` (transitório) ou ``RiskServiceError`` (permanente)."""
        ...


@dataclass(frozen=True)
class TransactionView:
    """Modelo de leitura (o que a API expõe)."""

    id: uuid.UUID
    customer_id: str
    value: Decimal
    status: str
    attempts: int
    last_error: str | None
    created_at: datetime
    updated_at: datetime

    @property
    def is_final(self) -> bool:
        return self.status in ("APPROVED", "REJECTED")

    @classmethod
    def from_entity(cls, tx: Transaction) -> TransactionView:
        return cls(
            id=tx.id,
            customer_id=tx.customer_id,
            value=tx.value,
            status=tx.status.value,
            attempts=tx.attempts,
            last_error=tx.last_error,
            created_at=tx.created_at,
            updated_at=tx.updated_at,
        )
