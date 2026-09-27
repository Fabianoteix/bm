"""Adapter de persistência (MySQL via SQLAlchemy) — repositório, outbox e UoW."""

from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from types import TracebackType
from typing import Self

import structlog
from sqlalchemy import create_engine, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from transactions.adapters.messaging.serialization import TopicRouting, serialize_event
from transactions.adapters.persistence.models import OutboxModel, TransactionModel
from transactions.application.errors import ConcurrencyConflict, DuplicateIdempotencyKey
from transactions.domain.events import DomainEvent, ProcessingRequested
from transactions.domain.transaction import Transaction, TransactionStatus


def build_engine(
    url: str, *, pool_size: int = 10, max_overflow: int = 20, pool_recycle: int = 1800
) -> Engine:
    kwargs: dict[str, object] = {"pool_pre_ping": True, "future": True}
    if not url.startswith("sqlite"):
        kwargs.update(
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_recycle=pool_recycle,
            isolation_level="READ COMMITTED",
        )
    return create_engine(url, **kwargs)


# ----------------------------------------------------------------- mapeamento
def _to_entity(row: TransactionModel) -> Transaction:
    return Transaction(
        id=uuid.UUID(row.id),
        customer_id=row.customer_id,
        value=row.value,
        status=TransactionStatus(row.status),
        created_at=row.created_at,
        updated_at=row.updated_at,
        attempts=row.attempts,
        version=row.version,
        last_error=row.last_error,
        idempotency_key=row.idempotency_key,
    )


class SqlAlchemyTransactionRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def get(self, transaction_id: uuid.UUID) -> Transaction | None:
        row = self._session.get(TransactionModel, str(transaction_id))
        return _to_entity(row) if row else None

    def get_by_idempotency_key(self, customer_id: str, key: str) -> Transaction | None:
        row = self._session.scalar(
            select(TransactionModel).where(
                TransactionModel.customer_id == customer_id,
                TransactionModel.idempotency_key == key,
            )
        )
        return _to_entity(row) if row else None

    def add(self, transaction: Transaction) -> None:
        self._session.add(
            TransactionModel(
                id=str(transaction.id),
                customer_id=transaction.customer_id,
                value=transaction.value,
                status=transaction.status.value,
                attempts=transaction.attempts,
                version=transaction.version,
                last_error=transaction.last_error,
                idempotency_key=transaction.idempotency_key,
                created_at=transaction.created_at,
                updated_at=transaction.updated_at,
            )
        )

    def update(self, transaction: Transaction, *, expected_version: int) -> None:
        # UPDATE ... WHERE id = :id AND version = :expected  (lock otimista)
        result = self._session.execute(
            update(TransactionModel)
            .where(
                TransactionModel.id == str(transaction.id),
                TransactionModel.version == expected_version,
            )
            .values(
                status=transaction.status.value,
                attempts=transaction.attempts,
                version=transaction.version,
                last_error=transaction.last_error,
                updated_at=transaction.updated_at,
            )
            .execution_options(synchronize_session=False)
        )
        if result.rowcount != 1:  # type: ignore[attr-defined]
            raise ConcurrencyConflict(
                f"transação {transaction.id} alterada concorrentemente "
                f"(versão esperada {expected_version})"
            )

    def list_ids_by_status(self, status: str, limit: int) -> list[uuid.UUID]:
        rows = self._session.scalars(
            select(TransactionModel.id)
            .where(TransactionModel.status == status)
            .order_by(TransactionModel.updated_at)
            .limit(limit)
        )
        return [uuid.UUID(r) for r in rows]


class SqlAlchemyOutbox:
    def __init__(self, session: Session, routing: TopicRouting) -> None:
        self._session = session
        self._routing = routing

    def add(self, events: Sequence[DomainEvent]) -> None:
        now = datetime.now(UTC)
        ctx = structlog.contextvars.get_contextvars()
        headers = json.dumps({k: str(ctx[k]) for k in ("correlation_id",) if ctx.get(k)})
        for event in events:
            available_at = now
            if isinstance(event, ProcessingRequested) and event.not_before:
                available_at = event.not_before
            self._session.add(
                OutboxModel(
                    event_id=str(event.event_id),
                    event_type=event.event_type,
                    aggregate_id=str(event.transaction_id),
                    topic=self._routing.topic_for(event),
                    message_key=str(event.transaction_id),  # ordenação por transação
                    payload=serialize_event(event).decode(),
                    headers=headers,
                    created_at=now,
                    available_at=available_at,
                    publish_attempts=0,
                )
            )


class SqlAlchemyUnitOfWork:
    """Uma instância = uma transação de banco. Rollback automático se não houver commit."""

    transactions: SqlAlchemyTransactionRepository
    outbox: SqlAlchemyOutbox

    def __init__(self, session_factory: sessionmaker[Session], routing: TopicRouting) -> None:
        self._session_factory = session_factory
        self._routing = routing

    def __enter__(self) -> Self:
        self._session = self._session_factory()
        self.transactions = SqlAlchemyTransactionRepository(self._session)
        self.outbox = SqlAlchemyOutbox(self._session, self._routing)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            self._session.rollback()  # no-op se já houve commit
        finally:
            self._session.close()

    def commit(self) -> None:
        try:
            self._session.commit()
        except IntegrityError as exc:
            self._session.rollback()
            if "idempotency_key" in str(exc.orig).lower():
                raise DuplicateIdempotencyKey(str(exc.orig)) from exc
            raise

    def rollback(self) -> None:
        self._session.rollback()
