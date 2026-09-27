"""Fixtures compartilhadas.

Estratégia: o domínio e os casos de uso são testados com adapters REAIS de
persistência (SQLAlchemy) sobre SQLite em memória — rápido e sem Docker — e
dublês apenas para o que é externo e não-determinístico (serviço de risco,
Kafka, relógio).

Com ``TEST_DATABASE_URL=mysql+pymysql://...`` a mesma suíte roda contra MySQL
real (valida SKIP LOCKED, DATETIME(6), constraint UNIQUE, lock otimista).
Kafka de verdade é coberto pelos testes e2e contra o docker compose.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from transactions.adapters.messaging.kafka_producer import OutgoingMessage
from transactions.adapters.messaging.serialization import TopicRouting
from transactions.adapters.persistence.models import Base
from transactions.adapters.persistence.sqlalchemy_uow import SqlAlchemyUnitOfWork, build_engine
from transactions.application.errors import RiskServiceError, RiskServiceUnavailable
from transactions.application.ports import RiskAnalysisRequest, TransactionView, UnitOfWork
from transactions.application.retry_policy import RetryPolicy
from transactions.application.use_cases import (
    CreateTransaction,
    CreateTransactionCommand,
    GetTransaction,
    ProcessTransaction,
    ReprocessTransaction,
)
from transactions.domain.transaction import RiskDecision

ROUTING = TopicRouting(processing="t.processing", events="t.events", dlq="t.dlq")


class FakeClock:
    def __init__(self, start: datetime | None = None) -> None:
        self.current = start or datetime(2026, 9, 25, 12, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.current

    def advance(self, **kwargs: float) -> None:
        self.current += timedelta(**kwargs)


class ScriptedRiskGateway:
    """Devolve respostas em sequência: RiskDecision ou exceção a lançar."""

    def __init__(self, *script: RiskDecision | Exception) -> None:
        self.script = list(script)
        self.calls: list[RiskAnalysisRequest] = []

    def analyze(self, request: RiskAnalysisRequest) -> RiskDecision:
        self.calls.append(request)
        item = self.script.pop(0) if len(self.script) > 1 else self.script[0]
        if isinstance(item, Exception):
            raise item
        return item


class FakePublisher:
    def __init__(self) -> None:
        self.sent: list[OutgoingMessage] = []
        self.fail_with: str | None = None

    def publish_batch(
        self, messages: Sequence[OutgoingMessage], timeout: float
    ) -> list[str | None]:
        if self.fail_with:
            return [self.fail_with] * len(messages)
        self.sent.extend(messages)
        return [None] * len(messages)


# ----------------------------------------------------------------- fixtures
@pytest.fixture(scope="session")
def _mysql_engine() -> Iterator[Engine | None]:
    """Se TEST_DATABASE_URL apontar para um MySQL, a MESMA suíte roda contra ele."""
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        yield None
        return
    eng = build_engine(url)
    Base.metadata.drop_all(eng)
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def engine(_mysql_engine: Engine | None) -> Iterator[Engine]:
    if _mysql_engine is not None:
        with _mysql_engine.begin() as conn:
            for table in reversed(Base.metadata.sorted_tables):
                conn.execute(table.delete())
        yield _mysql_engine
        return

    eng = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture
def uow_factory(session_factory: sessionmaker[Session]) -> Callable[[], UnitOfWork]:
    return lambda: SqlAlchemyUnitOfWork(session_factory, ROUTING)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def policy() -> RetryPolicy:
    return RetryPolicy(max_attempts=3, base_delay_seconds=5, jitter_ratio=0)


@pytest.fixture
def create_tx(uow_factory: Callable[[], UnitOfWork], clock: FakeClock) -> CreateTransaction:
    return CreateTransaction(uow_factory, clock)


@pytest.fixture
def get_tx(uow_factory: Callable[[], UnitOfWork]) -> GetTransaction:
    return GetTransaction(uow_factory)


@pytest.fixture
def reprocess_tx(uow_factory: Callable[[], UnitOfWork], clock: FakeClock) -> ReprocessTransaction:
    return ReprocessTransaction(uow_factory, clock)


@pytest.fixture
def make_processor(
    uow_factory: Callable[[], UnitOfWork],
    clock: FakeClock,
    policy: RetryPolicy,
) -> Callable[..., tuple[ProcessTransaction, ScriptedRiskGateway]]:
    def _make(*script: RiskDecision | Exception) -> tuple[ProcessTransaction, ScriptedRiskGateway]:
        gateway = ScriptedRiskGateway(*script)
        return ProcessTransaction(uow_factory, gateway, clock, policy), gateway

    return _make


@pytest.fixture
def new_transaction(create_tx: CreateTransaction) -> Callable[..., TransactionView]:
    def _new(
        customer_id: str = "123", value: str = "1500.00", key: str | None = None
    ) -> TransactionView:
        return create_tx.execute(
            CreateTransactionCommand(customer_id, Decimal(value), key)
        ).transaction

    return _new


TRANSIENT = RiskServiceUnavailable("HTTP 503")
PERMANENT = RiskServiceError("HTTP 400")
