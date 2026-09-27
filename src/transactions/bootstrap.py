"""Composition root: o ÚNICO lugar que conhece todos os adapters concretos."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from transactions.adapters.messaging.serialization import TopicRouting
from transactions.adapters.persistence.sqlalchemy_uow import SqlAlchemyUnitOfWork, build_engine
from transactions.adapters.risk.circuit_breaker import CircuitBreaker
from transactions.adapters.risk.http_client import HttpRiskAnalysisClient
from transactions.application.ports import UnitOfWork
from transactions.application.use_cases import (
    CreateTransaction,
    GetTransaction,
    ProcessTransaction,
    ReprocessTransaction,
)
from transactions.config import Settings
from transactions.observability import metrics
from transactions.observability.circuit_breaker import CircuitBreakerTelemetry


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


@dataclass
class Container:
    settings: Settings
    engine: Engine
    session_factory: sessionmaker[Session]
    routing: TopicRouting
    clock: SystemClock

    def uow(self) -> UnitOfWork:
        return SqlAlchemyUnitOfWork(self.session_factory, self.routing)

    # ---- casos de uso
    def create_transaction(self) -> CreateTransaction:
        return CreateTransaction(self.uow, self.clock)

    def get_transaction(self) -> GetTransaction:
        return GetTransaction(self.uow)

    def reprocess_transaction(self) -> ReprocessTransaction:
        return ReprocessTransaction(self.uow, self.clock)

    def risk_circuit_breaker(self) -> CircuitBreaker:
        s = self.settings
        return CircuitBreaker(
            failure_threshold=s.circuit_failure_threshold,
            reset_timeout=s.circuit_reset_timeout_seconds,
            on_state_change=CircuitBreakerTelemetry(),
        )

    def process_transaction(self, breaker: CircuitBreaker | None = None) -> ProcessTransaction:
        s = self.settings
        breaker = breaker or self.risk_circuit_breaker()
        http_client = httpx.Client(
            base_url=s.risk_service_url,
            timeout=httpx.Timeout(
                s.risk_read_timeout_seconds, connect=s.risk_connect_timeout_seconds
            ),
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )
        gateway = HttpRiskAnalysisClient(
            http_client,
            breaker,
            inline_retries=s.risk_inline_retries,
            inline_backoff_seconds=s.risk_inline_backoff_seconds,
            observe_latency=lambda outcome, secs: metrics.RISK_CALLS.labels(outcome).observe(secs),
        )
        return ProcessTransaction(self.uow, gateway, self.clock, s.retry_policy())

    def readiness_check(self) -> None:
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))


def build_container(settings: Settings) -> Container:
    engine = build_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_recycle=settings.db_pool_recycle_seconds,
    )
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    routing = TopicRouting(
        processing=settings.kafka_processing_topic,
        events=settings.kafka_events_topic,
        dlq=settings.kafka_dlq_topic,
    )
    return Container(settings, engine, session_factory, routing, SystemClock())
