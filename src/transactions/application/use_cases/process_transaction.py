from __future__ import annotations

import uuid
from collections.abc import Callable
from enum import StrEnum

import structlog

from transactions.application.errors import (
    ConcurrencyConflict,
    RiskServiceError,
    RiskServiceUnavailable,
)
from transactions.application.ports import (
    Clock,
    RiskAnalysisGateway,
    RiskAnalysisRequest,
    UnitOfWork,
)
from transactions.application.retry_policy import RetryPolicy
from transactions.domain.transaction import RiskDecision

log = structlog.get_logger(__name__)


class ProcessingOutcome(StrEnum):
    APPROVED = "approved"
    REJECTED = "rejected"
    RETRY_SCHEDULED = "retry_scheduled"
    FAILED = "failed"  # esgotou tentativas / erro permanente -> DLQ
    DUPLICATE = "duplicate"  # mensagem duplicada ou obsoleta (idempotência)
    CONCURRENT_UPDATE = "concurrent_update"  # outra instância venceu a corrida
    NOT_FOUND = "not_found"


class ProcessTransaction:
    """Analisa a transação no serviço de risco e registra o resultado.

    Garantias:
    * **Idempotência** — ``Transaction.accepts_attempt`` descarta mensagens
      duplicadas/obsoletas; o UPDATE com lock otimista (``version``) resolve a
      corrida entre dois consumers durante um rebalance.
    * **Sem locks durante IO externo** — o banco é lido, a transação de banco é
      fechada, o serviço externo é chamado e só então o resultado é gravado.
    * **Atomicidade estado + evento** — novo status e eventos (status alterado,
      retry agendado, dead letter) vão juntos no mesmo commit via outbox.
    """

    def __init__(
        self,
        uow_factory: Callable[[], UnitOfWork],
        risk_gateway: RiskAnalysisGateway,
        clock: Clock,
        retry_policy: RetryPolicy,
    ) -> None:
        self._uow_factory = uow_factory
        self._risk = risk_gateway
        self._clock = clock
        self._policy = retry_policy

    def execute(self, transaction_id: uuid.UUID, attempt: int) -> ProcessingOutcome:
        structlog.contextvars.bind_contextvars(transaction_id=str(transaction_id), attempt=attempt)

        with self._uow_factory() as uow:
            tx = uow.transactions.get(transaction_id)
        if tx is None:
            log.error("processing.transaction_not_found")
            return ProcessingOutcome.NOT_FOUND

        if not tx.accepts_attempt(attempt):
            log.info(
                "processing.duplicate_skipped",
                status=tx.status.value,
                attempts_done=tx.attempts,
            )
            return ProcessingOutcome.DUPLICATE

        expected_version = tx.version
        tx.start_attempt(attempt)
        request = RiskAnalysisRequest(tx.id, tx.customer_id, tx.value)

        outcome: ProcessingOutcome
        try:
            decision = self._risk.analyze(request)
        except RiskServiceUnavailable as exc:
            now = self._clock.now()
            if self._policy.can_retry(attempt):
                delay = self._policy.delay_after(attempt)
                tx.schedule_retry(error=str(exc), now=now, retry_at=now + delay)
                outcome = ProcessingOutcome.RETRY_SCHEDULED
                log.warning(
                    "processing.transient_failure",
                    error=str(exc),
                    retry_in_seconds=round(delay.total_seconds(), 2),
                    max_attempts=self._policy.max_attempts,
                )
            else:
                tx.fail(error=f"max attempts reached: {exc}", now=now)
                outcome = ProcessingOutcome.FAILED
                log.error("processing.max_attempts_exceeded", error=str(exc))
        except RiskServiceError as exc:
            tx.fail(error=f"permanent error: {exc}", now=self._clock.now())
            outcome = ProcessingOutcome.FAILED
            log.error("processing.permanent_failure", error=str(exc))
        else:
            tx.apply_decision(decision, self._clock.now())
            outcome = (
                ProcessingOutcome.APPROVED
                if decision is RiskDecision.APPROVED
                else ProcessingOutcome.REJECTED
            )

        try:
            with self._uow_factory() as uow:
                uow.transactions.update(tx, expected_version=expected_version)
                uow.outbox.add(tx.pull_events())
                uow.commit()
        except ConcurrencyConflict:
            log.warning("processing.concurrent_update_skipped")
            return ProcessingOutcome.CONCURRENT_UPDATE

        log.info("processing.completed", outcome=outcome.value, status=tx.status.value)
        return outcome
