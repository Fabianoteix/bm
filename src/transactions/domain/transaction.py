"""Entidade Transaction e sua máquina de estados.

Esta camada não conhece Kafka, MySQL ou HTTP. Ela só sabe:
- quais estados existem e quais transições são válidas;
- quais eventos de domínio cada transição produz;
- se uma tentativa de processamento é legítima (fencing por número de tentativa),
  o que é a base da idempotência do consumer.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from enum import StrEnum

from transactions.domain.errors import InvalidStateTransition, InvalidTransaction
from transactions.domain.events import (
    DomainEvent,
    ProcessingRequested,
    TransactionCreated,
    TransactionDeadLettered,
    TransactionStatusChanged,
)

MAX_VALUE = Decimal("999999999999.99")  # cabe em DECIMAL(14,2)
MAX_CUSTOMER_ID_LENGTH = 64
MAX_ERROR_LENGTH = 500


class TransactionStatus(StrEnum):
    PENDING = "PENDING"  # criada, aguardando (primeira) análise
    RETRYING = "RETRYING"  # falha temporária; nova tentativa agendada
    APPROVED = "APPROVED"  # final
    REJECTED = "REJECTED"  # final
    FAILED = "FAILED"  # esgotou tentativas ou erro permanente; pode ser reprocessada

    @property
    def is_final(self) -> bool:
        return self in (TransactionStatus.APPROVED, TransactionStatus.REJECTED)

    @property
    def is_processable(self) -> bool:
        return self in (TransactionStatus.PENDING, TransactionStatus.RETRYING)


_ALLOWED_TRANSITIONS: dict[TransactionStatus, frozenset[TransactionStatus]] = {
    TransactionStatus.PENDING: frozenset(
        {
            TransactionStatus.APPROVED,
            TransactionStatus.REJECTED,
            TransactionStatus.RETRYING,
            TransactionStatus.FAILED,
        }
    ),
    TransactionStatus.RETRYING: frozenset(
        {
            TransactionStatus.APPROVED,
            TransactionStatus.REJECTED,
            TransactionStatus.RETRYING,
            TransactionStatus.FAILED,
        }
    ),
    TransactionStatus.FAILED: frozenset({TransactionStatus.PENDING}),  # reprocessamento manual
    TransactionStatus.APPROVED: frozenset(),
    TransactionStatus.REJECTED: frozenset(),
}


class RiskDecision(StrEnum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


def normalize_value(raw: Decimal | float | int | str) -> Decimal:
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError) as exc:
        raise InvalidTransaction("value deve ser numérico") from exc
    if not value.is_finite():
        raise InvalidTransaction("value deve ser finito")
    if value <= 0:
        raise InvalidTransaction("value deve ser maior que zero")
    if value > MAX_VALUE:
        raise InvalidTransaction(f"value deve ser no máximo {MAX_VALUE}")
    if value != value.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN):
        raise InvalidTransaction("value deve ter no máximo 2 casas decimais")
    return value.quantize(Decimal("0.01"))


def normalize_customer_id(raw: str) -> str:
    customer_id = (raw or "").strip()
    if not customer_id:
        raise InvalidTransaction("customer_id é obrigatório")
    if len(customer_id) > MAX_CUSTOMER_ID_LENGTH:
        raise InvalidTransaction(
            f"customer_id deve ter no máximo {MAX_CUSTOMER_ID_LENGTH} caracteres"
        )
    return customer_id


@dataclass
class Transaction:
    id: uuid.UUID
    customer_id: str
    value: Decimal
    status: TransactionStatus
    created_at: datetime
    updated_at: datetime
    attempts: int = 0
    version: int = 1  # controle de concorrência otimista + ordenação de eventos de status
    last_error: str | None = None
    idempotency_key: str | None = None
    _events: list[DomainEvent] = field(default_factory=list, repr=False, compare=False)

    # ------------------------------------------------------------------ criação
    @classmethod
    def create(
        cls,
        *,
        customer_id: str,
        value: Decimal | float | int | str,
        now: datetime,
        idempotency_key: str | None = None,
        transaction_id: uuid.UUID | None = None,
    ) -> Transaction:
        tx = cls(
            id=transaction_id or uuid.uuid4(),
            customer_id=normalize_customer_id(customer_id),
            value=normalize_value(value),
            status=TransactionStatus.PENDING,
            created_at=now,
            updated_at=now,
            idempotency_key=idempotency_key,
        )
        tx._events.append(
            TransactionCreated(
                transaction_id=tx.id,
                customer_id=tx.customer_id,
                value=tx.value,
                occurred_at=now,
            )
        )
        tx._events.append(ProcessingRequested(transaction_id=tx.id, attempt=1, occurred_at=now))
        return tx

    def same_request_as(self, customer_id: str, value: Decimal | float | int | str) -> bool:
        """Usado na idempotência da API: mesma chave precisa significar mesmo payload."""
        return self.customer_id == normalize_customer_id(customer_id) and self.value == (
            normalize_value(value)
        )

    # ------------------------------------------------------------ processamento
    def accepts_attempt(self, attempt: int) -> bool:
        """Fencing de tentativa.

        Uma mensagem de processamento só é legítima se a transação ainda pode ser
        processada E se ela carrega exatamente a próxima tentativa esperada.
        Mensagens duplicadas (mesma tentativa entregue 2x) ou atrasadas (tentativa
        antiga) são descartadas sem efeitos colaterais.
        """
        return self.status.is_processable and attempt == self.attempts + 1

    def start_attempt(self, attempt: int) -> None:
        if not self.accepts_attempt(attempt):
            raise InvalidStateTransition(
                f"tentativa {attempt} não aceita (status={self.status}, attempts={self.attempts})"
            )
        self.attempts = attempt

    def apply_decision(self, decision: RiskDecision, now: datetime) -> None:
        target = (
            TransactionStatus.APPROVED
            if decision is RiskDecision.APPROVED
            else TransactionStatus.REJECTED
        )
        self.last_error = None
        self._transition(target, now, reason=f"risk_analysis:{decision.value}")

    def schedule_retry(self, *, error: str, now: datetime, retry_at: datetime) -> None:
        self.last_error = _truncate(error)
        self._transition(TransactionStatus.RETRYING, now, reason="transient_failure")
        self._events.append(
            ProcessingRequested(
                transaction_id=self.id,
                attempt=self.attempts + 1,
                occurred_at=now,
                not_before=retry_at,
            )
        )

    def fail(self, *, error: str, now: datetime) -> None:
        self.last_error = _truncate(error)
        self._transition(TransactionStatus.FAILED, now, reason="processing_failed")
        self._events.append(
            TransactionDeadLettered(
                transaction_id=self.id,
                attempts=self.attempts,
                error=self.last_error,
                occurred_at=now,
            )
        )

    def reset_for_reprocessing(self, now: datetime) -> None:
        """Reprocessamento manual (operação) de uma transação FAILED."""
        self._transition(TransactionStatus.PENDING, now, reason="manual_reprocess")
        self.attempts = 0
        self.last_error = None
        self._events.append(ProcessingRequested(transaction_id=self.id, attempt=1, occurred_at=now))

    # ------------------------------------------------------------------ eventos
    def pull_events(self) -> list[DomainEvent]:
        events, self._events = self._events, []
        return events

    # ------------------------------------------------------------------ interno
    def _transition(self, target: TransactionStatus, now: datetime, *, reason: str) -> None:
        if target not in _ALLOWED_TRANSITIONS[self.status]:
            raise InvalidStateTransition(f"{self.status} -> {target} não é permitido")
        previous = self.status
        self.status = target
        self.updated_at = now
        self.version += 1
        if previous != target:
            self._events.append(
                TransactionStatusChanged(
                    transaction_id=self.id,
                    customer_id=self.customer_id,
                    value=self.value,
                    previous_status=previous.value,
                    status=target.value,
                    version=self.version,
                    reason=reason,
                    occurred_at=now,
                )
            )


def _truncate(text: str) -> str:
    return text if len(text) <= MAX_ERROR_LENGTH else text[: MAX_ERROR_LENGTH - 3] + "..."
