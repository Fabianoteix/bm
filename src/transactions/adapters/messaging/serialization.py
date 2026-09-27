"""Contrato das mensagens Kafka (envelope + versionamento de schema).

Envelope inspirado em CloudEvents::

    {
      "event_id": "uuid",            # chave de deduplicação para consumidores
      "event_type": "TransactionStatusChanged",
      "schema_version": 1,
      "occurred_at": "2026-09-25T17:00:00.123456+00:00",
      "source": "transactions-service",
      "transaction_id": "uuid",      # também é a KEY da mensagem (ordenação)
      "data": { ... }
    }

Estratégia de evolução:
* mudanças **aditivas** (campo novo opcional) → mesmo tópico, ``schema_version``
  incrementado; consumidores são *tolerant readers* (ignoram campos
  desconhecidos) e ``UPCASTERS`` convertem versões antigas para a atual;
* mudanças **incompatíveis** → novo tópico ``*.v2`` com período de publicação
  dupla até os consumidores migrarem.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from transactions.domain.events import (
    DomainEvent,
    ProcessingRequested,
    TransactionCreated,
    TransactionDeadLettered,
    TransactionStatusChanged,
)

SOURCE = "transactions-service"

CURRENT_SCHEMA_VERSION: dict[str, int] = {
    "TransactionCreated": 1,
    "ProcessingRequested": 2,
    "TransactionStatusChanged": 1,
    "TransactionDeadLettered": 1,
}


class PoisonMessage(Exception):
    """Mensagem que nunca poderá ser processada (malformada/versão desconhecida)."""


@dataclass(frozen=True)
class TopicRouting:
    processing: str
    events: str
    dlq: str

    def topic_for(self, event: DomainEvent) -> str:
        if isinstance(event, ProcessingRequested):
            return self.processing
        if isinstance(event, TransactionDeadLettered):
            return self.dlq
        if isinstance(event, TransactionCreated | TransactionStatusChanged):
            return self.events
        raise ValueError(f"sem rota para {event.event_type}")  # pragma: no cover


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def _event_data(event: DomainEvent) -> dict[str, Any]:
    if isinstance(event, TransactionCreated):
        return {"customer_id": event.customer_id, "value": str(event.value)}
    if isinstance(event, ProcessingRequested):
        return {"attempt": event.attempt, "not_before": _iso(event.not_before)}
    if isinstance(event, TransactionStatusChanged):
        return {
            "customer_id": event.customer_id,
            "value": str(event.value),
            "previous_status": event.previous_status,
            "status": event.status,
            "version": event.version,
            "reason": event.reason,
        }
    if isinstance(event, TransactionDeadLettered):
        return {"attempts": event.attempts, "error": event.error}
    raise ValueError(f"evento não serializável: {event.event_type}")  # pragma: no cover


def serialize_event(event: DomainEvent) -> bytes:
    envelope = {
        "event_id": str(event.event_id),
        "event_type": event.event_type,
        "schema_version": CURRENT_SCHEMA_VERSION[event.event_type],
        "occurred_at": _iso(event.occurred_at),
        "source": SOURCE,
        "transaction_id": str(event.transaction_id),
        "data": _event_data(event),
    }
    return json.dumps(envelope, separators=(",", ":")).encode()


# --------------------------------------------------------------- upcasting
Upcaster = Callable[[dict[str, Any]], dict[str, Any]]


def _processing_requested_v1_to_v2(data: dict[str, Any]) -> dict[str, Any]:
    # v1 (primeira versão) não tinha retry agendado: toda mensagem era a tentativa 1.
    return {"attempt": 1, "not_before": None, **data}


UPCASTERS: dict[tuple[str, int], Upcaster] = {
    ("ProcessingRequested", 1): _processing_requested_v1_to_v2,
}


@dataclass(frozen=True)
class ProcessingMessage:
    event_id: uuid.UUID
    transaction_id: uuid.UUID
    attempt: int


def deserialize_processing_message(raw: bytes | None) -> ProcessingMessage:
    """Decodifica mensagens do tópico de processamento, aplicando upcasters."""
    if not raw:
        raise PoisonMessage("mensagem vazia")
    try:
        envelope = json.loads(raw)
        event_type = envelope["event_type"]
        version = int(envelope["schema_version"])
        data = dict(envelope.get("data") or {})
        transaction_id = uuid.UUID(envelope["transaction_id"])
        event_id = uuid.UUID(envelope["event_id"])
    except (ValueError, KeyError, TypeError) as exc:
        raise PoisonMessage(f"envelope inválido: {exc}") from exc

    if event_type != "ProcessingRequested":
        raise PoisonMessage(f"event_type inesperado no tópico de processamento: {event_type}")

    current = CURRENT_SCHEMA_VERSION[event_type]
    if version > current:
        raise PoisonMessage(f"schema_version {version} > suportada ({current}) — deploy pendente?")
    while version < current:
        upcaster = UPCASTERS.get((event_type, version))
        if upcaster is None:
            raise PoisonMessage(f"sem upcaster para {event_type} v{version}")
        data = upcaster(data)
        version += 1

    try:
        attempt = int(data["attempt"])
    except (KeyError, TypeError, ValueError) as exc:
        raise PoisonMessage(f"attempt inválido: {exc}") from exc
    if attempt < 1:
        raise PoisonMessage("attempt deve ser >= 1")
    return ProcessingMessage(event_id=event_id, transaction_id=transaction_id, attempt=attempt)
