"""Relay do Transactional Outbox (MySQL → Kafka).

Resolve o *dual write*: a API e o worker nunca publicam direto no Kafka; eles
gravam o evento na tabela ``outbox`` no mesmo commit da mudança de estado. Este
processo lê os eventos prontos e publica com garantia *at-least-once*.

Cada lote passa por três fases, e **nenhuma transação de banco fica aberta
enquanto o relay espera o Kafka**:

1. **Reserva** (transação curta): ``SELECT ... FOR UPDATE SKIP LOCKED`` das
   linhas prontas e sem lease válido, grava ``locked_until = agora + lease`` e
   ``locked_by`` e faz COMMIT. O lock de linha dura milissegundos; várias
   instâncias do relay dividem o trabalho sem pegar a mesma linha.
2. **Publicação** (sem transação): envia ao Kafka (``acks=all``, producer
   idempotente) e espera o ack. Se o Kafka estiver lento, só este relay espera;
   o banco não segura nada.
3. **Confirmação** (transação curta): sucesso → ``published_at``; falha →
   ``publish_attempts + 1`` e o lease é liberado para a próxima volta.

Se o relay morrer entre 1 e 3, o lease expira e outro relay publica de novo:
duplicata possível, e por isso todo consumidor é idempotente
(``event_id``/fencing/versão). O lease precisa ser maior que o tempo máximo de
entrega do producer (``delivery.timeout.ms`` = 30 s), senão outro relay assume
uma linha que ainda está em voo.

``available_at`` futuro = retry agendado com backoff.
"""

from __future__ import annotations

import json
import os
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Event

import structlog
from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.orm import Session, sessionmaker

from transactions.adapters.messaging.kafka_producer import (
    DELIVERY_TIMEOUT_SECONDS,
    MessagePublisher,
    OutgoingMessage,
)
from transactions.adapters.persistence.models import OutboxModel
from transactions.observability import metrics

log = structlog.get_logger(__name__)


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class _Claimed:
    """Cópia em memória de uma linha reservada (a sessão já foi fechada)."""

    id: int
    event_id: str
    aggregate_id: str
    message: OutgoingMessage


class OutboxRelay:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        publisher: MessagePublisher,
        *,
        batch_size: int = 500,
        publish_timeout: float = 10.0,
        lease_seconds: float = 45.0,
        relay_id: str | None = None,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        if lease_seconds <= max(publish_timeout, DELIVERY_TIMEOUT_SECONDS):
            raise ValueError(
                "lease_seconds precisa ser maior que o publish_timeout e que o "
                f"delivery.timeout.ms do producer ({DELIVERY_TIMEOUT_SECONDS:.0f}s)"
            )
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._publish_timeout = publish_timeout
        self._lease = timedelta(seconds=lease_seconds)
        self._relay_id = relay_id or f"{socket.gethostname()}:{os.getpid()}"
        self._clock = clock

    def run_once(self) -> int:
        """Publica um lote. Retorna quantos eventos foram publicados com sucesso."""
        claimed = self._claim()
        if not claimed:
            return 0
        results = self._publisher.publish_batch([c.message for c in claimed], self._publish_timeout)
        published = self._confirm(claimed, results)
        if published:
            log.debug("outbox.batch_published", count=published)
        return published

    # ------------------------------------------------------------ fase 1
    def _claim(self) -> list[_Claimed]:
        now = self._clock()
        with self._session_factory() as session, session.begin():
            rows = list(
                session.scalars(
                    select(OutboxModel)
                    .where(
                        OutboxModel.published_at.is_(None),
                        OutboxModel.available_at <= now,
                        or_(OutboxModel.locked_until.is_(None), OutboxModel.locked_until <= now),
                    )
                    .order_by(OutboxModel.id)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            claimed: list[_Claimed] = []
            for row in rows:
                if row.locked_by is not None:
                    # Lease vencido: o relay anterior morreu (ou travou) no meio.
                    metrics.OUTBOX_LEASE_RECLAIMED.inc()
                    log.warning(
                        "outbox.lease_reclaimed",
                        event_id=row.event_id,
                        transaction_id=row.aggregate_id,
                        previous_owner=row.locked_by,
                    )
                row.locked_until = now + self._lease
                row.locked_by = self._relay_id
                claimed.append(
                    _Claimed(
                        id=row.id,
                        event_id=row.event_id,
                        aggregate_id=row.aggregate_id,
                        message=OutgoingMessage(
                            topic=row.topic,
                            key=row.message_key,
                            value=row.payload.encode(),
                            headers={
                                "event_id": row.event_id,
                                "event_type": row.event_type,
                                **json.loads(row.headers or "{}"),
                            },
                        ),
                    )
                )
        return claimed  # COMMIT feito: nenhum lock de linha segue aberto

    # ------------------------------------------------------------ fase 3
    def _confirm(self, claimed: list[_Claimed], results: list[str | None]) -> int:
        published_at = self._clock()
        ok_ids = [c.id for c, error in zip(claimed, results, strict=True) if error is None]
        with self._session_factory() as session, session.begin():
            if ok_ids:
                # Publicado é publicado, mesmo que o lease tenha vencido e outro
                # relay tenha reservado a linha: marcar evita uma terceira cópia.
                session.execute(
                    update(OutboxModel)
                    .where(OutboxModel.id.in_(ok_ids), OutboxModel.published_at.is_(None))
                    .values(published_at=published_at, locked_until=None, locked_by=None)
                )
            for item, error in zip(claimed, results, strict=True):
                if error is None:
                    metrics.OUTBOX_PUBLISHED.labels(topic=item.message.topic).inc()
                    continue
                metrics.OUTBOX_PUBLISH_FAILURES.inc()
                # Só libera o lease se ele ainda for deste relay.
                session.execute(
                    update(OutboxModel)
                    .where(OutboxModel.id == item.id, OutboxModel.locked_by == self._relay_id)
                    .values(
                        publish_attempts=OutboxModel.publish_attempts + 1,
                        last_error=error[:500],
                        locked_until=None,
                        locked_by=None,
                    )
                )
                log.warning(
                    "outbox.publish_failed",
                    event_id=item.event_id,
                    transaction_id=item.aggregate_id,
                    topic=item.message.topic,
                    error=error,
                )
        return len(ok_ids)

    def refresh_lag_metrics(self) -> None:
        now = datetime.now(UTC)
        with self._session_factory() as session:
            count, oldest = session.execute(
                select(func.count(OutboxModel.id), func.min(OutboxModel.available_at)).where(
                    OutboxModel.published_at.is_(None), OutboxModel.available_at <= now
                )
            ).one()
        metrics.OUTBOX_PENDING.set(count or 0)
        if oldest is not None and oldest.tzinfo is None:
            oldest = oldest.replace(tzinfo=UTC)
        metrics.OUTBOX_OLDEST_AGE.set((now - oldest).total_seconds() if oldest else 0)

    def purge_published(self, older_than: datetime) -> int:
        """Housekeeping: remove eventos já publicados (retenção configurável)."""
        with self._session_factory() as session, session.begin():
            result = session.execute(
                delete(OutboxModel).where(
                    OutboxModel.published_at.is_not(None), OutboxModel.published_at < older_than
                )
            )
            return int(result.rowcount or 0)  # type: ignore[attr-defined]

    def run_forever(self, stop: Event, idle_sleep: float = 0.2, max_sleep: float = 5.0) -> None:
        sleep = idle_sleep
        last_metrics = 0.0
        while not stop.is_set():
            try:
                published = self.run_once()
                sleep = idle_sleep
                if published >= self._batch_size:
                    continue  # há backlog: não dorme
            except Exception:
                # Banco ou Kafka indisponível: nada foi perdido; tenta de novo com backoff.
                log.exception("outbox.relay_error")
                sleep = min(max(sleep, idle_sleep) * 2, max_sleep)
            if time.monotonic() - last_metrics > 5:
                try:
                    self.refresh_lag_metrics()
                except Exception:
                    log.warning("outbox.metrics_error")
                last_metrics = time.monotonic()
            stop.wait(sleep)
