"""Adapter de entrada Kafka: consome ``transactions.processing.v1``.

Semântica de entrega: **at-least-once**.

* ``enable.auto.offset.store=false``: o offset só é marcado para commit DEPOIS
  que o caso de uso terminou e o MySQL confirmou o commit. Se o processo morrer
  antes (Cenário 2), a mensagem é reentregue e o fencing de tentativa no domínio
  a descarta como duplicada — sem efeito colateral duplicado no banco.
* Mensagem inválida (poison) → DLQ imediatamente com o motivo nos headers; a
  partição não fica travada.
* Erro inesperado de infraestrutura (ex.: MySQL fora) → offset NÃO é avançado,
  o consumer faz ``seek`` para a mesma mensagem e tenta de novo com backoff.
  Isso pausa a partição (correto: nenhuma mensagem conseguiria ser processada).
* Backpressure: enquanto ``should_pause()`` for verdadeiro (circuit breaker do
  serviço de risco ABERTO), as partições ficam pausadas. As mensagens esperam no
  Kafka em vez de virarem, uma a uma, retries agendados no MySQL.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from threading import Event

import structlog
from confluent_kafka import Consumer, KafkaError, Message, TopicPartition

from transactions.adapters.messaging.kafka_producer import MessagePublisher, OutgoingMessage
from transactions.adapters.messaging.serialization import (
    PoisonMessage,
    deserialize_processing_message,
)
from transactions.application.use_cases import ProcessingOutcome, ProcessTransaction
from transactions.observability import metrics

log = structlog.get_logger(__name__)


class HandleResult(StrEnum):
    DONE = "done"  # pode avançar o offset
    RETRY = "retry"  # não avançar; reprocessar a mesma mensagem


@dataclass(frozen=True)
class IncomingMessage:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: dict[str, str]


class ProcessingMessageHandler:
    """Lógica do consumer independente do cliente Kafka (fácil de testar)."""

    def __init__(
        self,
        use_case: ProcessTransaction,
        dlq_publisher: MessagePublisher,
        dlq_topic: str,
    ) -> None:
        self._use_case = use_case
        self._dlq = dlq_publisher
        self._dlq_topic = dlq_topic

    def handle(self, msg: IncomingMessage) -> HandleResult:
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            correlation_id=msg.headers.get("correlation_id"),
            event_id=msg.headers.get("event_id"),
            kafka_partition=msg.partition,
            kafka_offset=msg.offset,
        )
        started = time.perf_counter()
        try:
            decoded = deserialize_processing_message(msg.value)
        except PoisonMessage as exc:
            return self._to_dlq(msg, reason="poison", error=str(exc))

        try:
            outcome = self._use_case.execute(decoded.transaction_id, decoded.attempt)
        except Exception as exc:
            metrics.CONSUMER_ERRORS.inc()
            log.exception("consumer.unexpected_error", error=str(exc))
            return HandleResult.RETRY
        finally:
            metrics.PROCESSING_DURATION.observe(time.perf_counter() - started)

        metrics.PROCESSING_OUTCOMES.labels(outcome=outcome.value).inc()
        if outcome is ProcessingOutcome.NOT_FOUND:
            # Não deveria acontecer (o outbox garante que a transação existe).
            return self._to_dlq(msg, reason="transaction_not_found", error="transaction not found")
        return HandleResult.DONE

    def _to_dlq(self, msg: IncomingMessage, *, reason: str, error: str) -> HandleResult:
        metrics.POISON_MESSAGES.labels(reason=reason).inc()
        headers = {
            **msg.headers,
            "dlq_reason": reason,
            "dlq_error": error[:500],
            "dlq_source_topic": msg.topic,
            "dlq_source_partition": str(msg.partition),
            "dlq_source_offset": str(msg.offset),
        }
        result = self._dlq.publish_batch(
            [
                OutgoingMessage(
                    topic=self._dlq_topic,
                    key=(msg.key or b"").decode(errors="replace"),
                    value=msg.value or b"",
                    headers=headers,
                )
            ],
            timeout=10.0,
        )[0]
        if result is not None:
            log.error("consumer.dlq_publish_failed", error=result)
            return HandleResult.RETRY  # não perder a mensagem
        log.error("consumer.sent_to_dlq", reason=reason, error=error)
        return HandleResult.DONE


def consumer_config(
    bootstrap_servers: str, group_id: str, max_poll_interval_ms: int
) -> dict[str, object]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": True,  # commita periodicamente APENAS offsets armazenados
        "enable.auto.offset.store": False,  # ... e só armazenamos após processar
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": max_poll_interval_ms,
        "partition.assignment.strategy": "cooperative-sticky",  # rebalance incremental
        "isolation.level": "read_committed",
    }


def _to_incoming(msg: Message) -> IncomingMessage:
    headers: dict[str, str] = {}
    raw_headers: list[tuple[str, bytes | str | None]] = list(msg.headers() or [])  # type: ignore[arg-type]
    for k, v in raw_headers:
        headers[k] = v.decode(errors="replace") if isinstance(v, bytes) else str(v)
    return IncomingMessage(
        topic=msg.topic() or "",
        partition=msg.partition() or 0,
        offset=msg.offset() or 0,
        key=msg.key(),
        value=msg.value(),
        headers=headers,
    )


class KafkaProcessingConsumer:
    def __init__(
        self,
        consumer: Consumer,
        topic: str,
        handler: ProcessingMessageHandler,
        *,
        poll_timeout: float = 1.0,
        max_retry_delay: float = 30.0,
        should_pause: Callable[[], bool] = lambda: False,
    ) -> None:
        self._consumer = consumer
        self._topic = topic
        self._handler = handler
        self._poll_timeout = poll_timeout
        self._max_retry_delay = max_retry_delay
        self._should_pause = should_pause
        self._paused = False

    def _apply_backpressure(self) -> None:
        """Pausa/retoma as partições atribuídas conforme ``should_pause()``.

        ``pause`` é reaplicado a cada volta porque um rebalance pode atribuir
        partições novas (que chegam despausadas). O ``poll`` continua sendo
        chamado mesmo pausado, para manter o consumer vivo no grupo.
        """
        pause = self._should_pause()
        if pause:
            assignment = self._consumer.assignment()
            if assignment:
                self._consumer.pause(assignment)
            if not self._paused:
                self._paused = True
                metrics.CONSUMER_PAUSED.set(1)
                log.warning("consumer.paused", reason="risk_circuit_open")
        elif self._paused:
            assignment = self._consumer.assignment()
            if assignment:
                self._consumer.resume(assignment)
            self._paused = False
            metrics.CONSUMER_PAUSED.set(0)
            log.info("consumer.resumed")

    def run(self, stop: Event) -> None:
        self._consumer.subscribe([self._topic])
        retry_delay = 0.5
        try:
            while not stop.is_set():
                self._apply_backpressure()
                msg = self._consumer.poll(self._poll_timeout)
                if msg is None:
                    continue
                error = msg.error()
                if error is not None:
                    if error.code() != KafkaError._PARTITION_EOF:
                        log.error("consumer.kafka_error", error=str(error))
                    continue

                incoming = _to_incoming(msg)
                result = self._handler.handle(incoming)
                if result is HandleResult.DONE:
                    self._consumer.store_offsets(message=msg)
                    retry_delay = 0.5
                else:
                    # Volta o ponteiro para esta mensagem e espera (backoff).
                    self._consumer.seek(
                        TopicPartition(incoming.topic, incoming.partition, incoming.offset)
                    )
                    stop.wait(retry_delay)
                    retry_delay = min(retry_delay * 2, self._max_retry_delay)
        finally:
            structlog.contextvars.clear_contextvars()
            self._consumer.close()  # commita offsets armazenados e sai do grupo
