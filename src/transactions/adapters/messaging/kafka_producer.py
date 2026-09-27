from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol

from confluent_kafka import KafkaError, Message, Producer


@dataclass(frozen=True)
class OutgoingMessage:
    topic: str
    key: str
    value: bytes
    headers: dict[str, str] = field(default_factory=dict)


class MessagePublisher(Protocol):
    def publish_batch(
        self, messages: Sequence[OutgoingMessage], timeout: float
    ) -> list[str | None]:
        """Publica e espera confirmação. Retorna, por mensagem, ``None`` (ok) ou o erro."""
        ...


# Tempo máximo para o producer confirmar (ou desistir de) uma entrega. O lease do
# outbox relay precisa ser maior que isto.
DELIVERY_TIMEOUT_SECONDS = 30.0


def producer_config(bootstrap_servers: str, client_id: str) -> dict[str, object]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": client_id,
        # Produtor idempotente: sem duplicatas por retry interno e ordem preservada
        # por partição mesmo com vários requests em voo.
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "compression.type": "lz4",
        "linger.ms": 5,
        "delivery.timeout.ms": int(DELIVERY_TIMEOUT_SECONDS * 1000),
    }


class KafkaPublisher:
    def __init__(self, producer: Producer) -> None:
        self._producer = producer

    @classmethod
    def from_config(cls, bootstrap_servers: str, client_id: str) -> KafkaPublisher:
        return cls(Producer(producer_config(bootstrap_servers, client_id)))

    def publish_batch(
        self, messages: Sequence[OutgoingMessage], timeout: float
    ) -> list[str | None]:
        results: list[str | None] = ["delivery timeout"] * len(messages)

        def _callback(index: int):  # type: ignore[no-untyped-def]
            def on_delivery(err: KafkaError | None, _msg: Message) -> None:
                results[index] = str(err) if err is not None else None

            return on_delivery

        for i, m in enumerate(messages):
            try:
                self._producer.produce(
                    m.topic,
                    key=m.key.encode(),
                    value=m.value,
                    headers=list(m.headers.items()),
                    on_delivery=_callback(i),
                )
            except BufferError:
                self._producer.poll(0.5)
                try:
                    self._producer.produce(
                        m.topic,
                        key=m.key.encode(),
                        value=m.value,
                        headers=list(m.headers.items()),
                        on_delivery=_callback(i),
                    )
                except Exception as exc:  # fila local cheia de novo ou erro de config
                    results[i] = f"produce error: {exc}"
            except Exception as exc:
                results[i] = f"produce error: {exc}"
            self._producer.poll(0)
        self._producer.flush(timeout)
        return results

    def close(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)
