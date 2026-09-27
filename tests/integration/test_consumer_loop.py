"""Loop do consumer Kafka com um ``Consumer`` falso (semântica de offsets)."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from transactions.adapters.messaging.processing_consumer import (
    HandleResult,
    IncomingMessage,
    KafkaProcessingConsumer,
)


@dataclass
class FakeMsg:
    _offset: int
    _value: bytes = b"{}"

    def error(self) -> None:
        return None

    def topic(self) -> str:
        return "t.processing"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes:
        return b"k"

    def value(self) -> bytes:
        return self._value

    def headers(self) -> list[tuple[str, bytes]]:
        return [("event_id", b"e-1")]


@dataclass
class FakeConsumer:
    queue: list[FakeMsg]
    stop: threading.Event
    stored: list[int] = field(default_factory=list)
    seeks: list[int] = field(default_factory=list)
    closed: bool = False
    paused: bool = False
    pause_calls: int = 0
    resume_calls: int = 0
    polls_while_paused: int = 0

    def assignment(self) -> list[str]:
        return ["t.processing[0]"]

    def pause(self, partitions: list[str]) -> None:
        self.paused = True
        self.pause_calls += 1

    def resume(self, partitions: list[str]) -> None:
        self.paused = False
        self.resume_calls += 1

    def subscribe(self, topics: list[str]) -> None:
        pass

    def poll(self, timeout: float) -> FakeMsg | None:
        if self.paused:  # partição pausada: o broker não entrega nada
            self.polls_while_paused += 1
            return None
        if not self.queue:
            self.stop.set()
            return None
        return self.queue.pop(0)

    def store_offsets(self, message: FakeMsg) -> None:
        self.stored.append(message.offset())

    def seek(self, tp) -> None:  # type: ignore[no-untyped-def]
        self.seeks.append(tp.offset)
        self.queue.insert(0, FakeMsg(tp.offset))  # reentrega

    def close(self) -> None:
        self.closed = True


class ScriptedHandler:
    def __init__(self, *results: HandleResult) -> None:
        self.results = list(results)
        self.seen: list[IncomingMessage] = []

    def handle(self, msg: IncomingMessage) -> HandleResult:
        self.seen.append(msg)
        return self.results.pop(0)


def run(consumer: FakeConsumer, handler: ScriptedHandler, should_pause=lambda: False) -> None:  # type: ignore[no-untyped-def]
    KafkaProcessingConsumer(
        consumer,  # type: ignore[arg-type]
        "t.processing",
        handler,  # type: ignore[arg-type]
        max_retry_delay=0.0,
        should_pause=should_pause,
    ).run(consumer.stop)


def test_offset_is_stored_only_after_successful_handling() -> None:
    stop = threading.Event()
    consumer = FakeConsumer([FakeMsg(10), FakeMsg(11)], stop)
    handler = ScriptedHandler(HandleResult.DONE, HandleResult.DONE)
    run(consumer, handler)

    assert consumer.stored == [10, 11]
    assert handler.seen[0].headers == {"event_id": "e-1"}
    assert consumer.closed  # close() commita offsets armazenados (graceful shutdown)


def test_retry_result_seeks_back_and_redelivers_same_offset() -> None:
    """Falha de infraestrutura: o offset NÃO avança; a mesma mensagem volta."""
    stop = threading.Event()
    consumer = FakeConsumer([FakeMsg(10), FakeMsg(11)], stop)
    handler = ScriptedHandler(HandleResult.RETRY, HandleResult.DONE, HandleResult.DONE)
    run(consumer, handler)

    assert consumer.seeks == [10]
    assert [m.offset for m in handler.seen] == [10, 10, 11]
    assert consumer.stored == [10, 11]


def test_pauses_while_circuit_open_and_resumes_after() -> None:
    """Backpressure: com o circuito aberto nada é consumido; ao fechar, retoma."""
    stop = threading.Event()
    consumer = FakeConsumer([FakeMsg(10), FakeMsg(11)], stop)
    handler = ScriptedHandler(HandleResult.DONE, HandleResult.DONE)

    open_for = {"polls": 3}  # circuito "aberto" durante as 3 primeiras voltas

    def circuit_open() -> bool:
        if open_for["polls"] > 0:
            open_for["polls"] -= 1
            return True
        return False

    run(consumer, handler, should_pause=circuit_open)

    assert consumer.polls_while_paused == 3  # continuou chamando poll (fica no grupo)
    assert consumer.resume_calls == 1
    assert [m.offset for m in handler.seen] == [10, 11]  # nada perdido nem pulado
    assert consumer.stored == [10, 11]


def test_no_pause_when_circuit_closed() -> None:
    stop = threading.Event()
    consumer = FakeConsumer([FakeMsg(1)], stop)
    run(consumer, ScriptedHandler(HandleResult.DONE))
    assert consumer.pause_calls == 0 and consumer.resume_calls == 0
