"""Outbox relay e handler do consumer, sem broker (publisher em memória)."""

from __future__ import annotations

import json
import uuid
from datetime import timedelta

from sqlalchemy import select

from tests.conftest import TRANSIENT, FakePublisher
from transactions.adapters.messaging.outbox_relay import OutboxRelay
from transactions.adapters.messaging.processing_consumer import (
    HandleResult,
    IncomingMessage,
    ProcessingMessageHandler,
)
from transactions.adapters.persistence.models import OutboxModel
from transactions.domain.transaction import RiskDecision


def as_incoming(msg, offset: int = 0) -> IncomingMessage:  # type: ignore[no-untyped-def]
    return IncomingMessage(msg.topic, 0, offset, msg.key.encode(), msg.value, msg.headers)


class TestOutboxRelay:
    def test_publishes_ready_events_once(self, new_transaction, session_factory):
        tx = new_transaction()
        publisher = FakePublisher()
        relay = OutboxRelay(session_factory, publisher)

        assert relay.run_once() == 2
        assert relay.run_once() == 0  # já publicados
        assert {m.topic for m in publisher.sent} == {"t.events", "t.processing"}
        assert all(m.key == str(tx.id) for m in publisher.sent)
        assert all("event_id" in m.headers for m in publisher.sent)

    def test_kafka_down_keeps_events_for_later(self, new_transaction, session_factory):
        """Cenário 1: MySQL ok, Kafka fora → nada se perde, publica quando voltar."""
        new_transaction()
        publisher = FakePublisher()
        publisher.fail_with = "Broker: transport failure"
        relay = OutboxRelay(session_factory, publisher)

        assert relay.run_once() == 0
        with session_factory() as s:
            rows = list(s.scalars(select(OutboxModel)))
        assert all(r.published_at is None and r.publish_attempts == 1 for r in rows)
        assert rows[0].last_error == "Broker: transport failure"

        publisher.fail_with = None  # Kafka voltou
        assert relay.run_once() == 2

    def test_delayed_retry_is_not_published_before_due(
        self, new_transaction, make_processor, session_factory, clock
    ):
        tx = new_transaction()
        processor, _ = make_processor(TRANSIENT)
        # Relógio "real" do relay é datetime.now(); o retry fica no futuro distante.
        clock.current = clock.current.replace(year=2999)
        processor.execute(tx.id, 1)

        publisher = FakePublisher()
        OutboxRelay(session_factory, publisher).run_once()
        attempts = [
            json.loads(m.value)["data"].get("attempt")
            for m in publisher.sent
            if m.topic == "t.processing"
        ]
        assert attempts == [1]  # tentativa 2 aguarda available_at

    def test_purge_removes_only_published(self, new_transaction, session_factory):
        from datetime import UTC, datetime

        new_transaction()
        relay = OutboxRelay(session_factory, FakePublisher())
        relay.run_once()
        new_transaction()  # ainda não publicado
        removed = relay.purge_published(datetime.now(UTC) + timedelta(seconds=1))
        assert removed == 2
        with session_factory() as s:
            assert len(list(s.scalars(select(OutboxModel)))) == 2

    def test_lag_metrics(self, new_transaction, session_factory):
        from transactions.observability import metrics

        new_transaction()
        OutboxRelay(session_factory, FakePublisher()).refresh_lag_metrics()
        assert metrics.OUTBOX_PENDING._value.get() == 2


class TestConsumerHandler:
    def _pipeline(self, new_transaction, session_factory, make_processor, *script):  # type: ignore[no-untyped-def]
        tx = new_transaction()
        out = FakePublisher()
        OutboxRelay(session_factory, out).run_once()
        [processing_msg] = [m for m in out.sent if m.topic == "t.processing"]
        processor, gateway = make_processor(*script)
        dlq = FakePublisher()
        handler = ProcessingMessageHandler(processor, dlq, "t.dlq")
        return tx, processing_msg, handler, gateway, dlq

    def test_end_to_end_success_and_duplicate(
        self, new_transaction, session_factory, make_processor, get_tx
    ):
        tx, msg, handler, gateway, dlq = self._pipeline(
            new_transaction, session_factory, make_processor, RiskDecision.APPROVED
        )
        assert handler.handle(as_incoming(msg)) is HandleResult.DONE
        assert handler.handle(as_incoming(msg)) is HandleResult.DONE  # reentrega
        assert get_tx.execute(tx.id).status == "APPROVED"
        assert len(gateway.calls) == 1
        assert dlq.sent == []

    def test_poison_message_goes_to_dlq(self, new_transaction, session_factory, make_processor):
        _, _msg, handler, gateway, dlq = self._pipeline(
            new_transaction, session_factory, make_processor, RiskDecision.APPROVED
        )
        bad = IncomingMessage("t.processing", 3, 42, b"k", b"{garbage", {"event_id": "x"})
        assert handler.handle(bad) is HandleResult.DONE  # não trava a partição
        [dead] = dlq.sent
        assert dead.topic == "t.dlq"
        assert dead.headers["dlq_reason"] == "poison"
        assert dead.headers["dlq_source_offset"] == "42"
        assert dead.value == b"{garbage"  # payload original preservado p/ análise
        assert gateway.calls == []

    def test_dlq_unavailable_does_not_lose_poison_message(
        self, new_transaction, session_factory, make_processor
    ):
        _, _, handler, _, dlq = self._pipeline(
            new_transaction, session_factory, make_processor, RiskDecision.APPROVED
        )
        dlq.fail_with = "broker down"
        bad = IncomingMessage("t.processing", 0, 1, b"k", b"{garbage", {})
        assert handler.handle(bad) is HandleResult.RETRY

    def test_infrastructure_error_asks_for_retry_without_commit(
        self, new_transaction, session_factory, make_processor
    ):
        _, msg, handler, _, _ = self._pipeline(
            new_transaction, session_factory, make_processor, RiskDecision.APPROVED
        )

        def db_down(*_args, **_kwargs):  # type: ignore[no-untyped-def]
            raise ConnectionError("MySQL server has gone away")

        handler._use_case.execute = db_down  # type: ignore[method-assign]
        assert handler.handle(as_incoming(msg)) is HandleResult.RETRY

    def test_orphan_message_goes_to_dlq(self, new_transaction, session_factory, make_processor):
        _, msg, handler, _, dlq = self._pipeline(
            new_transaction, session_factory, make_processor, RiskDecision.APPROVED
        )
        envelope = json.loads(msg.value)
        envelope["transaction_id"] = str(uuid.uuid4())
        orphan = IncomingMessage("t.processing", 0, 7, b"k", json.dumps(envelope).encode(), {})
        assert handler.handle(orphan) is HandleResult.DONE
        assert dlq.sent[0].headers["dlq_reason"] == "transaction_not_found"


def test_parallel_relays_never_publish_the_same_row(engine, session_factory, new_transaction):
    """SKIP LOCKED: duas instâncias do relay em paralelo dividem o trabalho.
    Só faz sentido em MySQL (SQLite não tem lock de linha)."""
    import threading

    import pytest

    if engine.dialect.name != "mysql":
        pytest.skip("requer MySQL (TEST_DATABASE_URL)")

    for _ in range(50):
        new_transaction()

    class SlowPublisher(FakePublisher):
        def publish_batch(self, messages, timeout):  # type: ignore[no-untyped-def]
            import time

            time.sleep(0.2)  # segura o lock enquanto o outro relay tenta
            return super().publish_batch(messages, timeout)

    p1, p2 = SlowPublisher(), SlowPublisher()
    r1 = OutboxRelay(session_factory, p1, batch_size=30)
    r2 = OutboxRelay(session_factory, p2, batch_size=30)

    def drain(relay):  # type: ignore[no-untyped-def]
        while relay.run_once():
            pass

    threads = [threading.Thread(target=drain, args=(r,)) for r in (r1, r2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ids = [m.headers["event_id"] for m in p1.sent + p2.sent]
    assert len(ids) == 100
    assert len(set(ids)) == 100  # nenhuma duplicata
    assert p1.sent and p2.sent  # ambos trabalharam
