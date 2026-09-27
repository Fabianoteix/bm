"""Lease do outbox relay: o banco não fica travado enquanto o relay espera o Kafka."""

from __future__ import annotations

import threading
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import pytest
from prometheus_client import REGISTRY
from sqlalchemy import select, text

from tests.conftest import FakePublisher
from transactions.adapters.messaging.kafka_producer import OutgoingMessage
from transactions.adapters.messaging.outbox_relay import OutboxRelay
from transactions.adapters.persistence.models import OutboxModel


class MovableClock:
    def __init__(self) -> None:
        self.current = datetime.now(UTC)

    def __call__(self) -> datetime:
        return self.current


class CrashingPublisher(FakePublisher):
    """Simula o relay morrendo depois de reservar e antes de confirmar."""

    def publish_batch(
        self, messages: Sequence[OutgoingMessage], timeout: float
    ) -> list[str | None]:
        raise RuntimeError("relay morreu no meio da publicação")


def _rows(session_factory) -> list[OutboxModel]:  # type: ignore[no-untyped-def]
    with session_factory() as s:
        return list(s.scalars(select(OutboxModel).order_by(OutboxModel.id)))


def _reclaimed() -> float:
    return REGISTRY.get_sample_value("outbox_lease_reclaimed_total") or 0.0


def test_lease_is_committed_before_publishing(new_transaction, session_factory):
    """Durante a espera pelo Kafka a reserva já está commitada (visível de fora)
    e o relay não tem transação aberta."""
    new_transaction()
    seen: list[tuple[str | None, bool]] = []

    class Inspecting(FakePublisher):
        def publish_batch(self, messages, timeout):  # type: ignore[no-untyped-def]
            for row in _rows(session_factory):  # outra sessão, outra transação
                seen.append((row.locked_by, row.locked_until is not None))
            return super().publish_batch(messages, timeout)

    relay = OutboxRelay(session_factory, Inspecting(), relay_id="relay-a")
    assert relay.run_once() == 2
    assert seen == [("relay-a", True), ("relay-a", True)]
    assert all(
        r.published_at is not None and r.locked_by is None and r.locked_until is None
        for r in _rows(session_factory)
    )


def test_crashed_relay_lease_expires_and_another_relay_takes_over(new_transaction, session_factory):
    new_transaction()
    clock = MovableClock()
    crashed = OutboxRelay(
        session_factory, CrashingPublisher(), relay_id="relay-a", lease_seconds=45, clock=clock
    )
    with pytest.raises(RuntimeError):
        crashed.run_once()
    assert all(r.locked_by == "relay-a" for r in _rows(session_factory))

    survivor_publisher = FakePublisher()
    survivor = OutboxRelay(
        session_factory, survivor_publisher, relay_id="relay-b", lease_seconds=45, clock=clock
    )
    clock.current += timedelta(seconds=44)
    assert survivor.run_once() == 0  # lease ainda válido: ninguém rouba a linha em voo

    before = _reclaimed()
    clock.current += timedelta(seconds=2)  # 46 s: lease vencido
    assert survivor.run_once() == 2
    assert _reclaimed() == before + 2
    assert len(survivor_publisher.sent) == 2
    assert all(r.published_at is not None for r in _rows(session_factory))


def test_failed_publish_releases_lease_for_the_next_round(new_transaction, session_factory):
    new_transaction()
    publisher = FakePublisher()
    publisher.fail_with = "Broker: transport failure"
    relay = OutboxRelay(session_factory, publisher, relay_id="relay-a")

    assert relay.run_once() == 0
    rows = _rows(session_factory)
    assert all(r.locked_by is None and r.locked_until is None for r in rows)
    assert all(r.publish_attempts == 1 for r in rows)

    publisher.fail_with = None
    assert relay.run_once() == 2  # sem esperar o lease vencer


def test_failure_does_not_release_a_lease_taken_by_another_relay(new_transaction, session_factory):
    """Relay A travou além do lease; B reservou a linha. Quando A finalmente
    falha, ele não pode apagar a reserva de B."""
    new_transaction()
    clock = MovableClock()

    class SlowThenFails(FakePublisher):
        def publish_batch(self, messages, timeout):  # type: ignore[no-untyped-def]
            clock.current += timedelta(seconds=60)  # A "trava" além do lease
            OutboxRelay(
                session_factory, CrashingPublisher(), relay_id="relay-b", clock=clock
            )._claim()  # B reserva as linhas vencidas
            return ["timeout"] * len(messages)

    OutboxRelay(session_factory, SlowThenFails(), relay_id="relay-a", clock=clock).run_once()
    rows = _rows(session_factory)
    assert all(r.locked_by == "relay-b" for r in rows)
    assert all(r.publish_attempts == 0 for r in rows)


def test_lease_must_outlive_producer_delivery_timeout(session_factory):
    with pytest.raises(ValueError, match="lease_seconds"):
        OutboxRelay(session_factory, FakePublisher(), lease_seconds=10)


def test_slow_kafka_does_not_hold_row_locks(engine, new_transaction, session_factory):
    """No MySQL: enquanto o relay espera o Kafka, outra conexão consegue travar
    as mesmas linhas com NOWAIT. Antes do lease isso falhava na hora."""
    if engine.dialect.name != "mysql":
        pytest.skip("requer MySQL (TEST_DATABASE_URL)")
    new_transaction()
    locked_elsewhere: list[int] = []
    publishing = threading.Event()
    release = threading.Event()

    class SlowKafka(FakePublisher):
        def publish_batch(self, messages, timeout):  # type: ignore[no-untyped-def]
            publishing.set()
            release.wait(5)
            return super().publish_batch(messages, timeout)

    relay = OutboxRelay(session_factory, SlowKafka(), relay_id="relay-a")
    t = threading.Thread(target=relay.run_once)
    t.start()
    assert publishing.wait(5)
    with engine.connect() as conn, conn.begin():
        ids = conn.execute(text("SELECT id FROM outbox FOR UPDATE NOWAIT")).scalars().all()
        locked_elsewhere.extend(ids)
    release.set()
    t.join()
    assert len(locked_elsewhere) == 2
    assert all(r.published_at is not None for r in _rows(session_factory))
