"""Casos de uso + persistência real (SQLAlchemy/SQLite) + outbox.

Cobre os comportamentos pedidos no enunciado: sucesso, duplicidade, falha
temporária, falha definitiva após N tentativas e reprocessamento.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from tests.conftest import PERMANENT, TRANSIENT, FakeClock
from transactions.adapters.persistence.models import OutboxModel
from transactions.application.errors import (
    IdempotencyConflict,
    NotReprocessable,
    TransactionNotFound,
)
from transactions.application.ports import UnitOfWork
from transactions.application.use_cases import (
    CreateTransactionCommand,
    GetTransaction,
    ProcessingOutcome,
    ReprocessTransaction,
)
from transactions.domain.transaction import RiskDecision

APPROVED, REJECTED = RiskDecision.APPROVED, RiskDecision.REJECTED


def outbox_rows(session_factory: sessionmaker[Session]) -> list[OutboxModel]:
    with session_factory() as s:
        return list(s.scalars(select(OutboxModel).order_by(OutboxModel.id)))


def status_of(get_tx: GetTransaction, tx_id) -> str:  # type: ignore[no-untyped-def]
    return get_tx.execute(tx_id).status


# --------------------------------------------------------------- criação
class TestCreate:
    def test_persists_transaction_and_outbox_atomically(self, new_transaction, session_factory):
        tx = new_transaction()
        rows = outbox_rows(session_factory)

        assert tx.status == "PENDING"
        assert [(r.event_type, r.topic) for r in rows] == [
            ("TransactionCreated", "t.events"),
            ("ProcessingRequested", "t.processing"),
        ]
        assert all(r.message_key == str(tx.id) for r in rows)  # key = ordenação por transação
        assert all(r.published_at is None for r in rows)

    def test_idempotency_key_replays_same_transaction(self, create_tx, session_factory):
        cmd = CreateTransactionCommand("123", __import__("decimal").Decimal("10.00"), "key-1")
        first = create_tx.execute(cmd)
        second = create_tx.execute(cmd)

        assert first.created and not second.created
        assert first.transaction.id == second.transaction.id
        assert len(outbox_rows(session_factory)) == 2  # nenhum evento duplicado

    def test_idempotency_key_with_different_payload_conflicts(self, new_transaction):
        new_transaction(key="key-1", value="10.00")
        with pytest.raises(IdempotencyConflict):
            new_transaction(key="key-1", value="99.00")


# ------------------------------------------------------------ processamento
class TestProcess:
    def test_success_flow_approved(self, new_transaction, make_processor, get_tx, session_factory):
        tx = new_transaction()
        processor, gateway = make_processor(APPROVED)

        assert processor.execute(tx.id, 1) is ProcessingOutcome.APPROVED
        assert status_of(get_tx, tx.id) == "APPROVED"
        assert len(gateway.calls) == 1

        status_events = [
            r for r in outbox_rows(session_factory) if r.event_type == "TransactionStatusChanged"
        ]
        payload = json.loads(status_events[0].payload)
        assert payload["data"]["status"] == "APPROVED"
        assert payload["data"]["version"] == 2

    def test_rejected(self, new_transaction, make_processor, get_tx):
        tx = new_transaction()
        processor, _ = make_processor(REJECTED)
        assert processor.execute(tx.id, 1) is ProcessingOutcome.REJECTED
        assert status_of(get_tx, tx.id) == "REJECTED"

    def test_duplicate_message_is_ignored(self, new_transaction, make_processor, session_factory):
        """Cenário 4: a mesma mensagem entregue duas vezes."""
        tx = new_transaction()
        processor, gateway = make_processor(APPROVED)

        assert processor.execute(tx.id, 1) is ProcessingOutcome.APPROVED
        rows_before = len(outbox_rows(session_factory))
        assert processor.execute(tx.id, 1) is ProcessingOutcome.DUPLICATE

        assert len(gateway.calls) == 1  # serviço externo NÃO chamado de novo
        assert len(outbox_rows(session_factory)) == rows_before  # nenhum evento novo

    def test_redelivery_after_crash_before_commit_is_processed(
        self, new_transaction, make_processor, get_tx
    ):
        """Cenário 2 (variante): o crash foi ANTES do commit no banco — nada foi
        gravado, então a reentrega precisa ser processada normalmente."""
        tx = new_transaction()
        processor, _ = make_processor(APPROVED)
        # nenhuma chamada anterior persistiu nada; reentrega da tentativa 1:
        assert processor.execute(tx.id, 1) is ProcessingOutcome.APPROVED
        assert status_of(get_tx, tx.id) == "APPROVED"

    def test_transient_failure_schedules_delayed_retry(
        self, new_transaction, make_processor, get_tx, session_factory, clock: FakeClock
    ):
        tx = new_transaction()
        processor, _ = make_processor(TRANSIENT)

        assert processor.execute(tx.id, 1) is ProcessingOutcome.RETRY_SCHEDULED
        view = get_tx.execute(tx.id)
        assert (view.status, view.attempts, view.last_error) == ("RETRYING", 1, "HTTP 503")

        retry = [r for r in outbox_rows(session_factory) if r.event_type == "ProcessingRequested"][
            -1
        ]
        assert json.loads(retry.payload)["data"]["attempt"] == 2
        assert retry.available_at == clock.now() + timedelta(seconds=5)  # backoff

    def test_transient_then_success(self, new_transaction, make_processor, get_tx):
        """Falha temporária do serviço externo com recuperação."""
        tx = new_transaction()
        processor, gateway = make_processor(TRANSIENT, TRANSIENT, APPROVED)

        assert processor.execute(tx.id, 1) is ProcessingOutcome.RETRY_SCHEDULED
        assert processor.execute(tx.id, 2) is ProcessingOutcome.RETRY_SCHEDULED
        assert processor.execute(tx.id, 3) is ProcessingOutcome.APPROVED
        assert status_of(get_tx, tx.id) == "APPROVED"
        assert len(gateway.calls) == 3

    def test_stale_retry_message_is_ignored(self, new_transaction, make_processor):
        tx = new_transaction()
        processor, gateway = make_processor(TRANSIENT, APPROVED)
        processor.execute(tx.id, 1)
        processor.execute(tx.id, 2)
        assert processor.execute(tx.id, 2) is ProcessingOutcome.DUPLICATE
        assert processor.execute(tx.id, 1) is ProcessingOutcome.DUPLICATE
        assert len(gateway.calls) == 2

    def test_definitive_failure_after_max_attempts(
        self, new_transaction, make_processor, get_tx, session_factory
    ):
        """Falha definitiva após múltiplas tentativas → FAILED + DLQ."""
        tx = new_transaction()
        processor, gateway = make_processor(TRANSIENT)  # max_attempts=3 na fixture

        outcomes = [processor.execute(tx.id, n) for n in (1, 2, 3)]
        assert outcomes == [ProcessingOutcome.RETRY_SCHEDULED] * 2 + [ProcessingOutcome.FAILED]

        view = get_tx.execute(tx.id)
        assert view.status == "FAILED"
        assert view.last_error is not None and "max attempts" in view.last_error
        dlq = [r for r in outbox_rows(session_factory) if r.topic == "t.dlq"]
        assert len(dlq) == 1 and dlq[0].event_type == "TransactionDeadLettered"
        assert processor.execute(tx.id, 4) is ProcessingOutcome.DUPLICATE
        assert len(gateway.calls) == 3

    def test_permanent_error_fails_immediately(self, new_transaction, make_processor, get_tx):
        tx = new_transaction()
        processor, gateway = make_processor(PERMANENT)
        assert processor.execute(tx.id, 1) is ProcessingOutcome.FAILED
        assert status_of(get_tx, tx.id) == "FAILED"
        assert len(gateway.calls) == 1

    def test_concurrent_consumers_only_one_wins(
        self, new_transaction, make_processor, uow_factory: Callable[[], UnitOfWork], get_tx
    ):
        """Rebalance: dois consumers leram a mesma versão. O lock otimista garante
        que só um grava; o outro desiste sem efeito colateral."""
        tx = new_transaction()
        slow, _ = make_processor(APPROVED)
        fast, _ = make_processor(REJECTED)

        original_analyze = slow._risk.analyze  # type: ignore[attr-defined]

        def analyze_and_let_other_win(request):  # type: ignore[no-untyped-def]
            assert fast.execute(tx.id, 1) is ProcessingOutcome.REJECTED
            return original_analyze(request)

        slow._risk.analyze = analyze_and_let_other_win  # type: ignore[attr-defined]
        assert slow.execute(tx.id, 1) is ProcessingOutcome.CONCURRENT_UPDATE
        assert status_of(get_tx, tx.id) == "REJECTED"

    def test_unknown_transaction(self, make_processor):
        import uuid

        processor, gateway = make_processor(APPROVED)
        assert processor.execute(uuid.uuid4(), 1) is ProcessingOutcome.NOT_FOUND
        assert gateway.calls == []


# ------------------------------------------------------------ consulta
class TestGet:
    def test_not_found(self, get_tx):
        import uuid

        with pytest.raises(TransactionNotFound):
            get_tx.execute(uuid.uuid4())

    def test_reads_current_state_from_database(self, new_transaction, get_tx):
        tx = new_transaction()
        assert get_tx.execute(tx.id).status == "PENDING"


# ------------------------------------------------------------ reprocessamento
class TestReprocess:
    def test_failed_transaction_can_be_reprocessed(
        self, new_transaction, make_processor, reprocess_tx: ReprocessTransaction, get_tx
    ):
        tx = new_transaction()
        failing, _ = make_processor(PERMANENT)
        failing.execute(tx.id, 1)

        reprocess_tx.execute(tx.id)
        assert get_tx.execute(tx.id).status == "PENDING"

        healthy, _ = make_processor(APPROVED)
        assert healthy.execute(tx.id, 1) is ProcessingOutcome.APPROVED

    def test_only_failed_can_be_reprocessed(self, new_transaction, reprocess_tx):
        tx = new_transaction()
        with pytest.raises(NotReprocessable):
            reprocess_tx.execute(tx.id)

    def test_reprocess_all_failed(self, new_transaction, make_processor, reprocess_tx):
        failing, _ = make_processor(PERMANENT)
        ids = []
        for _ in range(3):
            tx = new_transaction()
            failing.execute(tx.id, 1)
            ids.append(tx.id)
        assert sorted(reprocess_tx.execute_all_failed()) == sorted(ids)


class TestIdempotencyRace:
    def test_unique_constraint_resolves_concurrent_requests(self, create_tx, session_factory):
        """Duas requisições com a mesma chave passam juntas pelo lookup: a
        constraint UNIQUE escolhe a vencedora e a perdedora devolve a mesma."""
        from decimal import Decimal

        cmd = CreateTransactionCommand("123", Decimal("10.00"), "race-key")
        winner = create_tx.execute(cmd).transaction

        original = create_tx._find_existing
        calls = {"n": 0}

        def miss_first_time(c):  # type: ignore[no-untyped-def]
            calls["n"] += 1
            return None if calls["n"] == 1 else original(c)

        create_tx._find_existing = miss_first_time
        loser = create_tx.execute(cmd)

        assert not loser.created
        assert loser.transaction.id == winner.id
        assert len(outbox_rows(session_factory)) == 2


class TestIdempotencyKeyPerCustomer:
    """Evolução: a Idempotency-Key é única POR CLIENTE, não globalmente."""

    def test_same_key_different_customers_are_independent(self, new_transaction):
        a = new_transaction(customer_id="cliente-a", key="pedido-1", value="10.00")
        b = new_transaction(customer_id="cliente-b", key="pedido-1", value="99.00")
        assert a.id != b.id  # antes: o cliente B tomava 409

    def test_same_key_same_customer_still_replays(self, create_tx):
        from decimal import Decimal

        cmd = CreateTransactionCommand("cliente-a", Decimal("10.00"), "pedido-1")
        first, again = create_tx.execute(cmd), create_tx.execute(cmd)
        assert again.transaction.id == first.transaction.id and not again.created

    def test_same_key_same_customer_other_payload_conflicts(self, new_transaction):
        new_transaction(customer_id="cliente-a", key="pedido-1", value="10.00")
        with pytest.raises(IdempotencyConflict):
            new_transaction(customer_id="cliente-a", key="pedido-1", value="11.00")
