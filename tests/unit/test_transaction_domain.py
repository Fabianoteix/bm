from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from transactions.domain.errors import InvalidStateTransition, InvalidTransaction
from transactions.domain.events import (
    ProcessingRequested,
    TransactionCreated,
    TransactionDeadLettered,
    TransactionStatusChanged,
)
from transactions.domain.transaction import RiskDecision, Transaction, TransactionStatus

NOW = datetime(2026, 9, 25, tzinfo=UTC)


def make() -> Transaction:
    tx = Transaction.create(customer_id="123", value=Decimal("1500.00"), now=NOW)
    tx.pull_events()
    return tx


class TestCreation:
    def test_starts_pending_and_emits_created_and_processing_requested(self) -> None:
        tx = Transaction.create(customer_id=" 123 ", value="1500", now=NOW)
        events = tx.pull_events()

        assert tx.status is TransactionStatus.PENDING
        assert tx.customer_id == "123"
        assert tx.value == Decimal("1500.00")
        assert [type(e) for e in events] == [TransactionCreated, ProcessingRequested]
        assert events[1].attempt == 1  # type: ignore[attr-defined]
        assert tx.pull_events() == []  # eventos são consumidos uma única vez

    @pytest.mark.parametrize(
        ("value", "message"),
        [
            ("0", "maior que zero"),
            ("-10", "maior que zero"),
            ("10.001", "2 casas"),
            ("abc", "numérico"),
            ("NaN", "finito"),
            ("1000000000000", "no máximo"),
        ],
    )
    def test_rejects_invalid_values(self, value: str, message: str) -> None:
        with pytest.raises(InvalidTransaction, match=message):
            Transaction.create(customer_id="1", value=value, now=NOW)

    @pytest.mark.parametrize("customer_id", ["", "   ", "x" * 65])
    def test_rejects_invalid_customer(self, customer_id: str) -> None:
        with pytest.raises(InvalidTransaction):
            Transaction.create(customer_id=customer_id, value="10", now=NOW)


class TestAttemptFencing:
    def test_accepts_only_next_attempt(self) -> None:
        tx = make()
        assert tx.accepts_attempt(1)
        assert not tx.accepts_attempt(2)
        assert not tx.accepts_attempt(0)

    def test_duplicate_attempt_is_rejected_after_retry_scheduled(self) -> None:
        tx = make()
        tx.start_attempt(1)
        tx.schedule_retry(error="503", now=NOW, retry_at=NOW + timedelta(seconds=5))
        assert not tx.accepts_attempt(1)  # reentrega da tentativa 1
        assert tx.accepts_attempt(2)

    def test_final_status_accepts_nothing(self) -> None:
        tx = make()
        tx.start_attempt(1)
        tx.apply_decision(RiskDecision.APPROVED, NOW)
        assert not tx.accepts_attempt(1)
        assert not tx.accepts_attempt(2)
        with pytest.raises(InvalidStateTransition):
            tx.start_attempt(2)


class TestTransitions:
    def test_approval_emits_status_changed_with_monotonic_version(self) -> None:
        tx = make()
        tx.start_attempt(1)
        tx.apply_decision(RiskDecision.APPROVED, NOW)
        [event] = tx.pull_events()
        assert isinstance(event, TransactionStatusChanged)
        assert (event.previous_status, event.status, event.version) == ("PENDING", "APPROVED", 2)

    def test_retry_emits_delayed_processing_request(self) -> None:
        tx = make()
        tx.start_attempt(1)
        retry_at = NOW + timedelta(seconds=5)
        tx.schedule_retry(error="timeout", now=NOW, retry_at=retry_at)
        status_changed, retry = tx.pull_events()
        assert isinstance(status_changed, TransactionStatusChanged)
        assert isinstance(retry, ProcessingRequested)
        assert (retry.attempt, retry.not_before) == (2, retry_at)
        assert tx.last_error == "timeout"

    def test_second_retry_does_not_repeat_status_event(self) -> None:
        tx = make()
        tx.start_attempt(1)
        tx.schedule_retry(error="e", now=NOW, retry_at=NOW)
        tx.pull_events()
        tx.start_attempt(2)
        tx.schedule_retry(error="e", now=NOW, retry_at=NOW)
        assert [type(e) for e in tx.pull_events()] == [ProcessingRequested]

    def test_fail_emits_dead_letter(self) -> None:
        tx = make()
        tx.start_attempt(1)
        tx.fail(error="x" * 1000, now=NOW)
        events = tx.pull_events()
        assert tx.status is TransactionStatus.FAILED
        assert isinstance(events[-1], TransactionDeadLettered)
        assert len(tx.last_error or "") == 500

    def test_final_states_are_immutable(self) -> None:
        tx = make()
        tx.start_attempt(1)
        tx.apply_decision(RiskDecision.REJECTED, NOW)
        with pytest.raises(InvalidStateTransition):
            tx.fail(error="late", now=NOW)

    def test_reprocess_only_from_failed(self) -> None:
        tx = make()
        with pytest.raises(InvalidStateTransition):
            tx.reset_for_reprocessing(NOW)
        tx.start_attempt(1)
        tx.fail(error="boom", now=NOW)
        tx.pull_events()
        tx.reset_for_reprocessing(NOW)
        assert (tx.status, tx.attempts, tx.last_error) == (TransactionStatus.PENDING, 0, None)
        assert isinstance(tx.pull_events()[-1], ProcessingRequested)
