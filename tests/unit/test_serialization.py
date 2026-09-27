import json
import uuid
from datetime import UTC, datetime

import pytest

from transactions.adapters.messaging.serialization import (
    PoisonMessage,
    deserialize_processing_message,
    serialize_event,
)
from transactions.domain.events import ProcessingRequested

NOW = datetime(2026, 9, 25, tzinfo=UTC)


def test_roundtrip_processing_requested() -> None:
    event = ProcessingRequested(transaction_id=uuid.uuid4(), attempt=3, occurred_at=NOW)
    msg = deserialize_processing_message(serialize_event(event))
    assert (msg.transaction_id, msg.attempt, msg.event_id) == (
        event.transaction_id,
        3,
        event.event_id,
    )


def test_envelope_contains_versioning_metadata() -> None:
    event = ProcessingRequested(transaction_id=uuid.uuid4(), attempt=1, occurred_at=NOW)
    envelope = json.loads(serialize_event(event))
    assert envelope["schema_version"] == 2
    assert envelope["event_type"] == "ProcessingRequested"
    assert envelope["source"] == "transactions-service"


def test_upcasts_v1_messages() -> None:
    """Mensagens antigas (v1, sem 'attempt') continuam sendo processadas."""
    v1 = {
        "event_id": str(uuid.uuid4()),
        "event_type": "ProcessingRequested",
        "schema_version": 1,
        "transaction_id": str(uuid.uuid4()),
        "data": {},
    }
    assert deserialize_processing_message(json.dumps(v1).encode()).attempt == 1


def test_tolerant_reader_ignores_unknown_fields() -> None:
    event = ProcessingRequested(transaction_id=uuid.uuid4(), attempt=1, occurred_at=NOW)
    envelope = json.loads(serialize_event(event))
    envelope["data"]["new_optional_field"] = "x"
    envelope["extra_top_level"] = True
    assert deserialize_processing_message(json.dumps(envelope).encode()).attempt == 1


@pytest.mark.parametrize(
    "raw",
    [
        None,
        b"",
        b"{not json",
        b'{"event_type": "ProcessingRequested"}',
        json.dumps(
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "Other",
                "schema_version": 1,
                "transaction_id": str(uuid.uuid4()),
                "data": {},
            }
        ).encode(),
        json.dumps(
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "ProcessingRequested",
                "schema_version": 99,
                "transaction_id": str(uuid.uuid4()),
                "data": {"attempt": 1},
            }
        ).encode(),
        json.dumps(
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "ProcessingRequested",
                "schema_version": 2,
                "transaction_id": "not-a-uuid",
                "data": {"attempt": 1},
            }
        ).encode(),
        json.dumps(
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "ProcessingRequested",
                "schema_version": 2,
                "transaction_id": str(uuid.uuid4()),
                "data": {"attempt": 0},
            }
        ).encode(),
    ],
)
def test_poison_messages(raw: bytes | None) -> None:
    with pytest.raises(PoisonMessage):
        deserialize_processing_message(raw)
