"""Contrato HTTP (FastAPI TestClient) com casos de uso e persistência reais."""

from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from transactions.adapters.http.app import create_app
from transactions.application.use_cases import CreateTransaction, GetTransaction
from transactions.domain.transaction import RiskDecision


@pytest.fixture
def client(create_tx: CreateTransaction, get_tx: GetTransaction) -> TestClient:
    return TestClient(
        create_app(create_tx, get_tx, readiness_check=lambda: None), raise_server_exceptions=False
    )


def test_create_returns_202_with_location(client: TestClient) -> None:
    r = client.post("/transactions", json={"customer_id": "123", "value": 1500.00})
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == "PENDING"
    assert body["value"] == 1500.0
    assert uuid.UUID(body["id"])
    assert r.headers["Location"] == f"/transactions/{body['id']}"
    assert r.headers["X-Correlation-ID"]


def test_get_returns_current_state(client: TestClient, make_processor) -> None:
    created = client.post("/transactions", json={"customer_id": "123", "value": 1500}).json()
    processor, _ = make_processor(RiskDecision.APPROVED)
    processor.execute(uuid.UUID(created["id"]), 1)

    r = client.get(f"/transactions/{created['id']}")
    assert r.status_code == 200
    assert {k: r.json()[k] for k in ("id", "customer_id", "value", "status")} == {
        "id": created["id"],
        "customer_id": "123",
        "value": 1500.0,
        "status": "APPROVED",
    }


def test_get_unknown_transaction_returns_404_problem(client: TestClient) -> None:
    r = client.get(f"/transactions/{uuid.uuid4()}")
    assert r.status_code == 404
    assert r.headers["content-type"].startswith("application/problem+json")
    assert r.json()["title"] == "Transação não encontrada"
    assert r.json()["correlation_id"]


def test_get_with_invalid_id_returns_422(client: TestClient) -> None:
    assert client.get("/transactions/not-a-uuid").status_code == 422


@pytest.mark.parametrize(
    "payload",
    [
        {"customer_id": "123"},
        {"customer_id": "", "value": 10},
        {"customer_id": "123", "value": 0},
        {"customer_id": "123", "value": -5},
        {"customer_id": "123", "value": 10.123},
        {"customer_id": "123", "value": "abc"},
        {"customer_id": "123", "value": 10, "unexpected": True},
    ],
)
def test_validation_errors(client: TestClient, payload: dict[str, object]) -> None:
    r = client.post("/transactions", json=payload)
    assert r.status_code == 422
    assert r.json()["errors"]


def test_idempotency_key(client: TestClient) -> None:
    headers = {"Idempotency-Key": "abc-123"}
    first = client.post("/transactions", json={"customer_id": "1", "value": 10}, headers=headers)
    again = client.post("/transactions", json={"customer_id": "1", "value": 10}, headers=headers)
    other = client.post("/transactions", json={"customer_id": "1", "value": 11}, headers=headers)

    assert first.status_code == 202
    assert again.status_code == 200 and again.headers["Idempotent-Replayed"] == "true"
    assert again.json()["id"] == first.json()["id"]
    assert other.status_code == 409


def test_correlation_id_is_propagated(client: TestClient) -> None:
    r = client.get(f"/transactions/{uuid.uuid4()}", headers={"X-Correlation-ID": "req-42"})
    assert r.headers["X-Correlation-ID"] == "req-42"
    assert r.json()["correlation_id"] == "req-42"


def test_unexpected_error_returns_500_problem(get_tx: GetTransaction) -> None:
    class Boom:
        def execute(self, _cmd):  # type: ignore[no-untyped-def]
            raise RuntimeError("db exploded")

    app = create_app(Boom(), get_tx, readiness_check=lambda: None)  # type: ignore[arg-type]
    r = TestClient(app, raise_server_exceptions=False).post(
        "/transactions", json={"customer_id": "1", "value": 1}
    )
    assert r.status_code == 500
    assert "db exploded" not in r.text  # não vaza detalhes internos


def test_health_and_metrics(create_tx: CreateTransaction, get_tx: GetTransaction) -> None:
    def not_ready() -> None:
        raise ConnectionError("mysql down")

    ok = TestClient(create_app(create_tx, get_tx, readiness_check=lambda: None))
    down = TestClient(create_app(create_tx, get_tx, readiness_check=not_ready))
    assert ok.get("/health/live").status_code == 200
    assert ok.get("/health/ready").status_code == 200
    assert down.get("/health/ready").status_code == 503
    assert "http_requests_total" in ok.get("/metrics").text


def test_get_always_reflects_latest_status(client: TestClient, make_processor) -> None:
    """Sem cache: logo após o worker gravar, o GET já mostra o status final."""
    created = client.post("/transactions", json={"customer_id": "1", "value": 1}).json()
    assert client.get(f"/transactions/{created['id']}").json()["status"] == "PENDING"
    processor, _ = make_processor(RiskDecision.REJECTED)
    processor.execute(uuid.UUID(created["id"]), 1)
    assert client.get(f"/transactions/{created['id']}").json()["status"] == "REJECTED"
