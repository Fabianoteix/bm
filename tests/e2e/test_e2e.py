"""Testes ponta a ponta contra o ambiente real (docker compose).

    make up && make e2e

Exercitam MySQL, Kafka, relay, worker e o mock de risco juntos.
São pulados automaticamente se ``E2E_BASE_URL`` não estiver definido.
"""

from __future__ import annotations

import os
import time
import uuid

import httpx
import pytest

BASE_URL = os.getenv("E2E_BASE_URL")
RISK_URL = os.getenv("E2E_RISK_URL", "http://localhost:8081")

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(not BASE_URL, reason="E2E_BASE_URL não definido"),
]


@pytest.fixture(scope="module")
def api() -> httpx.Client:
    return httpx.Client(base_url=BASE_URL or "", timeout=10)


@pytest.fixture(autouse=True)
def calm_risk_service() -> None:
    """Desliga falhas aleatórias do mock para os testes serem determinísticos."""
    with httpx.Client(base_url=RISK_URL, timeout=5) as risk:
        risk.post("/admin/chaos", json={"failure_rate": 0, "slow_rate": 0})
        risk.delete("/admin/outage")


def wait_for_status(api: httpx.Client, tx_id: str, expected: set[str], timeout: float = 60) -> dict:
    deadline = time.monotonic() + timeout
    body: dict = {}
    while time.monotonic() < deadline:
        body = api.get(f"/transactions/{tx_id}").json()
        if body["status"] in expected:
            return body
        time.sleep(0.5)
    raise AssertionError(f"status final não atingido: {body}")


def test_approved_flow(api: httpx.Client) -> None:
    r = api.post("/transactions", json={"customer_id": "123", "value": 1500.00})
    assert r.status_code == 202
    assert wait_for_status(api, r.json()["id"], {"APPROVED"})["value"] == 1500.0


def test_rejected_flow(api: httpx.Client) -> None:
    r = api.post("/transactions", json={"customer_id": "123", "value": 50000})
    assert wait_for_status(api, r.json()["id"], {"REJECTED"})["status"] == "REJECTED"


def test_permanent_error_goes_to_failed(api: httpx.Client) -> None:
    r = api.post("/transactions", json={"customer_id": "bad-request", "value": 10})
    body = wait_for_status(api, r.json()["id"], {"FAILED"})
    assert "permanent" in body["last_error"]


def test_outage_then_recovery(api: httpx.Client) -> None:
    with httpx.Client(base_url=RISK_URL) as risk:
        risk.post("/admin/outage", json={"seconds": 3})
    r = api.post("/transactions", json={"customer_id": "123", "value": 10})
    body = wait_for_status(api, r.json()["id"], {"APPROVED"}, timeout=120)
    assert body["attempts"] >= 2


def test_not_found(api: httpx.Client) -> None:
    assert api.get(f"/transactions/{uuid.uuid4()}").status_code == 404
