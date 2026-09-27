"""Adapter HTTP do serviço de risco — usando httpx.MockTransport (sem rede)."""

import uuid
from collections.abc import Callable
from decimal import Decimal

import httpx
import pytest

from transactions.adapters.risk.circuit_breaker import CircuitBreaker, CircuitState
from transactions.adapters.risk.http_client import HttpRiskAnalysisClient
from transactions.application.errors import RiskServiceError, RiskServiceUnavailable
from transactions.application.ports import RiskAnalysisRequest
from transactions.domain.transaction import RiskDecision

REQ = RiskAnalysisRequest(uuid.uuid4(), "123", Decimal("1500.00"))
Handler = Callable[[httpx.Request], httpx.Response]


def client_for(
    handler: Handler, *, retries: int = 2, breaker: CircuitBreaker | None = None
) -> tuple[HttpRiskAnalysisClient, list[float]]:
    sleeps: list[float] = []
    http = httpx.Client(base_url="http://risk", transport=httpx.MockTransport(handler))
    return (
        HttpRiskAnalysisClient(
            http,
            breaker or CircuitBreaker(failure_threshold=100),
            inline_retries=retries,
            sleep=sleeps.append,
        ),
        sleeps,
    )


def sequence(*responses: httpx.Response | Exception) -> Handler:
    items = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        item = items.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    return handler


def test_approved_and_sends_idempotency_key() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"result": "APPROVED"})

    client, _ = client_for(handler)
    assert client.analyze(REQ) is RiskDecision.APPROVED
    assert seen[0].headers["Idempotency-Key"] == str(REQ.transaction_id)
    assert seen[0].url.path == "/risk-analysis"


def test_rejected() -> None:
    client, _ = client_for(lambda r: httpx.Response(200, json={"result": "REJECTED"}))
    assert client.analyze(REQ) is RiskDecision.REJECTED


def test_inline_retry_recovers_from_blip() -> None:
    client, sleeps = client_for(
        sequence(
            httpx.Response(503),
            httpx.ReadTimeout("slow"),
            httpx.Response(200, json={"result": "APPROVED"}),
        )
    )
    assert client.analyze(REQ) is RiskDecision.APPROVED
    assert len(sleeps) == 2


@pytest.mark.parametrize(
    "failure",
    [
        httpx.Response(500),
        httpx.Response(429),
        httpx.Response(408),
        httpx.ConnectError("refused"),
        httpx.ReadTimeout("slow"),
    ],
)
def test_transient_failures_raise_unavailable(failure: httpx.Response | Exception) -> None:
    client, _ = client_for(sequence(failure), retries=0)
    with pytest.raises(RiskServiceUnavailable):
        client.analyze(REQ)


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(400, text="bad"),
        httpx.Response(200, json={"result": "MAYBE"}),
        httpx.Response(200, text="not json"),
    ],
)
def test_permanent_failures_do_not_retry(response: httpx.Response) -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return response

    client, _ = client_for(handler)
    with pytest.raises(RiskServiceError) as exc:
        client.analyze(REQ)
    assert not isinstance(exc.value, RiskServiceUnavailable)
    assert len(calls) == 1


def test_open_circuit_fails_fast_without_calling_service() -> None:
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout=60)
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(503)

    client, _ = client_for(handler, retries=5, breaker=breaker)
    with pytest.raises(RiskServiceUnavailable, match="circuit breaker"):
        client.analyze(REQ)
    assert len(calls) == 2
    assert breaker.state is CircuitState.OPEN
