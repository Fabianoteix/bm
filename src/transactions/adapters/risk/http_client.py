"""Adapter HTTP do serviço de análise de risco.

Classificação de falhas (decide entre retry e falha definitiva):

=====================================  ===========================
Situação                                Classificação
=====================================  ===========================
timeout, conexão recusada, DNS          transitória
HTTP 5xx, 408, 429                      transitória
circuit breaker aberto                  transitória (fail fast)
HTTP 4xx (demais)                       permanente
200 com corpo fora do contrato          permanente
=====================================  ===========================

Dentro de uma tentativa há poucos retries curtos (ms) para absorver "soluços";
indisponibilidades longas viram retry *agendado* na camada de aplicação.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable

import httpx
import structlog

from transactions.adapters.risk.circuit_breaker import CircuitBreaker
from transactions.application.errors import RiskServiceError, RiskServiceUnavailable
from transactions.application.ports import RiskAnalysisRequest
from transactions.domain.transaction import RiskDecision

log = structlog.get_logger(__name__)

TRANSIENT_STATUS = {408, 429}


class HttpRiskAnalysisClient:
    def __init__(
        self,
        client: httpx.Client,
        breaker: CircuitBreaker,
        *,
        inline_retries: int = 2,
        inline_backoff_seconds: float = 0.2,
        sleep: Callable[[float], None] = time.sleep,
        observe_latency: Callable[[str, float], None] | None = None,
    ) -> None:
        self._client = client
        self._breaker = breaker
        self._inline_retries = inline_retries
        self._backoff = inline_backoff_seconds
        self._sleep = sleep
        self._observe = observe_latency or (lambda _outcome, _seconds: None)

    def analyze(self, request: RiskAnalysisRequest) -> RiskDecision:
        last_error = "unknown"
        for try_number in range(self._inline_retries + 1):
            if not self._breaker.allow_request():
                raise RiskServiceUnavailable("circuit breaker aberto para o serviço de risco")
            try:
                return self._call_once(request)
            except RiskServiceUnavailable as exc:
                last_error = str(exc)
                self._breaker.record_failure()
                if try_number < self._inline_retries:
                    delay = self._backoff * (2**try_number) * random.uniform(0.5, 1.5)  # noqa: S311
                    log.info("risk.inline_retry", try_number=try_number + 1, error=last_error)
                    self._sleep(delay)
        raise RiskServiceUnavailable(last_error)

    def _call_once(self, request: RiskAnalysisRequest) -> RiskDecision:
        started = time.perf_counter()
        ctx = structlog.contextvars.get_contextvars()
        headers = {"Idempotency-Key": str(request.transaction_id)}
        if ctx.get("correlation_id"):
            headers["X-Correlation-ID"] = str(ctx["correlation_id"])
        try:
            response = self._client.post(
                "/risk-analysis",
                json={"customer_id": request.customer_id, "value": float(request.value)},
                headers=headers,
            )
        except httpx.TimeoutException as exc:
            self._observe("timeout", time.perf_counter() - started)
            raise RiskServiceUnavailable(f"timeout: {type(exc).__name__}") from exc
        except httpx.TransportError as exc:
            self._observe("transport_error", time.perf_counter() - started)
            raise RiskServiceUnavailable(f"erro de transporte: {type(exc).__name__}") from exc

        elapsed = time.perf_counter() - started
        status = response.status_code
        if status >= 500 or status in TRANSIENT_STATUS:
            self._observe(f"http_{status}", elapsed)
            raise RiskServiceUnavailable(f"HTTP {status}")

        # Serviço respondeu: está vivo, independentemente do conteúdo.
        self._breaker.record_success()
        if status >= 400:
            self._observe(f"http_{status}", elapsed)
            raise RiskServiceError(f"HTTP {status}: {response.text[:200]}")

        try:
            decision = RiskDecision(response.json()["result"])
        except (ValueError, KeyError, TypeError) as exc:
            self._observe("invalid_contract", elapsed)
            raise RiskServiceError(f"resposta fora do contrato: {response.text[:200]}") from exc
        self._observe("ok", elapsed)
        return decision
