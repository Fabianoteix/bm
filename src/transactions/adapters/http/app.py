"""Adapter de entrada HTTP (FastAPI).

Só traduz HTTP <-> casos de uso. Nenhuma regra de negócio mora aqui.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Annotated, Any

import structlog
from fastapi import FastAPI, Header, Request, Response, status
from fastapi.concurrency import run_in_threadpool
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from transactions.adapters.http.schemas import (
    CreateTransactionRequest,
    Problem,
    TransactionResponse,
)
from transactions.application.errors import IdempotencyConflict, TransactionNotFound
from transactions.application.use_cases import (
    CreateTransaction,
    CreateTransactionCommand,
    GetTransaction,
)
from transactions.domain.errors import DomainError
from transactions.observability import metrics

log = structlog.get_logger(__name__)

CORRELATION_HEADER = "X-Correlation-ID"


def _problem(
    status_code: int,
    title: str,
    detail: str | None = None,
    errors: list[dict[str, Any]] | None = None,
) -> JSONResponse:
    ctx = structlog.contextvars.get_contextvars()
    body = Problem(
        title=title,
        status=status_code,
        detail=detail,
        correlation_id=ctx.get("correlation_id"),
        errors=errors,
    )
    return JSONResponse(
        body.model_dump(exclude_none=True),
        status_code=status_code,
        media_type="application/problem+json",
    )


def create_app(
    create_transaction: CreateTransaction,
    get_transaction: GetTransaction,
    readiness_check: Callable[[], None],
) -> FastAPI:
    app = FastAPI(
        title="BAMAQ Capital — Transactions",
        version="1.0.0",
        description="Recebe transações e as processa de forma assíncrona (Kafka).",
    )

    # ------------------------------------------------------------ middleware
    @app.middleware("http")
    async def correlation_and_metrics(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        correlation_id = request.headers.get(CORRELATION_HEADER) or str(uuid.uuid4())
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)
        started = time.perf_counter()
        response: Response | None = None
        try:
            response = await call_next(request)
            return response
        finally:
            route = request.scope.get("route")
            route_path = getattr(route, "path", "unmatched")
            elapsed = time.perf_counter() - started
            status_code = response.status_code if response is not None else 500
            if response is not None:
                response.headers[CORRELATION_HEADER] = correlation_id
            metrics.HTTP_REQUESTS.labels(request.method, route_path, str(status_code)).inc()
            metrics.HTTP_LATENCY.labels(request.method, route_path).observe(elapsed)
            if not route_path.startswith(("/metrics", "/health")):
                log.info(
                    "http.request",
                    method=request.method,
                    path=route_path,
                    status=status_code,
                    duration_ms=round(elapsed * 1000, 2),
                    transaction_id=getattr(request.state, "transaction_id", None),
                )

    # -------------------------------------------------------- error handlers
    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [
            {"loc": list(e.get("loc", [])), "msg": e.get("msg"), "type": e.get("type")}
            for e in exc.errors()
        ]
        return _problem(422, "Requisição inválida", errors=errors)

    @app.exception_handler(DomainError)
    async def _domain(_: Request, exc: DomainError) -> JSONResponse:
        return _problem(422, "Regra de negócio violada", str(exc))

    @app.exception_handler(TransactionNotFound)
    async def _not_found(_: Request, exc: TransactionNotFound) -> JSONResponse:
        return _problem(404, "Transação não encontrada", f"id={exc}")

    @app.exception_handler(IdempotencyConflict)
    async def _conflict(_: Request, exc: IdempotencyConflict) -> JSONResponse:
        return _problem(409, "Conflito de idempotência", str(exc))

    @app.exception_handler(Exception)
    async def _unexpected(_: Request, exc: Exception) -> JSONResponse:
        log.exception("http.unhandled_error", error=str(exc))
        return _problem(
            500, "Erro interno", "Tente novamente; se persistir, informe o correlation_id."
        )

    # --------------------------------------------------------------- routes
    @app.post(
        "/transactions",
        status_code=status.HTTP_202_ACCEPTED,
        response_model=TransactionResponse,
        responses={
            200: {"description": "Replay idempotente"},
            409: {"model": Problem},
            422: {"model": Problem},
        },
    )
    async def post_transaction(
        body: CreateTransactionRequest,
        request: Request,
        response: Response,
        idempotency_key: Annotated[
            str | None, Header(alias="Idempotency-Key", max_length=128)
        ] = None,
    ) -> TransactionResponse:
        result = await run_in_threadpool(
            create_transaction.execute,
            CreateTransactionCommand(body.customer_id, body.value, idempotency_key),
        )
        request.state.transaction_id = str(result.transaction.id)
        metrics.TRANSACTIONS_CREATED.labels(replayed=str(not result.created).lower()).inc()
        response.headers["Location"] = f"/transactions/{result.transaction.id}"
        if not result.created:
            response.status_code = status.HTTP_200_OK
            response.headers["Idempotent-Replayed"] = "true"
        return TransactionResponse.from_view(result.transaction)

    @app.get(
        "/transactions/{transaction_id}",
        response_model=TransactionResponse,
        responses={404: {"model": Problem}},
    )
    async def get_transaction_by_id(
        transaction_id: uuid.UUID, request: Request
    ) -> TransactionResponse:
        request.state.transaction_id = str(transaction_id)
        structlog.contextvars.bind_contextvars(transaction_id=str(transaction_id))
        view = await run_in_threadpool(get_transaction.execute, transaction_id)
        return TransactionResponse.from_view(view)

    @app.get("/health/live", include_in_schema=False)
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", include_in_schema=False)
    async def ready() -> JSONResponse:
        # Kafka NÃO entra na prontidão da API: graças ao outbox, a API continua
        # aceitando transações mesmo com o broker fora (Cenário 1).
        try:
            await run_in_threadpool(readiness_check)
        except Exception as exc:
            return JSONResponse({"status": "unavailable", "error": str(exc)}, status_code=503)
        return JSONResponse({"status": "ok"})

    @app.get("/metrics", include_in_schema=False)
    async def prometheus_metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app
