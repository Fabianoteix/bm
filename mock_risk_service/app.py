"""Mock do serviço externo de análise de risco — com injeção de falhas.

Regras de negócio (determinísticas, facilitam testes manuais):
* value > REJECT_ABOVE (default 10000)  → REJECTED
* customer_id começando com "blocked"   → REJECTED
* caso contrário                         → APPROVED

Injeção de falhas (env vars ou em runtime via /admin/chaos):
* FAILURE_RATE   probabilidade de responder 503        (default 0.1)
* SLOW_RATE      probabilidade de responder devagar    (default 0.1)
* SLOW_SECONDS   atraso da resposta lenta              (default 5 — maior que o timeout do cliente)
* POST /admin/outage {"seconds": 1800}  → indisponível por 30 min (Cenário 3)
* customer_id "always-fail" → sempre 503 (testa DLQ);  "bad-request" → 400 (erro permanente)
"""

from __future__ import annotations

import asyncio
import os
import random
import time
from typing import Literal

from fastapi import FastAPI, Header, Response
from pydantic import BaseModel

app = FastAPI(title="Risk Analysis (mock)")


class Chaos(BaseModel):
    failure_rate: float = float(os.getenv("FAILURE_RATE", "0.1"))
    slow_rate: float = float(os.getenv("SLOW_RATE", "0.1"))
    slow_seconds: float = float(os.getenv("SLOW_SECONDS", "5"))
    reject_above: float = float(os.getenv("REJECT_ABOVE", "10000"))
    outage_until: float = 0.0


chaos = Chaos()
seen_idempotency_keys: dict[str, str] = {}


class RiskRequest(BaseModel):
    customer_id: str
    value: float


class RiskResponse(BaseModel):
    result: Literal["APPROVED", "REJECTED"]


@app.post("/risk-analysis", response_model=RiskResponse)
async def risk_analysis(
    body: RiskRequest,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> RiskResponse | Response:
    if time.time() < chaos.outage_until or body.customer_id == "always-fail":
        return Response(status_code=503, content="service unavailable (simulated)")
    if body.customer_id == "bad-request":
        return Response(status_code=400, content="invalid customer (simulated)")
    if random.random() < chaos.slow_rate:
        await asyncio.sleep(chaos.slow_seconds)
    if random.random() < chaos.failure_rate:
        return Response(status_code=503, content="random failure (simulated)")

    # Idempotência do lado do provedor: a mesma chave devolve a mesma decisão.
    if idempotency_key and idempotency_key in seen_idempotency_keys:
        response.headers["Idempotent-Replayed"] = "true"
        return RiskResponse(result=seen_idempotency_keys[idempotency_key])  # type: ignore[arg-type]

    rejected = body.value > chaos.reject_above or body.customer_id.startswith("blocked")
    result = "REJECTED" if rejected else "APPROVED"
    if idempotency_key:
        seen_idempotency_keys[idempotency_key] = result
    return RiskResponse(result=result)  # type: ignore[arg-type]


class ChaosUpdate(BaseModel):
    failure_rate: float | None = None
    slow_rate: float | None = None
    slow_seconds: float | None = None
    reject_above: float | None = None


@app.get("/admin/chaos")
async def get_chaos() -> dict[str, float]:
    return {**chaos.model_dump(), "outage_remaining_s": max(0.0, chaos.outage_until - time.time())}


@app.post("/admin/chaos")
async def set_chaos(update: ChaosUpdate) -> dict[str, float]:
    for field, value in update.model_dump(exclude_none=True).items():
        setattr(chaos, field, value)
    return await get_chaos()


class Outage(BaseModel):
    seconds: float = 1800


@app.post("/admin/outage")
async def start_outage(outage: Outage) -> dict[str, float]:
    chaos.outage_until = time.time() + outage.seconds
    return await get_chaos()


@app.delete("/admin/outage")
async def stop_outage() -> dict[str, float]:
    chaos.outage_until = 0.0
    return await get_chaos()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
