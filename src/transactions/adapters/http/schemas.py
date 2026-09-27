from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer

from transactions.application.ports import TransactionView

# Dinheiro trafega como número JSON (como no enunciado), mas internamente é Decimal.
MoneyOut = Annotated[Decimal, PlainSerializer(lambda v: float(v), return_type=float)]


class CreateTransactionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    customer_id: str = Field(min_length=1, max_length=64, examples=["123"])
    value: Decimal = Field(gt=0, max_digits=14, decimal_places=2, examples=[1500.00])


class TransactionResponse(BaseModel):
    id: uuid.UUID
    customer_id: str
    value: MoneyOut
    status: str
    attempts: int
    last_error: str | None = None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_view(cls, view: TransactionView) -> TransactionResponse:
        return cls(
            id=view.id,
            customer_id=view.customer_id,
            value=view.value,
            status=view.status,
            attempts=view.attempts,
            last_error=view.last_error,
            created_at=view.created_at,
            updated_at=view.updated_at,
        )


class Problem(BaseModel):
    """RFC 9457 (Problem Details)."""

    type: str = "about:blank"
    title: str
    status: int
    detail: str | None = None
    correlation_id: str | None = None
    errors: list[dict[str, object]] | None = None
