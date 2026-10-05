import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from app.db.models import Payment
from app.enums import Currency, PaymentStatus


class PaymentCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2, examples=["1500.00"])
    currency: Currency
    description: str | None = Field(default=None, max_length=500)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentAcceptedResponse(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentResponse(BaseModel):
    payment_id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    webhook_delivered_at: datetime | None
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_payment(cls, payment: Payment) -> "PaymentResponse":
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=Currency(payment.currency),
            description=payment.description,
            metadata=payment.metadata_,
            status=PaymentStatus(payment.status),
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            webhook_delivered_at=payment.webhook_delivered_at,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class ErrorResponse(BaseModel):
    success: bool = False
    error_code: str
    message: str
    trace_id: str
    details: dict[str, Any] = Field(default_factory=dict)
