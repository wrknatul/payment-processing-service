import hashlib
import hmac
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx
from pydantic import BaseModel

from app.db.models import Payment
from app.errors import WebhookDeliveryError

EVENT_ID_HEADER = "X-Webhook-Event-Id"
SIGNATURE_HEADER = "X-Webhook-Signature"


class WebhookPayload(BaseModel):
    event_id: uuid.UUID
    event_type: str
    payment_id: uuid.UUID
    status: str
    amount: Decimal
    currency: str
    description: str | None
    metadata: dict[str, Any]
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_payment(cls, payment: Payment) -> "WebhookPayload":
        return cls(
            # One notification per payment, so the payment id doubles as the event id the
            # receiver deduplicates on: delivery is at-least-once.
            event_id=payment.id,
            event_type=f"payment.{payment.status}",
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


def sign(body: bytes, secret: str) -> str:
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


class WebhookSender:
    def __init__(
        self, client: httpx.AsyncClient, *, signing_secret: str, timeout_seconds: float = 5.0
    ) -> None:
        self._client = client
        self._signing_secret = signing_secret
        self._timeout = timeout_seconds

    async def send(self, payment: Payment) -> None:
        """Deliver the payment result; anything but a 2xx answer is a delivery failure."""
        # Serialized once: the signature must cover the exact bytes that go on the wire.
        body = WebhookPayload.from_payment(payment).model_dump_json().encode()
        headers = {
            "Content-Type": "application/json",
            EVENT_ID_HEADER: str(payment.id),
            SIGNATURE_HEADER: sign(body, self._signing_secret),
        }
        try:
            response = await self._client.post(
                payment.webhook_url,
                content=body,
                headers=headers,
                timeout=self._timeout,
                follow_redirects=False,
            )
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(
                f"webhook to {payment.webhook_url} failed: {type(exc).__name__}: {exc}"
            ) from exc
        if not response.is_success:
            raise WebhookDeliveryError(
                f"webhook to {payment.webhook_url} answered HTTP {response.status_code}"
            )
