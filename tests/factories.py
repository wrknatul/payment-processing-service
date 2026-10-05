import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from app.db.models import Payment
from app.enums import PaymentStatus


def make_payment(**overrides: Any) -> Payment:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "amount": Decimal("100.50"),
        "currency": "RUB",
        "description": "Order 42",
        "metadata_": {"order_id": "42"},
        "status": PaymentStatus.PENDING.value,
        "idempotency_key": str(uuid.uuid4()),
        "request_hash": "0" * 64,
        "webhook_url": "https://client.example/webhooks/payments",
        "webhook_delivered_at": None,
        "created_at": datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
        "processed_at": None,
    }
    return Payment(**{**values, **overrides})
