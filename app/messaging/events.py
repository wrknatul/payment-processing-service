import uuid
from datetime import datetime
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict

PAYMENT_CREATED: Final = "payment.created"
PAYMENTS_NEW_ROUTING_KEY: Final = "payments.new"


class PaymentCreatedEvent(BaseModel):
    """Carries only identifiers: the consumer reads the payment itself from the database."""

    model_config = ConfigDict(extra="ignore")

    event_id: uuid.UUID
    event_type: Literal["payment.created"] = PAYMENT_CREATED
    payment_id: uuid.UUID
    occurred_at: datetime
