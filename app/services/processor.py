import logging
import uuid
from typing import Protocol

from app.db.models import Payment
from app.enums import PaymentStatus
from app.errors import NonRetryableError
from app.services.gateway import PaymentGateway

logger = logging.getLogger(__name__)


class PaymentRepository(Protocol):
    async def get(self, payment_id: uuid.UUID) -> Payment | None: ...

    async def complete(self, payment_id: uuid.UUID, status: PaymentStatus) -> Payment | None: ...

    async def mark_webhook_delivered(self, payment_id: uuid.UUID) -> None: ...


class WebhookNotifier(Protocol):
    async def send(self, payment: Payment) -> None: ...


class PaymentProcessor:
    """Charges a payment and notifies the client, safely repeatable for the same payment.

    Messages arrive at least once (outbox redelivery, broker redelivery, retries), so every step
    looks at the stored state first: a payment that already has a final status is not charged
    again, and a delivered webhook is not sent again.
    """

    def __init__(
        self, *, payments: PaymentRepository, gateway: PaymentGateway, webhooks: WebhookNotifier
    ) -> None:
        self._payments = payments
        self._gateway = gateway
        self._webhooks = webhooks

    async def process(self, payment_id: uuid.UUID) -> None:
        payment = await self._payments.get(payment_id)
        if payment is None:
            raise NonRetryableError(f"payment {payment_id} does not exist")

        if payment.status == PaymentStatus.PENDING:
            payment = await self._charge(payment)

        if payment.webhook_delivered_at is None:
            await self._webhooks.send(payment)
            await self._payments.mark_webhook_delivered(payment.id)
            logger.info("payment %s: webhook delivered", payment.id)

    async def _charge(self, payment: Payment) -> Payment:
        # The gateway call takes seconds, so it runs outside of any database transaction.
        status = await self._gateway.charge(payment)
        completed = await self._payments.complete(payment.id, status)
        if completed is not None:
            logger.info("payment %s: %s", payment.id, status.value)
            return completed

        # A concurrent duplicate of this message finished first; its result is the one that counts.
        current = await self._payments.get(payment.id)
        if current is None:
            raise NonRetryableError(f"payment {payment.id} does not exist")
        return current
