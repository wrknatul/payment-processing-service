import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutboxEvent, Payment
from app.enums import Currency, PaymentStatus
from app.errors import IdempotencyKeyConflictError, PaymentNotFoundError
from app.messaging.events import PAYMENT_CREATED, PAYMENTS_NEW_ROUTING_KEY, PaymentCreatedEvent

logger = logging.getLogger(__name__)

CENTS = Decimal("0.01")


@dataclass(frozen=True, slots=True)
class NewPayment:
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any]
    webhook_url: str

    def fingerprint(self) -> str:
        """Stable hash of the request body, so that 100 and 100.00 count as the same request."""
        canonical = json.dumps(
            {
                "amount": str(self.amount.quantize(CENTS)),
                "currency": self.currency.value,
                "description": self.description,
                "metadata": self.metadata,
                "webhook_url": self.webhook_url,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    replayed: bool


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, data: NewPayment, idempotency_key: str) -> CreatePaymentResult:
        """Create a payment and its outbox event atomically, once per idempotency key.

        INSERT ... ON CONFLICT DO NOTHING lets the unique index arbitrate concurrent requests with
        the same key: exactly one of them inserts, the others wait for it to commit and get no row
        back. A check-then-insert would let two requests both pass the check.
        """
        request_hash = data.fingerprint()
        statement = (
            insert(Payment)
            .values(
                id=uuid.uuid4(),
                amount=data.amount,
                currency=data.currency.value,
                description=data.description,
                metadata_=data.metadata,
                status=PaymentStatus.PENDING.value,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                webhook_url=data.webhook_url,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        payment = await self._session.scalar(statement)
        if payment is None:
            return await self._replay(idempotency_key, request_hash)

        # Same transaction as the payment: the event exists if and only if the payment does.
        outbox_id = uuid.uuid4()
        event = PaymentCreatedEvent(
            event_id=outbox_id, payment_id=payment.id, occurred_at=payment.created_at
        )
        self._session.add(
            OutboxEvent(
                id=outbox_id,
                event_type=PAYMENT_CREATED,
                routing_key=PAYMENTS_NEW_ROUTING_KEY,
                payload=event.model_dump(mode="json"),
            )
        )
        await self._session.commit()
        logger.info("payment %s: created, outbox event %s", payment.id, outbox_id)
        return CreatePaymentResult(payment=payment, replayed=False)

    async def get(self, payment_id: uuid.UUID) -> Payment:
        payment = await self._session.get(Payment, payment_id)
        if payment is None:
            raise PaymentNotFoundError
        return payment

    async def _replay(self, idempotency_key: str, request_hash: str) -> CreatePaymentResult:
        existing = await self._session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if existing is None:
            # The conflicting row vanished between the two statements; nothing sane to return.
            raise RuntimeError(f"payment for idempotency key {idempotency_key!r} disappeared")
        if existing.request_hash != request_hash:
            raise IdempotencyKeyConflictError
        logger.info("payment %s: idempotent replay", existing.id)
        return CreatePaymentResult(payment=existing, replayed=True)
