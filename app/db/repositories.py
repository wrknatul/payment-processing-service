import uuid

from sqlalchemy import func, update

from app.db.models import Payment
from app.db.session import SessionFactory
from app.enums import PaymentStatus


class SqlPaymentRepository:
    """Payment persistence for the consumer: every method is its own short transaction."""

    def __init__(self, session_factory: SessionFactory) -> None:
        self._session_factory = session_factory

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        async with self._session_factory() as session:
            return await session.get(Payment, payment_id)

    async def complete(self, payment_id: uuid.UUID, status: PaymentStatus) -> Payment | None:
        """Move a pending payment to its final status.

        The `status = 'pending'` guard makes the transition happen at most once: when the same
        message is processed twice, the second writer gets None and must not overwrite the result.
        """
        statement = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING.value)
            .values(status=status.value, processed_at=func.now())
            .returning(Payment)
        )
        async with self._session_factory() as session:
            payment = await session.scalar(statement)
            await session.commit()
            return payment

    async def mark_webhook_delivered(self, payment_id: uuid.UUID) -> None:
        statement = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
            .values(webhook_delivered_at=func.now())
        )
        async with self._session_factory() as session:
            await session.execute(statement)
            await session.commit()
