import uuid

from app.db.repositories import SqlPaymentRepository
from app.db.session import SessionFactory
from app.enums import PaymentStatus
from tests.factories import make_payment


async def add_payment(session_factory: SessionFactory) -> uuid.UUID:
    payment = make_payment()
    async with session_factory() as session:
        session.add(payment)
        await session.commit()
    return payment.id


async def test_get(session_factory: SessionFactory) -> None:
    payment_id = await add_payment(session_factory)
    repository = SqlPaymentRepository(session_factory)

    payment = await repository.get(payment_id)

    assert payment is not None
    assert payment.status == "pending"
    assert payment.metadata_ == {"order_id": "42"}
    assert await repository.get(uuid.uuid4()) is None


async def test_complete_sets_the_final_status_once(session_factory: SessionFactory) -> None:
    payment_id = await add_payment(session_factory)
    repository = SqlPaymentRepository(session_factory)

    first = await repository.complete(payment_id, PaymentStatus.SUCCEEDED)
    second = await repository.complete(payment_id, PaymentStatus.FAILED)

    assert first is not None
    assert first.status == "succeeded"
    assert first.processed_at is not None
    assert second is None

    stored = await repository.get(payment_id)
    assert stored is not None
    assert stored.status == "succeeded"
    assert stored.processed_at == first.processed_at


async def test_mark_webhook_delivered_keeps_the_first_timestamp(
    session_factory: SessionFactory,
) -> None:
    payment_id = await add_payment(session_factory)
    repository = SqlPaymentRepository(session_factory)

    await repository.mark_webhook_delivered(payment_id)
    first = await repository.get(payment_id)
    await repository.mark_webhook_delivered(payment_id)
    second = await repository.get(payment_id)

    assert first is not None and second is not None
    assert first.webhook_delivered_at is not None
    assert second.webhook_delivered_at == first.webhook_delivered_at
