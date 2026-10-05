import uuid
from datetime import UTC, datetime

import pytest

from app.db.models import Payment
from app.enums import PaymentStatus
from app.errors import NonRetryableError, WebhookDeliveryError
from app.services.processor import PaymentProcessor
from tests.factories import make_payment


class InMemoryPayments:
    def __init__(self, *payments: Payment) -> None:
        self._payments = {payment.id: payment for payment in payments}

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return self._payments.get(payment_id)

    async def complete(self, payment_id: uuid.UUID, status: PaymentStatus) -> Payment | None:
        payment = self._payments[payment_id]
        if payment.status != PaymentStatus.PENDING:
            return None
        payment.status = status.value
        payment.processed_at = datetime.now(UTC)
        return payment

    async def mark_webhook_delivered(self, payment_id: uuid.UUID) -> None:
        self._payments[payment_id].webhook_delivered_at = datetime.now(UTC)


class StubGateway:
    def __init__(self, status: PaymentStatus = PaymentStatus.SUCCEEDED) -> None:
        self.status = status
        self.calls = 0

    async def charge(self, payment: Payment) -> PaymentStatus:
        self.calls += 1
        return self.status


class RecordingWebhooks:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.sent: list[tuple[uuid.UUID, str]] = []

    async def send(self, payment: Payment) -> None:
        if self.error is not None:
            raise self.error
        self.sent.append((payment.id, payment.status))


def processor_for(
    payments: InMemoryPayments, gateway: StubGateway, webhooks: RecordingWebhooks
) -> PaymentProcessor:
    return PaymentProcessor(payments=payments, gateway=gateway, webhooks=webhooks)


@pytest.mark.parametrize("gateway_status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_pending_payment_is_charged_then_client_is_notified(
    gateway_status: PaymentStatus,
) -> None:
    payment = make_payment()
    gateway, webhooks = StubGateway(gateway_status), RecordingWebhooks()

    await processor_for(InMemoryPayments(payment), gateway, webhooks).process(payment.id)

    assert gateway.calls == 1
    assert payment.status == gateway_status
    assert payment.processed_at is not None
    assert webhooks.sent == [(payment.id, gateway_status.value)]
    assert payment.webhook_delivered_at is not None


async def test_webhook_failure_propagates_and_keeps_the_final_status() -> None:
    payment = make_payment()
    gateway = StubGateway()
    webhooks = RecordingWebhooks(error=WebhookDeliveryError("HTTP 500"))

    with pytest.raises(WebhookDeliveryError):
        await processor_for(InMemoryPayments(payment), gateway, webhooks).process(payment.id)

    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.webhook_delivered_at is None


async def test_retry_after_webhook_failure_does_not_charge_again() -> None:
    payment = make_payment()
    payments, gateway = InMemoryPayments(payment), StubGateway()

    failing = RecordingWebhooks(error=WebhookDeliveryError("HTTP 500"))
    with pytest.raises(WebhookDeliveryError):
        await processor_for(payments, gateway, failing).process(payment.id)

    working = RecordingWebhooks()
    await processor_for(payments, gateway, working).process(payment.id)

    assert gateway.calls == 1
    assert working.sent == [(payment.id, "succeeded")]
    assert payment.webhook_delivered_at is not None


async def test_fully_processed_payment_is_a_no_op() -> None:
    payment = make_payment(
        status=PaymentStatus.FAILED.value,
        processed_at=datetime.now(UTC),
        webhook_delivered_at=datetime.now(UTC),
    )
    gateway, webhooks = StubGateway(), RecordingWebhooks()

    await processor_for(InMemoryPayments(payment), gateway, webhooks).process(payment.id)

    assert gateway.calls == 0
    assert webhooks.sent == []
    assert payment.status == PaymentStatus.FAILED


async def test_unknown_payment_is_not_retryable() -> None:
    processor = processor_for(InMemoryPayments(), StubGateway(), RecordingWebhooks())

    with pytest.raises(NonRetryableError):
        await processor.process(uuid.uuid4())


async def test_result_of_a_concurrent_duplicate_wins() -> None:
    """Another worker completed the payment while this one was waiting for the gateway."""

    class RacingPayments(InMemoryPayments):
        async def complete(self, payment_id: uuid.UUID, status: PaymentStatus) -> Payment | None:
            await super().complete(payment_id, PaymentStatus.FAILED)
            return await super().complete(payment_id, status)

    payment = make_payment()
    webhooks = RecordingWebhooks()
    processor = processor_for(
        RacingPayments(payment), StubGateway(PaymentStatus.SUCCEEDED), webhooks
    )

    await processor.process(payment.id)

    assert payment.status == PaymentStatus.FAILED
    assert webhooks.sent == [(payment.id, "failed")]
