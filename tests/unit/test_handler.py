import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from app.errors import NonRetryableError, WebhookDeliveryError
from app.messaging.events import PaymentCreatedEvent
from app.messaging.handler import PaymentMessageHandler
from app.messaging.retry import RetryPolicy
from app.messaging.subscriber import read_attempt

BODY = b'{"payment_id": "..."}'


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[uuid.UUID] = []

    async def process(self, payment_id: uuid.UUID) -> None:
        self.calls.append(payment_id)
        if self.error is not None:
            raise self.error


class FakeRetryPublisher:
    def __init__(self) -> None:
        self.scheduled: list[dict[str, Any]] = []

    async def schedule(self, **kwargs: Any) -> None:
        self.scheduled.append(kwargs)


def make_event() -> PaymentCreatedEvent:
    return PaymentCreatedEvent(
        event_id=uuid.uuid4(), payment_id=uuid.uuid4(), occurred_at=datetime.now(UTC)
    )


def make_handler(processor: FakeProcessor, publisher: FakeRetryPublisher) -> PaymentMessageHandler:
    return PaymentMessageHandler(
        processor=processor, retry_policy=RetryPolicy(), retry_publisher=publisher
    )


async def test_success_schedules_nothing() -> None:
    processor, publisher = FakeProcessor(), FakeRetryPublisher()
    event = make_event()

    await make_handler(processor, publisher).handle(event, attempt=1, body=BODY, message_id="m1")

    assert processor.calls == [event.payment_id]
    assert publisher.scheduled == []


@pytest.mark.parametrize(("attempt", "delay_ms"), [(1, 5_000), (2, 10_000)])
async def test_failure_with_attempts_left_schedules_a_delayed_retry(
    attempt: int, delay_ms: int
) -> None:
    processor = FakeProcessor(error=WebhookDeliveryError("HTTP 500"))
    publisher = FakeRetryPublisher()

    # Returning without an exception is what acks the original message.
    await make_handler(processor, publisher).handle(
        make_event(), attempt=attempt, body=BODY, message_id="m1"
    )

    assert publisher.scheduled == [
        {
            "body": BODY,
            "message_id": "m1",
            "delay_ms": delay_ms,
            "attempt": attempt + 1,
            "error": "WebhookDeliveryError: HTTP 500",
        }
    ]


async def test_failure_on_the_last_attempt_is_raised_for_dead_lettering() -> None:
    processor = FakeProcessor(error=WebhookDeliveryError("HTTP 500"))
    publisher = FakeRetryPublisher()

    with pytest.raises(WebhookDeliveryError):
        await make_handler(processor, publisher).handle(
            make_event(), attempt=3, body=BODY, message_id="m1"
        )

    assert publisher.scheduled == []


async def test_non_retryable_error_is_raised_on_the_first_attempt() -> None:
    processor = FakeProcessor(error=NonRetryableError("payment does not exist"))
    publisher = FakeRetryPublisher()

    with pytest.raises(NonRetryableError):
        await make_handler(processor, publisher).handle(
            make_event(), attempt=1, body=BODY, message_id="m1"
        )

    assert publisher.scheduled == []


async def test_message_is_dead_lettered_when_the_retry_cannot_be_scheduled() -> None:
    class BrokenRetryPublisher:
        async def schedule(self, **kwargs: Any) -> None:
            raise TimeoutError("broker did not confirm the publish")

    handler = PaymentMessageHandler(
        processor=FakeProcessor(error=WebhookDeliveryError("HTTP 500")),
        retry_policy=RetryPolicy(),
        retry_publisher=BrokenRetryPublisher(),
    )

    # Raising is what rejects the message, so it is kept in the dead letter queue, not dropped.
    with pytest.raises(TimeoutError):
        await handler.handle(make_event(), attempt=1, body=BODY, message_id="m1")


async def test_unexpected_errors_are_retried_too() -> None:
    processor = FakeProcessor(error=ConnectionError("database is down"))
    publisher = FakeRetryPublisher()

    await make_handler(processor, publisher).handle(
        make_event(), attempt=1, body=BODY, message_id=None
    )

    assert publisher.scheduled[0]["attempt"] == 2
    assert publisher.scheduled[0]["error"] == "ConnectionError: database is down"


async def test_long_error_text_is_truncated() -> None:
    processor = FakeProcessor(error=RuntimeError("x" * 2000))
    publisher = FakeRetryPublisher()

    await make_handler(processor, publisher).handle(
        make_event(), attempt=1, body=BODY, message_id="m1"
    )

    assert len(publisher.scheduled[0]["error"]) == 500


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({}, 1),
        ({"x-attempt": 1}, 1),
        ({"x-attempt": 3}, 3),
        ({"x-attempt": "2"}, 2),
        ({"x-attempt": "garbage"}, 1),
        ({"x-attempt": None}, 1),
        ({"x-attempt": 0}, 1),
    ],
)
def test_read_attempt(headers: dict[str, Any], expected: int) -> None:
    assert read_attempt(headers) == expected
