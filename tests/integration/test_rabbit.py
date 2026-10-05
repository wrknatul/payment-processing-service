"""The retry and dead-letter flow against a real RabbitMQ."""

import json
import uuid

import aio_pika
import pytest

from app.errors import NonRetryableError
from app.messaging.publishers import RabbitEventPublisher
from tests.integration.conftest import TEST_RETRY_POLICY, RabbitEnv

FIRST_DELAY, SECOND_DELAY = (delay_ms / 1000 for delay_ms in TEST_RETRY_POLICY.delays_ms)


async def test_successful_message_is_processed_once(rabbit: RabbitEnv) -> None:
    await rabbit.publish_event()

    await rabbit.processor.wait_for_calls(1)

    assert await rabbit.dead_letter_queue_stays_empty()
    assert len(rabbit.processor.calls) == 1


async def test_message_is_dead_lettered_after_three_attempts_with_growing_delays(
    rabbit: RabbitEnv,
) -> None:
    rabbit.processor.always = RuntimeError("boom")

    event = await rabbit.publish_event()
    dead = await rabbit.get_dead_letter()

    calls = rabbit.processor.calls
    assert len(calls) == 3
    # A TTL queue never releases a message early, so the delays are lower bounds.
    assert calls[1] - calls[0] >= FIRST_DELAY * 0.9
    assert calls[2] - calls[1] >= SECOND_DELAY * 0.9

    assert json.loads(dead.body)["payment_id"] == str(event.payment_id)
    assert dead.message_id == str(event.event_id)
    assert dead.headers["x-attempt"] == 3
    assert dead.headers["x-last-error"] == "RuntimeError: boom"
    assert dead.headers["x-death"][0]["reason"] == "rejected"
    assert dead.headers["x-death"][0]["queue"] == rabbit.topology.queue.name


async def test_message_recovers_when_a_retry_succeeds(rabbit: RabbitEnv) -> None:
    rabbit.processor.errors = [RuntimeError("first"), RuntimeError("second")]

    await rabbit.publish_event()
    await rabbit.processor.wait_for_calls(3)

    assert await rabbit.dead_letter_queue_stays_empty()
    assert len(rabbit.processor.calls) == 3


async def test_non_retryable_error_is_dead_lettered_immediately(rabbit: RabbitEnv) -> None:
    rabbit.processor.always = NonRetryableError("payment does not exist")

    await rabbit.publish_event()
    dead = await rabbit.get_dead_letter()

    assert len(rabbit.processor.calls) == 1
    assert dead.headers["x-attempt"] == 1


@pytest.mark.parametrize(
    "body",
    [b"not json", b'{"unexpected": "shape"}', b'{"payment_id": "not-a-uuid"}'],
)
async def test_malformed_message_is_dead_lettered_without_processing(
    rabbit: RabbitEnv, body: bytes
) -> None:
    await rabbit.publish_raw(body)

    dead = await rabbit.get_dead_letter()

    assert dead.body == body
    assert rabbit.processor.calls == []


@pytest.mark.no_consumer
async def test_event_publisher_sends_a_persistent_message(rabbit: RabbitEnv) -> None:
    event = await rabbit.publish_event()

    message = await rabbit.get_message(rabbit.topology.queue.name)

    assert json.loads(message.body) == event.model_dump(mode="json")
    assert message.message_id == str(event.event_id)
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert message.content_type == "application/json"
    assert message.headers["x-attempt"] == 1


@pytest.mark.no_consumer
async def test_event_publisher_fails_loudly_when_the_message_cannot_be_routed(
    rabbit: RabbitEnv,
) -> None:
    publisher = RabbitEventPublisher(rabbit.broker, rabbit.topology)

    with pytest.raises(aio_pika.exceptions.PublishError):
        await publisher.publish(
            message_id=str(uuid.uuid4()), routing_key="no.such.queue", payload={"a": 1}
        )
