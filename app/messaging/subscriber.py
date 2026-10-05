from collections.abc import Mapping
from typing import Any

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from app.messaging.events import PaymentCreatedEvent
from app.messaging.handler import PaymentMessageHandler
from app.messaging.topology import ATTEMPT_HEADER, Topology


def read_attempt(headers: Mapping[str, Any]) -> int:
    """Attempt number of this delivery; a message without the header is on its first attempt."""
    try:
        return max(int(headers.get(ATTEMPT_HEADER, 1)), 1)
    except (TypeError, ValueError):
        return 1


def register_payment_handler(
    broker: RabbitBroker, *, topology: Topology, handler: PaymentMessageHandler
) -> None:
    """Subscribe the single payment handler to payments.new."""

    # REJECT_ON_ERROR: any exception leaving the subscriber (the handler giving up, or a body that
    # fails to parse into PaymentCreatedEvent) rejects the message without requeue, and the
    # queue's dead letter exchange moves it to payments.dlq.
    @broker.subscriber(topology.queue, topology.exchange, ack_policy=AckPolicy.REJECT_ON_ERROR)
    async def on_payment_created(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        await handler.handle(
            event,
            attempt=read_attempt(message.headers),
            body=message.body,
            message_id=message.message_id,
        )
