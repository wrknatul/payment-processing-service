from typing import Any

from faststream.rabbit import RabbitBroker

from app.messaging.topology import ATTEMPT_HEADER, LAST_ERROR_HEADER, Topology

PUBLISH_TIMEOUT_SECONDS = 10.0


class RabbitEventPublisher:
    """Publishes outbox events to the payments exchange as persistent, confirmed messages."""

    def __init__(self, broker: RabbitBroker, topology: Topology) -> None:
        self._broker = broker
        self._topology = topology

    async def publish(self, *, message_id: str, routing_key: str, payload: dict[str, Any]) -> None:
        await self._broker.publish(
            payload,
            exchange=self._topology.exchange,
            routing_key=routing_key,
            message_id=message_id,
            headers={ATTEMPT_HEADER: 1},
            persist=True,
            mandatory=True,
            timeout=PUBLISH_TIMEOUT_SECONDS,
        )


class RabbitRetryPublisher:
    """Parks a failed message in the delay queue matching the requested backoff."""

    def __init__(self, broker: RabbitBroker, topology: Topology) -> None:
        self._broker = broker
        self._topology = topology

    async def schedule(
        self, *, body: bytes, message_id: str | None, delay_ms: int, attempt: int, error: str
    ) -> None:
        # Only our own headers are set: copying the broker's x-death history into a new message
        # would confuse RabbitMQ's dead-letter cycle detection.
        await self._broker.publish(
            body,
            exchange=self._topology.retry_exchange,
            routing_key=self._topology.retry_queue(delay_ms).routing_key,
            message_id=message_id,
            headers={ATTEMPT_HEADER: attempt, LAST_ERROR_HEADER: error},
            content_type="application/json",
            persist=True,
            mandatory=True,
            timeout=PUBLISH_TIMEOUT_SECONDS,
        )
