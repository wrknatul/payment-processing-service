"""Outbox relay: moves events from the outbox table to RabbitMQ.

Run with: python -m app.outbox.relay

Delivery is at-least-once. A row is marked as published only after the broker confirmed the
message, in the same transaction that holds the row lock. If the process dies between the confirm
and the commit, the row is published again on the next iteration, so consumers must be idempotent.
"""

import asyncio
import logging
import signal
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Protocol

from sqlalchemy import func, select

from app.config import get_settings
from app.db.models import OutboxEvent
from app.db.session import SessionFactory, create_engine, create_session_factory
from app.logging_config import setup_logging
from app.messaging.broker import build_broker
from app.messaging.publishers import RabbitEventPublisher
from app.messaging.retry import RetryPolicy
from app.messaging.topology import build_topology, declare_topology

# Named explicitly: under `python -m` the module name would be "__main__".
logger = logging.getLogger("app.outbox.relay")

MAX_BACKOFF_SECONDS = 30.0
MAX_ERROR_LENGTH = 1000


class EventPublisher(Protocol):
    async def publish(
        self, *, message_id: str, routing_key: str, payload: dict[str, Any]
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class BatchResult:
    published: int
    failed: bool


class OutboxRelay:
    def __init__(
        self,
        session_factory: SessionFactory,
        publisher: EventPublisher,
        *,
        batch_size: int = 100,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval = poll_interval_seconds

    async def publish_pending(self) -> BatchResult:
        """Publish one batch of unpublished events, oldest first."""
        published = 0
        failed = False
        async with self._session_factory() as session, session.begin():
            # SKIP LOCKED lets several relay instances run side by side without publishing the
            # same row twice: rows locked by one instance are invisible to the others.
            events = await session.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.created_at, OutboxEvent.id)
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            for event in events.all():
                try:
                    await self._publisher.publish(
                        message_id=str(event.id),
                        routing_key=event.routing_key,
                        payload=event.payload,
                    )
                except Exception as exc:
                    # Whatever went wrong, the row stays unpublished and is retried later. The
                    # rest of the batch is skipped: the broker is most likely unavailable.
                    error = f"{type(exc).__name__}: {exc}"[:MAX_ERROR_LENGTH]
                    logger.warning("outbox event %s: publish failed: %s", event.id, error)
                    event.attempts += 1
                    event.last_error = error
                    failed = True
                    break
                event.published_at = func.now()
                published += 1
        if published:
            logger.info("outbox: published %d event(s)", published)
        return BatchResult(published=published, failed=failed)

    async def run(self, stop: asyncio.Event) -> None:
        """Poll the outbox until `stop` is set, backing off while the database or broker is down."""
        delay = self._poll_interval
        while not stop.is_set():
            try:
                result = await self.publish_pending()
            except Exception:
                logger.exception("outbox: iteration failed")
                delay = min(delay * 2, MAX_BACKOFF_SECONDS)
            else:
                if result.failed:
                    delay = min(delay * 2, MAX_BACKOFF_SECONDS)
                elif result.published == self._batch_size:
                    # A full batch means more rows are probably waiting.
                    delay = self._poll_interval
                    continue
                else:
                    delay = self._poll_interval
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)

    topology = build_topology(RetryPolicy.from_settings(settings))
    engine = create_engine(settings.database_url)
    broker = build_broker(settings.rabbitmq_url)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)

    try:
        await broker.connect()
        # payments.new must exist before the first publish, whether or not a consumer is up.
        await declare_topology(broker, topology)
        relay = OutboxRelay(
            create_session_factory(engine),
            RabbitEventPublisher(broker, topology),
            batch_size=settings.outbox_batch_size,
            poll_interval_seconds=settings.outbox_poll_interval_seconds,
        )
        logger.info("outbox relay started")
        await relay.run(stop)
    finally:
        await broker.stop()
        await engine.dispose()
        logger.info("outbox relay stopped")


if __name__ == "__main__":
    asyncio.run(main())
