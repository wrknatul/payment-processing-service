import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select

from app.db.models import OutboxEvent
from app.db.session import SessionFactory
from app.outbox.relay import BatchResult, OutboxRelay


class RecordingPublisher:
    def __init__(self, *, fail_on_call: int | None = None, delay_seconds: float = 0) -> None:
        self.published: list[str] = []
        self._fail_on_call = fail_on_call
        self._delay = delay_seconds
        self._calls = 0

    async def publish(self, *, message_id: str, routing_key: str, payload: dict[str, Any]) -> None:
        self._calls += 1
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._calls == self._fail_on_call:
            raise ConnectionError("broker is down")
        self.published.append(message_id)


async def add_events(session_factory: SessionFactory, count: int) -> list[str]:
    """Insert unpublished events and return their ids, oldest first."""
    base = datetime.now(UTC) - timedelta(minutes=1)
    events = [
        OutboxEvent(
            id=uuid.uuid4(),
            event_type="payment.created",
            routing_key="payments.new",
            payload={"payment_id": str(uuid.uuid4())},
            created_at=base + timedelta(seconds=index),
        )
        for index in range(count)
    ]
    async with session_factory() as session:
        session.add_all(events)
        await session.commit()
    return [str(event.id) for event in events]


async def load(session_factory: SessionFactory) -> dict[str, OutboxEvent]:
    async with session_factory() as session:
        return {str(event.id): event for event in await session.scalars(select(OutboxEvent))}


async def test_publishes_pending_events_oldest_first_and_marks_them(
    session_factory: SessionFactory,
) -> None:
    ids = await add_events(session_factory, 3)
    publisher = RecordingPublisher()
    relay = OutboxRelay(session_factory, publisher)

    result = await relay.publish_pending()

    assert result == BatchResult(published=3, failed=False)
    assert publisher.published == ids
    assert all(event.published_at is not None for event in (await load(session_factory)).values())


async def test_published_events_are_not_sent_again(session_factory: SessionFactory) -> None:
    await add_events(session_factory, 2)
    publisher = RecordingPublisher()
    relay = OutboxRelay(session_factory, publisher)

    await relay.publish_pending()
    result = await relay.publish_pending()

    assert result == BatchResult(published=0, failed=False)
    assert len(publisher.published) == 2


async def test_batch_size_limits_one_iteration(session_factory: SessionFactory) -> None:
    ids = await add_events(session_factory, 5)
    publisher = RecordingPublisher()
    relay = OutboxRelay(session_factory, publisher, batch_size=2)

    assert (await relay.publish_pending()).published == 2
    assert (await relay.publish_pending()).published == 2
    assert (await relay.publish_pending()).published == 1
    assert publisher.published == ids


async def test_publish_failure_keeps_the_event_for_the_next_iteration(
    session_factory: SessionFactory,
) -> None:
    ids = await add_events(session_factory, 3)
    relay = OutboxRelay(session_factory, RecordingPublisher(fail_on_call=2))

    result = await relay.publish_pending()

    assert result == BatchResult(published=1, failed=True)
    events = await load(session_factory)
    assert events[ids[0]].published_at is not None
    assert events[ids[1]].published_at is None
    assert events[ids[1]].attempts == 1
    assert events[ids[1]].last_error == "ConnectionError: broker is down"
    # The batch stops at the first failure.
    assert events[ids[2]].published_at is None
    assert events[ids[2]].attempts == 0

    recovered = RecordingPublisher()
    result = await OutboxRelay(session_factory, recovered).publish_pending()

    assert result == BatchResult(published=2, failed=False)
    assert recovered.published == ids[1:]


async def test_parallel_relays_do_not_publish_an_event_twice(
    session_factory: SessionFactory,
) -> None:
    ids = await add_events(session_factory, 6)
    publisher = RecordingPublisher(delay_seconds=0.05)
    relays = [OutboxRelay(session_factory, publisher, batch_size=3) for _ in range(2)]

    results = await asyncio.gather(*(relay.publish_pending() for relay in relays))

    # Each relay locks its own three rows; with SKIP LOCKED neither waits for the other.
    assert [result.published for result in results] == [3, 3]
    assert sorted(publisher.published) == sorted(ids)


async def test_run_drains_the_outbox_and_stops_on_request(session_factory: SessionFactory) -> None:
    ids = await add_events(session_factory, 5)
    publisher = RecordingPublisher()
    relay = OutboxRelay(session_factory, publisher, batch_size=2, poll_interval_seconds=0.05)
    stop = asyncio.Event()

    task = asyncio.create_task(relay.run(stop))
    async with asyncio.timeout(5):
        while len(publisher.published) < len(ids):
            await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=5)

    assert publisher.published == ids
