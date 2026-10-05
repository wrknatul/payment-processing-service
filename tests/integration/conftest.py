import asyncio
import os
import sys
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import aio_pika
import asyncpg
import httpx
import pytest
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustChannel
from faststream.rabbit import RabbitBroker
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.api.main import create_app
from app.config import Settings
from app.db.session import SessionFactory, create_engine, create_session_factory
from app.messaging.broker import build_broker
from app.messaging.events import PaymentCreatedEvent
from app.messaging.handler import PaymentMessageHandler
from app.messaging.publishers import RabbitEventPublisher, RabbitRetryPublisher
from app.messaging.retry import RetryPolicy
from app.messaging.subscriber import register_payment_handler
from app.messaging.topology import Topology, build_topology, declare_topology

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")
TEST_RABBITMQ_URL = os.environ.get("TEST_RABBITMQ_URL")
API_KEY = "test-api-key"

# Short delays keep the retry tests fast while leaving the two steps clearly distinguishable.
TEST_RETRY_POLICY = RetryPolicy(max_attempts=3, base_delay_seconds=0.2, multiplier=3)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "integration" in item.path.parts:
            item.add_marker(pytest.mark.integration)


# --- PostgreSQL ---------------------------------------------------------------------------------


async def _create_database_if_missing(database_url: str) -> None:
    url = make_url(database_url)
    connection = await asyncpg.connect(
        host=url.host, port=url.port, user=url.username, password=url.password, database="postgres"
    )
    try:
        exists = await connection.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", url.database
        )
        if not exists:
            await connection.execute(f'CREATE DATABASE "{url.database}"')
    finally:
        await connection.close()


async def _migrate(database_url: str) -> None:
    # Alembic's async env calls asyncio.run(), so it needs a process of its own.
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "alembic",
        "upgrade",
        "head",
        cwd=PROJECT_ROOT,
        env={**os.environ, "DATABASE_URL": database_url},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    output, _ = await process.communicate()
    assert process.returncode == 0, output.decode()


@pytest.fixture(scope="session")
async def database_url() -> str:
    if not TEST_DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    await _create_database_if_missing(TEST_DATABASE_URL)
    await _migrate(TEST_DATABASE_URL)
    return TEST_DATABASE_URL


@pytest.fixture(scope="session")
async def engine(database_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(database_url)
    yield engine
    await engine.dispose()


@pytest.fixture
async def session_factory(engine: AsyncEngine) -> SessionFactory:
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox"))
    return create_session_factory(engine)


@pytest.fixture
def settings(database_url: str) -> Settings:
    return Settings(_env_file=None, database_url=database_url, api_key=API_KEY)


@pytest.fixture
async def anonymous_client(
    settings: Settings, session_factory: SessionFactory
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings)
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://test") as client,
    ):
        yield client


@pytest.fixture
def client(anonymous_client: httpx.AsyncClient) -> httpx.AsyncClient:
    anonymous_client.headers["X-API-Key"] = API_KEY
    return anonymous_client


# --- RabbitMQ -----------------------------------------------------------------------------------


@dataclass
class ScriptedProcessor:
    """Raises the scripted errors one per call, then succeeds (or keeps failing with `always`)."""

    errors: list[Exception] = field(default_factory=list)
    always: Exception | None = None
    calls: list[float] = field(default_factory=list)

    async def process(self, payment_id: uuid.UUID) -> None:
        self.calls.append(time.monotonic())
        if self.always is not None:
            raise self.always
        if self.errors:
            raise self.errors.pop(0)

    async def wait_for_calls(self, count: int, timeout: float = 10.0) -> None:
        async with asyncio.timeout(timeout):
            while len(self.calls) < count:
                await asyncio.sleep(0.02)


@dataclass
class RabbitEnv:
    broker: RabbitBroker
    topology: Topology
    channel: AbstractRobustChannel
    processor: ScriptedProcessor

    async def publish_event(self) -> PaymentCreatedEvent:
        event = PaymentCreatedEvent(
            event_id=uuid.uuid4(), payment_id=uuid.uuid4(), occurred_at=datetime.now(UTC)
        )
        await RabbitEventPublisher(self.broker, self.topology).publish(
            message_id=str(event.event_id),
            routing_key=self.topology.queue.routing_key,
            payload=event.model_dump(mode="json"),
        )
        return event

    async def publish_raw(self, body: bytes) -> None:
        exchange = await self.channel.get_exchange(self.topology.exchange.name)
        await exchange.publish(aio_pika.Message(body), routing_key=self.topology.queue.routing_key)

    async def get_message(self, queue_name: str, timeout: float = 10.0) -> AbstractIncomingMessage:
        queue = await self.channel.get_queue(queue_name)
        async with asyncio.timeout(timeout):
            while True:
                message = await queue.get(no_ack=True, fail=False)
                if message is not None:
                    return message
                await asyncio.sleep(0.05)

    async def get_dead_letter(self, timeout: float = 10.0) -> AbstractIncomingMessage:
        return await self.get_message(self.topology.dead_letter_queue.name, timeout)

    async def dead_letter_queue_stays_empty(self, settle_seconds: float = 0.5) -> bool:
        await asyncio.sleep(settle_seconds)
        queue = await self.channel.get_queue(self.topology.dead_letter_queue.name)
        return await queue.get(no_ack=True, fail=False) is None


async def _drop_topology(channel: AbstractRobustChannel, topology: Topology) -> None:
    queues = [topology.queue, *topology.retry_queues.values(), topology.dead_letter_queue]
    exchanges = [topology.exchange, topology.retry_exchange, topology.dead_letter_exchange]
    for queue in queues:
        await channel.queue_delete(queue.name)
    for exchange in exchanges:
        await channel.exchange_delete(exchange.name)


@pytest.fixture
async def rabbit(request: pytest.FixtureRequest) -> AsyncIterator[RabbitEnv]:
    """A broker with its own uniquely named topology.

    By default the payment subscriber is running with a ScriptedProcessor behind it; mark a test
    with `@pytest.mark.no_consumer` to get the topology alone and inspect the queues directly.
    """
    if not TEST_RABBITMQ_URL:
        pytest.skip("TEST_RABBITMQ_URL is not set")

    topology = build_topology(TEST_RETRY_POLICY, prefix=f"test-{uuid.uuid4().hex[:8]}.")
    broker = build_broker(TEST_RABBITMQ_URL, prefetch_count=10)
    processor = ScriptedProcessor()
    with_consumer = request.node.get_closest_marker("no_consumer") is None
    if with_consumer:
        handler = PaymentMessageHandler(
            processor=processor,
            retry_policy=TEST_RETRY_POLICY,
            retry_publisher=RabbitRetryPublisher(broker, topology),
        )
        register_payment_handler(broker, topology=topology, handler=handler)

    # A separate plain connection observes the queues independently of the broker under test.
    connection = await aio_pika.connect_robust(TEST_RABBITMQ_URL)
    channel = await connection.channel()

    await broker.connect()
    await declare_topology(broker, topology)
    if with_consumer:
        await broker.start()
    try:
        yield RabbitEnv(broker=broker, topology=topology, channel=channel, processor=processor)
    finally:
        await broker.stop()
        await _drop_topology(channel, topology)
        await connection.close()
