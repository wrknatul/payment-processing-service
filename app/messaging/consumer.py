"""Payment consumer. Run with: faststream run app.messaging.consumer:app"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from faststream import FastStream

from app.config import Settings, get_settings
from app.db.repositories import SqlPaymentRepository
from app.db.session import create_engine, create_session_factory
from app.logging_config import setup_logging
from app.messaging.broker import build_broker
from app.messaging.handler import PaymentMessageHandler
from app.messaging.publishers import RabbitRetryPublisher
from app.messaging.retry import RetryPolicy
from app.messaging.subscriber import register_payment_handler
from app.messaging.topology import build_topology, declare_topology
from app.services.gateway import EmulatedPaymentGateway
from app.services.processor import PaymentProcessor
from app.services.webhooks import WebhookSender


def create_app(settings: Settings | None = None) -> FastStream:
    settings = settings or get_settings()
    setup_logging(settings.log_level)

    retry_policy = RetryPolicy.from_settings(settings)
    topology = build_topology(retry_policy)
    broker = build_broker(settings.rabbitmq_url, prefetch_count=settings.consumer_prefetch)

    # Neither object opens a connection until first use, so building them here is cheap.
    engine = create_engine(settings.database_url)
    http_client = httpx.AsyncClient()

    processor = PaymentProcessor(
        payments=SqlPaymentRepository(create_session_factory(engine)),
        gateway=EmulatedPaymentGateway(
            min_delay_seconds=settings.gateway_min_delay_seconds,
            max_delay_seconds=settings.gateway_max_delay_seconds,
            success_rate=settings.gateway_success_rate,
        ),
        webhooks=WebhookSender(
            http_client,
            signing_secret=settings.webhook_signing_secret,
            timeout_seconds=settings.webhook_timeout_seconds,
        ),
    )
    handler = PaymentMessageHandler(
        processor=processor,
        retry_policy=retry_policy,
        retry_publisher=RabbitRetryPublisher(broker, topology),
    )
    register_payment_handler(broker, topology=topology, handler=handler)

    @asynccontextmanager
    async def lifespan() -> AsyncIterator[None]:
        # Retry and dead letter queues must exist before the first message is consumed.
        await broker.connect()
        await declare_topology(broker, topology)
        try:
            yield
        finally:
            await http_client.aclose()
            await engine.dispose()

    return FastStream(broker, lifespan=lifespan)


app = create_app()
