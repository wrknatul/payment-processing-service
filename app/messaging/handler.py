import logging
import uuid
from typing import Protocol

from app.errors import NonRetryableError
from app.messaging.events import PaymentCreatedEvent
from app.messaging.retry import RetryPolicy

logger = logging.getLogger(__name__)

MAX_ERROR_LENGTH = 500


class Processor(Protocol):
    async def process(self, payment_id: uuid.UUID) -> None: ...


class RetryPublisher(Protocol):
    async def schedule(
        self, *, body: bytes, message_id: str | None, delay_ms: int, attempt: int, error: str
    ) -> None: ...


class PaymentMessageHandler:
    """Runs the processor and decides what happens to a message that failed.

    Returning normally acks the message; raising rejects it without requeue, which sends it to the
    dead letter queue.
    """

    def __init__(
        self, *, processor: Processor, retry_policy: RetryPolicy, retry_publisher: RetryPublisher
    ) -> None:
        self._processor = processor
        self._retry_policy = retry_policy
        self._retry_publisher = retry_publisher

    async def handle(
        self, event: PaymentCreatedEvent, *, attempt: int, body: bytes, message_id: str | None
    ) -> None:
        payment_id = event.payment_id
        max_attempts = self._retry_policy.max_attempts
        try:
            await self._processor.process(payment_id)
        except NonRetryableError as exc:
            logger.error("payment %s: non-retryable failure, dead-lettering: %s", payment_id, exc)
            raise
        except Exception as exc:
            delay_ms = self._retry_policy.delay_ms_after(attempt)
            if delay_ms is None:
                logger.error(
                    "payment %s: attempt %d/%d failed, retries exhausted, dead-lettering: %s",
                    payment_id,
                    attempt,
                    max_attempts,
                    exc,
                )
                raise
            logger.warning(
                "payment %s: attempt %d/%d failed, retrying in %d ms: %s",
                payment_id,
                attempt,
                max_attempts,
                delay_ms,
                exc,
            )
            # The retry copy is confirmed by the broker before the original is acked. A crash in
            # between redelivers the original, so a payment may be handled twice but never lost;
            # the processor is idempotent for that reason.
            try:
                await self._retry_publisher.schedule(
                    body=body,
                    message_id=message_id,
                    delay_ms=delay_ms,
                    attempt=attempt + 1,
                    error=f"{type(exc).__name__}: {exc}"[:MAX_ERROR_LENGTH],
                )
            except Exception:
                # The retry could not be parked. Dead-lettering keeps the message for a manual
                # replay; requeueing it instead would redeliver it in a tight loop for as long
                # as the broker refuses publishes.
                logger.exception(
                    "payment %s: could not schedule a retry, dead-lettering", payment_id
                )
                raise
