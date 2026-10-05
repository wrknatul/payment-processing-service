import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import Protocol

from app.db.models import Payment
from app.enums import PaymentStatus


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> PaymentStatus:
        """Return the final status of the charge: succeeded or failed."""
        ...


class EmulatedPaymentGateway:
    """Stand-in for an external gateway: takes a few seconds and declines some payments.

    A declined payment is a business outcome (status `failed`), not an error to retry.
    """

    def __init__(
        self,
        *,
        min_delay_seconds: float = 2.0,
        max_delay_seconds: float = 5.0,
        success_rate: float = 0.9,
        rng: random.Random | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._min_delay = min_delay_seconds
        self._max_delay = max(max_delay_seconds, min_delay_seconds)
        self._success_rate = success_rate
        self._rng = rng or random.Random()
        self._sleep = sleep

    async def charge(self, payment: Payment) -> PaymentStatus:
        await self._sleep(self._rng.uniform(self._min_delay, self._max_delay))
        if self._rng.random() < self._success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
