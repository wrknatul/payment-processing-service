import random

from app.enums import PaymentStatus
from app.services.gateway import EmulatedPaymentGateway
from tests.factories import make_payment


class SleepRecorder:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


async def test_delay_stays_within_bounds_and_success_rate_is_about_ninety_percent() -> None:
    sleep = SleepRecorder()
    gateway = EmulatedPaymentGateway(
        min_delay_seconds=2,
        max_delay_seconds=5,
        success_rate=0.9,
        rng=random.Random(7),
        sleep=sleep,
    )
    payment = make_payment()

    results = [await gateway.charge(payment) for _ in range(2000)]

    assert all(2 <= delay <= 5 for delay in sleep.delays)
    assert set(results) == {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}
    assert 0.87 <= results.count(PaymentStatus.SUCCEEDED) / len(results) <= 0.93


async def test_success_rate_extremes() -> None:
    sleep = SleepRecorder()
    payment = make_payment()

    always = EmulatedPaymentGateway(success_rate=1.0, sleep=sleep)
    never = EmulatedPaymentGateway(success_rate=0.0, sleep=sleep)

    assert await always.charge(payment) is PaymentStatus.SUCCEEDED
    assert await never.charge(payment) is PaymentStatus.FAILED
