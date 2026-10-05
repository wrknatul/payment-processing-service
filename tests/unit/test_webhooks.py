import hashlib
import hmac
import json
from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import pytest

from app.enums import PaymentStatus
from app.errors import WebhookDeliveryError
from app.services.webhooks import EVENT_ID_HEADER, SIGNATURE_HEADER, WebhookSender
from tests.factories import make_payment

SECRET = "test-secret"

Handler = Callable[[httpx.Request], httpx.Response]


def sender_with(handler: Handler) -> WebhookSender:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return WebhookSender(client, signing_secret=SECRET, timeout_seconds=1)


async def test_sends_signed_json_payload() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    payment = make_payment(
        status=PaymentStatus.SUCCEEDED.value,
        processed_at=datetime(2026, 1, 1, 12, 0, 4, tzinfo=UTC),
    )

    await sender_with(handler).send(payment)

    (request,) = requests
    assert request.method == "POST"
    assert str(request.url) == payment.webhook_url
    assert request.headers["Content-Type"] == "application/json"
    assert request.headers[EVENT_ID_HEADER] == str(payment.id)

    expected_signature = hmac.new(SECRET.encode(), request.content, hashlib.sha256).hexdigest()
    assert request.headers[SIGNATURE_HEADER] == f"sha256={expected_signature}"

    assert json.loads(request.content) == {
        "event_id": str(payment.id),
        "event_type": "payment.succeeded",
        "payment_id": str(payment.id),
        "status": "succeeded",
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": "42"},
        "created_at": "2026-01-01T12:00:00Z",
        "processed_at": "2026-01-01T12:00:04Z",
    }


async def test_failed_payment_is_reported_as_payment_failed() -> None:
    bodies: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        return httpx.Response(200)

    await sender_with(handler).send(make_payment(status=PaymentStatus.FAILED.value))

    payload = json.loads(bodies[0])
    assert payload["event_type"] == "payment.failed"
    assert payload["status"] == "failed"


@pytest.mark.parametrize("status_code", [301, 400, 404, 500, 503])
async def test_non_2xx_response_is_a_delivery_error(status_code: int) -> None:
    sender = sender_with(lambda request: httpx.Response(status_code))

    with pytest.raises(WebhookDeliveryError, match=str(status_code)):
        await sender.send(make_payment(status=PaymentStatus.SUCCEEDED.value))


@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectError("connection refused"),
        httpx.ReadTimeout("timed out"),
    ],
)
async def test_network_failure_is_a_delivery_error(error: httpx.HTTPError) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error

    with pytest.raises(WebhookDeliveryError, match=type(error).__name__):
        await sender_with(handler).send(make_payment(status=PaymentStatus.SUCCEEDED.value))
