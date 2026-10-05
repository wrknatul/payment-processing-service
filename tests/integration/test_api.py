import asyncio
import uuid
from typing import Any

import httpx
from sqlalchemy import func, select

from app.db.models import OutboxEvent, Payment
from app.db.session import SessionFactory

BODY: dict[str, Any] = {
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order 42",
    "metadata": {"order_id": "42"},
    "webhook_url": "https://client.example/webhooks/payments",
}


async def create(client: httpx.AsyncClient, key: str = "key-1", **overrides: Any) -> httpx.Response:
    return await client.post(
        "/api/v1/payments", json={**BODY, **overrides}, headers={"Idempotency-Key": key}
    )


async def count(session_factory: SessionFactory, model: type) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(model)) or 0


def assert_error(response: httpx.Response, status_code: int, error_code: str) -> dict[str, Any]:
    assert response.status_code == status_code
    body = response.json()
    assert set(body) == {"success", "error_code", "message", "trace_id", "details"}
    assert body["success"] is False
    assert body["error_code"] == error_code
    assert body["trace_id"] == response.headers["X-Request-ID"]
    return body


async def test_create_stores_payment_and_outbox_event(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    response = await create(client)

    assert response.status_code == 202
    body = response.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    assert body["status"] == "pending"
    assert "Idempotent-Replayed" not in response.headers

    async with session_factory() as session:
        payment = (await session.scalars(select(Payment))).one()
        event = (await session.scalars(select(OutboxEvent))).one()

    assert str(payment.id) == body["payment_id"]
    assert payment.status == "pending"
    assert payment.idempotency_key == "key-1"
    assert payment.processed_at is None

    assert event.published_at is None
    assert event.event_type == "payment.created"
    assert event.routing_key == "payments.new"
    assert event.payload["payment_id"] == body["payment_id"]
    assert event.payload["event_id"] == str(event.id)


async def test_replay_returns_the_same_payment_without_a_second_event(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    first = await create(client)
    # 1500 and 1500.00 are the same request.
    second = await create(client, amount=1500)

    assert second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxEvent) == 1


async def test_same_key_with_another_body_is_a_conflict(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    await create(client)

    response = await create(client, amount="999.00")

    assert_error(response, 409, "IDEMPOTENCY_KEY_CONFLICT")
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxEvent) == 1


async def test_concurrent_requests_with_one_key_create_one_payment(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    responses = await asyncio.gather(*(create(client) for _ in range(20)))

    assert {response.status_code for response in responses} == {202}
    assert len({response.json()["payment_id"] for response in responses}) == 1
    replayed = [r for r in responses if r.headers.get("Idempotent-Replayed") == "true"]
    assert len(replayed) == 19
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxEvent) == 1


async def test_different_keys_create_different_payments(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    first = await create(client, key="key-1")
    second = await create(client, key="key-2")

    assert first.json()["payment_id"] != second.json()["payment_id"]
    assert await count(session_factory, Payment) == 2
    assert await count(session_factory, OutboxEvent) == 2


async def test_idempotency_key_is_required(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    response = await client.post("/api/v1/payments", json=BODY)

    body = assert_error(response, 422, "VALIDATION_ERROR")
    assert body["details"]["errors"][0]["loc"] == ["header", "Idempotency-Key"]
    assert await count(session_factory, Payment) == 0


async def test_invalid_body_is_rejected_without_side_effects(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    response = await create(client, amount="-5", currency="GBP")

    body = assert_error(response, 422, "VALIDATION_ERROR")
    assert {tuple(error["loc"]) for error in body["details"]["errors"]} == {
        ("body", "amount"),
        ("body", "currency"),
    }
    assert await count(session_factory, Payment) == 0
    assert await count(session_factory, OutboxEvent) == 0


async def test_get_returns_payment_details(client: httpx.AsyncClient) -> None:
    created = (await create(client)).json()

    response = await client.get(f"/api/v1/payments/{created['payment_id']}")

    assert response.status_code == 200
    assert response.json() == {
        "payment_id": created["payment_id"],
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": "42"},
        "status": "pending",
        "idempotency_key": "key-1",
        "webhook_url": "https://client.example/webhooks/payments",
        "webhook_delivered_at": None,
        "created_at": created["created_at"],
        "processed_at": None,
    }


async def test_get_unknown_payment_is_404(client: httpx.AsyncClient) -> None:
    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}")

    assert_error(response, 404, "PAYMENT_NOT_FOUND")


async def test_get_with_malformed_id_is_422(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/payments/not-a-uuid")

    assert_error(response, 422, "VALIDATION_ERROR")


async def test_api_key_is_required_for_every_payment_endpoint(
    anonymous_client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    for headers in ({}, {"X-API-Key": "wrong"}):
        post = await anonymous_client.post(
            "/api/v1/payments", json=BODY, headers={**headers, "Idempotency-Key": "key-1"}
        )
        get = await anonymous_client.get(f"/api/v1/payments/{uuid.uuid4()}", headers=headers)

        assert_error(post, 401, "INVALID_API_KEY")
        assert_error(get, 401, "INVALID_API_KEY")

    assert await count(session_factory, Payment) == 0


async def test_health_is_open(anonymous_client: httpx.AsyncClient) -> None:
    response = await anonymous_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_unknown_route_uses_the_error_format(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/nothing-here")

    assert_error(response, 404, "NOT_FOUND")


async def test_trace_id_is_taken_from_the_request_or_generated(client: httpx.AsyncClient) -> None:
    given = await client.get(f"/api/v1/payments/{uuid.uuid4()}", headers={"X-Request-ID": "abc"})
    generated = await create(client)

    assert given.headers["X-Request-ID"] == "abc"
    assert given.json()["trace_id"] == "abc"
    assert uuid.UUID(generated.headers["X-Request-ID"])
