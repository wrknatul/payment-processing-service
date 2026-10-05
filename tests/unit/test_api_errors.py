"""Error paths of the API that need no running database."""

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from app.api.main import create_app
from app.config import Settings

API_KEY = "test-api-key"
# Nothing listens on port 1, so every connection attempt is refused at once.
UNREACHABLE_DATABASE_URL = "postgresql+asyncpg://payments:payments@127.0.0.1:1/payments"

BODY: dict[str, Any] = {
    "amount": "10.00",
    "currency": "RUB",
    "webhook_url": "https://client.example/hook",
}


@pytest.fixture
def app() -> FastAPI:
    return create_app(
        Settings(_env_file=None, database_url=UNREACHABLE_DATABASE_URL, api_key=API_KEY)
    )


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    # raise_app_exceptions=False: get the 500 response a real client would, not the exception.
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=transport, base_url="http://test", headers={"X-API-Key": API_KEY}
        ) as client,
    ):
        yield client


def assert_error(response: httpx.Response, status_code: int, error_code: str) -> dict[str, Any]:
    assert response.status_code == status_code
    body = response.json()
    assert set(body) == {"success", "error_code", "message", "trace_id", "details"}
    assert body["success"] is False
    assert body["error_code"] == error_code
    assert body["trace_id"] == response.headers["X-Request-ID"]
    return body


async def test_unreachable_database_is_reported_as_503(client: httpx.AsyncClient) -> None:
    created = await client.post("/api/v1/payments", json=BODY, headers={"Idempotency-Key": "k"})
    fetched = await client.get("/api/v1/payments/00000000-0000-0000-0000-000000000000")

    for response in (created, fetched):
        body = assert_error(response, 503, "SERVICE_UNAVAILABLE")
        assert "127.0.0.1" not in response.text
        assert body["details"] == {}


async def test_health_reports_unreachable_database(client: httpx.AsyncClient) -> None:
    response = await client.get("/health")

    assert response.status_code == 503
    assert response.json() == {"status": "unavailable"}


async def test_validation_and_auth_do_not_need_the_database(client: httpx.AsyncClient) -> None:
    invalid = await client.post(
        "/api/v1/payments", json={**BODY, "amount": "-1"}, headers={"Idempotency-Key": "k"}
    )
    unauthorized = await client.get(
        "/api/v1/payments/00000000-0000-0000-0000-000000000000", headers={"X-API-Key": "wrong"}
    )

    assert_error(invalid, 422, "VALIDATION_ERROR")
    assert_error(unauthorized, 401, "INVALID_API_KEY")


async def test_unexpected_error_is_a_500_without_internal_details(
    app: FastAPI, client: httpx.AsyncClient
) -> None:
    @app.get("/boom")
    async def boom() -> None:
        raise RuntimeError("secret internal detail")

    response = await client.get("/boom", headers={"X-Request-ID": "trace-1"})

    body = assert_error(response, 500, "INTERNAL_ERROR")
    assert body["trace_id"] == "trace-1"
    assert "secret internal detail" not in response.text
