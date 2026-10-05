import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, Request, Security
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from app.errors import InvalidApiKeyError
from app.services.payments import PaymentService

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(
    request: Request, api_key: Annotated[str | None, Security(api_key_header)]
) -> None:
    expected: str = request.app.state.settings.api_key
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected.encode()):
        raise InvalidApiKeyError


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.session_factory() as session:
        yield session


def get_payment_service(session: Annotated[AsyncSession, Depends(get_session)]) -> PaymentService:
    return PaymentService(session)


SessionDep = Annotated[AsyncSession, Depends(get_session)]
PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]
IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)]
