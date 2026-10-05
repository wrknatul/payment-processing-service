from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from app.api.deps import require_api_key
from app.api.error_handlers import register_error_handlers
from app.api.middleware import TraceIdMiddleware
from app.api.routes import health, payments
from app.api.schemas import ErrorResponse
from app.config import Settings, get_settings
from app.db.session import create_engine, create_session_factory
from app.logging_config import setup_logging


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    engine = create_engine(app.state.settings.database_url)
    app.state.session_factory = create_session_factory(engine)
    try:
        yield
    finally:
        await engine.dispose()


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    setup_logging(settings.log_level)

    app = FastAPI(
        title="Payment Processing Service",
        description="Accepts payments and processes them asynchronously.",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.settings = settings

    app.add_middleware(TraceIdMiddleware)
    register_error_handlers(app)

    app.include_router(health.router)
    app.include_router(
        payments.router,
        prefix="/api/v1",
        dependencies=[Depends(require_api_key)],
        responses={401: {"model": ErrorResponse, "description": "Missing or invalid API key"}},
    )
    return app


app = create_app()
