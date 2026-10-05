import logging
from http import HTTPStatus
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from app.api.middleware import TRACE_HEADER, get_trace_id
from app.api.schemas import ErrorResponse
from app.errors import AppError

logger = logging.getLogger(__name__)


def error_response(
    request: Request,
    *,
    status_code: int,
    error_code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    trace_id = get_trace_id(request)
    body = ErrorResponse(
        error_code=error_code, message=message, trace_id=trace_id, details=details or {}
    )
    # The header is set here as well: the handler for unexpected exceptions runs outside of the
    # middleware stack, where TraceIdMiddleware cannot add it.
    return JSONResponse(
        status_code=status_code,
        content=body.model_dump(mode="json"),
        headers={TRACE_HEADER: trace_id},
    )


async def handle_app_error(request: Request, exc: AppError) -> JSONResponse:
    return error_response(
        request,
        status_code=exc.status_code,
        error_code=exc.error_code,
        message=exc.message,
        details=exc.details,
    )


async def handle_http_exception(request: Request, exc: HTTPException) -> JSONResponse:
    return error_response(
        request,
        status_code=exc.status_code,
        error_code=HTTPStatus(exc.status_code).name,
        message=str(exc.detail),
    )


async def handle_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    # Only loc/msg/type are returned: the raw pydantic errors also carry the rejected input and
    # exception objects, which are neither safe to echo nor always serializable.
    errors = [
        {"loc": list(error["loc"]), "msg": error["msg"], "type": error["type"]}
        for error in exc.errors()
    ]
    return error_response(
        request,
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        error_code="VALIDATION_ERROR",
        message="Request validation failed",
        details={"errors": errors},
    )


async def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    logger.error(
        "unhandled error, trace_id=%s: %s %s",
        get_trace_id(request),
        request.method,
        request.url.path,
        exc_info=exc,
    )
    return error_response(
        request,
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        error_code="INTERNAL_ERROR",
        message="Internal server error",
    )


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(AppError, handle_app_error)
    app.add_exception_handler(HTTPException, handle_http_exception)
    app.add_exception_handler(RequestValidationError, handle_validation_error)
    app.add_exception_handler(Exception, handle_unexpected_error)
