from typing import Any


class AppError(Exception):
    """API-facing error; rendered in the unified error format by the exception handlers."""

    status_code: int = 500
    error_code: str = "INTERNAL_ERROR"
    message: str = "Internal server error"

    def __init__(
        self, message: str | None = None, *, details: dict[str, Any] | None = None
    ) -> None:
        if message is not None:
            self.message = message
        self.details = details or {}
        super().__init__(self.message)


class InvalidApiKeyError(AppError):
    status_code = 401
    error_code = "INVALID_API_KEY"
    message = "Missing or invalid API key"


class PaymentNotFoundError(AppError):
    status_code = 404
    error_code = "PAYMENT_NOT_FOUND"
    message = "Payment not found"


class IdempotencyKeyConflictError(AppError):
    status_code = 409
    error_code = "IDEMPOTENCY_KEY_CONFLICT"
    message = "Idempotency-Key was already used with a different request body"


class ProcessingError(Exception):
    """Failure while a consumer processes a message."""


class NonRetryableError(ProcessingError):
    """Retrying cannot help; the message goes straight to the dead letter queue."""


class WebhookDeliveryError(ProcessingError):
    """The webhook endpoint did not accept the notification; worth retrying."""
