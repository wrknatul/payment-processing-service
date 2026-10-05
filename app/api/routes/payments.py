import uuid

from fastapi import APIRouter, Response, status

from app.api.deps import IdempotencyKey, PaymentServiceDep
from app.api.schemas import (
    ErrorResponse,
    PaymentAcceptedResponse,
    PaymentCreateRequest,
    PaymentResponse,
)
from app.enums import PaymentStatus
from app.services.payments import NewPayment

router = APIRouter(tags=["payments"])

REPLAYED_HEADER = "Idempotent-Replayed"


@router.post(
    "/payments",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        409: {"model": ErrorResponse, "description": "Idempotency-Key reused with another body"},
        422: {"model": ErrorResponse, "description": "Invalid body or missing Idempotency-Key"},
    },
)
async def create_payment(
    body: PaymentCreateRequest,
    idempotency_key: IdempotencyKey,
    response: Response,
    service: PaymentServiceDep,
) -> PaymentAcceptedResponse:
    """Accept a payment for asynchronous processing.

    Repeating the request with the same Idempotency-Key and body returns the original payment.
    """
    result = await service.create(
        NewPayment(
            amount=body.amount,
            currency=body.currency,
            description=body.description,
            metadata=body.metadata,
            webhook_url=str(body.webhook_url),
        ),
        idempotency_key,
    )
    if result.replayed:
        response.headers[REPLAYED_HEADER] = "true"
    return PaymentAcceptedResponse(
        payment_id=result.payment.id,
        status=PaymentStatus(result.payment.status),
        created_at=result.payment.created_at,
    )


@router.get(
    "/payments/{payment_id}",
    responses={404: {"model": ErrorResponse, "description": "Payment not found"}},
)
async def get_payment(payment_id: uuid.UUID, service: PaymentServiceDep) -> PaymentResponse:
    return PaymentResponse.from_payment(await service.get(payment_id))
