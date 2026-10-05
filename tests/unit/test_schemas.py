from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from app.api.schemas import PaymentCreateRequest
from app.enums import Currency
from app.services.payments import NewPayment

VALID_BODY: dict[str, Any] = {
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order 42",
    "metadata": {"order_id": "42"},
    "webhook_url": "https://client.example/webhooks/payments",
}


def test_valid_body_is_accepted() -> None:
    request = PaymentCreateRequest.model_validate(VALID_BODY)

    assert request.amount == Decimal("1500.00")
    assert request.currency is Currency.RUB


def test_description_and_metadata_are_optional() -> None:
    request = PaymentCreateRequest.model_validate(
        {"amount": 10, "currency": "EUR", "webhook_url": "http://client.example/hook"}
    )

    assert request.description is None
    assert request.metadata == {}


@pytest.mark.parametrize(
    "override",
    [
        {"amount": "0"},
        {"amount": "-1.00"},
        {"amount": "10.001"},
        {"amount": "12345678901234567.00"},
        {"amount": "abc"},
        {"currency": "GBP"},
        {"currency": "rub"},
        {"webhook_url": "ftp://client.example/hook"},
        {"webhook_url": "not a url"},
        {"description": "x" * 501},
        {"metadata": ["not", "an", "object"]},
        {"unexpected": "field"},
    ],
)
def test_invalid_body_is_rejected(override: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate({**VALID_BODY, **override})


@pytest.mark.parametrize("missing", ["amount", "currency", "webhook_url"])
def test_required_fields(missing: str) -> None:
    body = {key: value for key, value in VALID_BODY.items() if key != missing}

    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(body)


def new_payment(**overrides: Any) -> NewPayment:
    values: dict[str, Any] = {
        "amount": Decimal("100.00"),
        "currency": Currency.RUB,
        "description": "Order 42",
        "metadata": {"a": 1, "b": 2},
        "webhook_url": "https://client.example/hook",
    }
    return NewPayment(**{**values, **overrides})


def test_fingerprint_ignores_amount_scale_and_metadata_key_order() -> None:
    assert (
        new_payment(amount=Decimal("100")).fingerprint()
        == new_payment(amount=Decimal("100.00")).fingerprint()
    )
    assert (
        new_payment(metadata={"b": 2, "a": 1}).fingerprint()
        == new_payment(metadata={"a": 1, "b": 2}).fingerprint()
    )


@pytest.mark.parametrize(
    "override",
    [
        {"amount": Decimal("100.01")},
        {"currency": Currency.USD},
        {"description": "Order 43"},
        {"metadata": {"a": 1}},
        {"webhook_url": "https://client.example/other"},
    ],
)
def test_fingerprint_changes_with_any_field(override: dict[str, Any]) -> None:
    assert new_payment(**override).fingerprint() != new_payment().fingerprint()
