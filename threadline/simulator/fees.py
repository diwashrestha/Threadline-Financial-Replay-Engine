"""Generate processing fees using Threadline's existing fee rule."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from threadline.money import calculate_fee
from threadline.simulator.catalog import cents_to_eur
from threadline.simulator.orders_payments import (
    CheckoutResult,
    captured_payment_source_record,
)


FEE_GENERATOR_VERSION = "fees-v1"


@dataclass(frozen=True, slots=True)
class FeeFact:
    fee_id: str
    payment_id: str
    fee_type: str
    amount_cents: int
    currency: str
    effective_at_utc: datetime
    available_on: date
    generator_version: str


def _exact_cents(amount: Decimal) -> int:
    """Reject a result that has not been rounded to cents."""

    if not isinstance(amount, Decimal):
        raise TypeError("calculate_fee must return Decimal")

    cents = amount * Decimal("100")

    if cents != cents.to_integral_value():
        raise ValueError(
            "calculate_fee returned a sub-cent amount"
        )

    if cents <= 0:
        raise ValueError(
            "The current FEE contract requires a positive amount"
        )

    return int(cents)


def generate_processing_fee(
    checkout: CheckoutResult,
) -> FeeFact | None:
    """Create exactly one fee for a successful capture."""

    captures = [
        attempt
        for attempt in checkout.attempts
        if attempt.status == "CAPTURED"
    ]

    if not captures:
        return None

    if len(captures) != 1:
        raise ValueError("Expected exactly one capture")

    capture = captures[0]

    if capture.available_on is None:
        raise ValueError("Capture has no settlement date")

    # Calculate from the same values that the financial contract
    # will parse, using the same function as reconcile.py.
    payment_record = captured_payment_source_record(
        checkout
    )
    if payment_record is None:
        raise ValueError("Capture has no PAYMENT source record")

    currency = str(payment_record["currency"])
    fee_amount = calculate_fee(
        str(payment_record["payment_method"]),
        Decimal(str(payment_record["amount"])),
        currency=currency,
    )

    return FeeFact(
        fee_id=(
            "FEE-"
            + capture.payment_id.removeprefix("PAY-")
        ),
        payment_id=capture.payment_id,
        fee_type="PROCESSING",
        amount_cents=_exact_cents(fee_amount),
        currency=currency,
        effective_at_utc=capture.effective_at_utc,
        # The reconciler expects the fee movement on the
        # captured payment's available_on date.
        available_on=capture.available_on,
        generator_version=FEE_GENERATOR_VERSION,
    )


def fee_source_record(fee: FeeFact) -> dict[str, object]:
    """Map the internal fee to the existing FEE contract."""

    return {
        "fee_id": fee.fee_id,
        "payment_id": fee.payment_id,
        "fee_type": fee.fee_type,
        "amount": cents_to_eur(fee.amount_cents),
        "currency": fee.currency,
        "effective_at_utc": (
            fee.effective_at_utc
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        ),
        "available_on": fee.available_on.isoformat(),
        "source_version": 1,
    }