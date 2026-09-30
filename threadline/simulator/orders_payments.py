"""Deterministic checkout, payment attempts, and captures."""

from __future__ import annotations

import re

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from random import Random
from typing import Literal

from threadline.simulator.catalog import OrderDraft, cents_to_eur


PaymentStatus = Literal["DECLINED", "CAPTURED"]
CheckoutOutcome = Literal[
    "FIRST_CAPTURE",
    "RETRY_CAPTURE",
    "ABANDONED",
]


@dataclass(frozen=True, slots=True)
class PaymentPolicy:
    version: str = "payments-v1"
    first_capture_percent: int = 88
    retry_capture_percent: int = 9
    abandoned_percent: int = 3
    retry_delay_seconds: int = 90
    settlement_lag_business_days: int = 2

    def __post_init__(self) -> None:
        if (
            self.first_capture_percent
            + self.retry_capture_percent
            + self.abandoned_percent
            != 100
        ):
            raise ValueError("Checkout outcome weights must sum to 100")

        if min(
            self.first_capture_percent,
            self.retry_capture_percent,
            self.abandoned_percent,
        ) < 0:
            raise ValueError("Checkout outcome weights cannot be negative")

        if self.retry_delay_seconds <= 0:
            raise ValueError("Retry delay must be positive")

        if self.settlement_lag_business_days < 0:
            raise ValueError("Settlement lag cannot be negative")


@dataclass(frozen=True, slots=True)
class PaymentAttempt:
    payment_id: str
    order_id: str
    attempt_number: int
    status: PaymentStatus
    attempted_amount_cents: int
    effective_at_utc: datetime
    available_on: date | None

    @property
    def captured_amount_cents(self) -> int:
        if self.status == "CAPTURED":
            return self.attempted_amount_cents
        return 0


@dataclass(frozen=True, slots=True)
class PlacedOrder:
    order_id: str
    amount_gross_cents: int
    placed_at_utc: datetime


@dataclass(frozen=True, slots=True)
class CheckoutResult:
    draft: OrderDraft
    outcome: CheckoutOutcome
    attempts: tuple[PaymentAttempt, ...]
    placed_order: PlacedOrder | None


def add_business_days(start: date, days: int) -> date:
    """Add weekdays; public holidays are outside the v1 policy."""

    result = start
    added = 0

    while added < days:
        result += timedelta(days=1)
        if result.weekday() < 5:
            added += 1

    return result


def _check_draft(draft: OrderDraft) -> None:
    if not re.fullmatch(r"ORD-\d{6}", draft.order_id):
        raise ValueError("Expected an ID like ORD-000001")

    if not draft.items or draft.order_total_gross_cents <= 0:
        raise ValueError("Order draft must have a positive total")

    for item in draft.items:
        if item.order_id != draft.order_id:
            raise ValueError("Item belongs to another order")

        if item.line_total_gross_cents != (
            item.quantity * item.unit_price_gross_cents
        ):
            raise ValueError("Item line total does not balance")

    if draft.order_total_gross_cents != sum(
        item.line_total_gross_cents
        for item in draft.items
    ):
        raise ValueError("Order total does not equal item totals")


def _select_outcome(
    *,
    order_id: str,
    seed: int,
    policy: PaymentPolicy,
) -> CheckoutOutcome:
    material = (
        f"{policy.version}|{seed}|{order_id}"
    ).encode("utf-8")

    rng = Random(
        int.from_bytes(sha256(material).digest(), "big")
    )
    draw = rng.randrange(100)

    if draw < policy.first_capture_percent:
        return "FIRST_CAPTURE"

    if draw < (
        policy.first_capture_percent
        + policy.retry_capture_percent
    ):
        return "RETRY_CAPTURE"

    return "ABANDONED"


def generate_checkout(
    *,
    draft: OrderDraft,
    seed: int,
    checkout_started_at_utc: datetime,
    policy: PaymentPolicy = PaymentPolicy(),
    forced_outcome: CheckoutOutcome | None = None,
) -> CheckoutResult:
    """Generate attempts without using the wall clock or random IDs."""

    _check_draft(draft)

    if seed < 0:
        raise ValueError("seed must be non-negative")

    if checkout_started_at_utc.tzinfo is None:
        raise ValueError("Checkout time must be timezone-aware")

    started_at = checkout_started_at_utc.astimezone(timezone.utc)

    if forced_outcome is None:
        outcome = _select_outcome(
            order_id=draft.order_id,
            seed=seed,
            policy=policy,
        )
    elif forced_outcome in (
        "FIRST_CAPTURE",
        "RETRY_CAPTURE",
        "ABANDONED",
    ):
        outcome = forced_outcome
    else:
        raise ValueError("Unknown checkout outcome")

    statuses: tuple[PaymentStatus, ...]

    if outcome == "FIRST_CAPTURE":
        statuses = ("CAPTURED",)
    elif outcome == "RETRY_CAPTURE":
        statuses = ("DECLINED", "CAPTURED")
    else:
        statuses = ("DECLINED",)

    attempts = []

    for attempt_number, status in enumerate(statuses, start=1):
        effective_at = started_at + timedelta(
            seconds=5 + (
                (attempt_number - 1)
                * policy.retry_delay_seconds
            )
        )

        available_on = (
            add_business_days(
                effective_at.date(),
                policy.settlement_lag_business_days,
            )
            if status == "CAPTURED"
            else None
        )

        attempts.append(
            PaymentAttempt(
                payment_id=(
                    f"PAY-{draft.order_id[4:]}"
                    f"-A{attempt_number:02d}"
                ),
                order_id=draft.order_id,
                attempt_number=attempt_number,
                status=status,
                attempted_amount_cents=(
                    draft.order_total_gross_cents
                ),
                effective_at_utc=effective_at,
                available_on=available_on,
            )
        )

    captured = [
        attempt
        for attempt in attempts
        if attempt.status == "CAPTURED"
    ]

    if len(captured) > 1:
        raise AssertionError("Checkout has multiple captures")

    captured_total = sum(
        attempt.captured_amount_cents
        for attempt in attempts
    )

    if captured_total > draft.order_total_gross_cents:
        raise AssertionError("Capture exceeds valid order total")

    placed_order = (
        PlacedOrder(
            order_id=draft.order_id,
            amount_gross_cents=draft.order_total_gross_cents,
            placed_at_utc=captured[0].effective_at_utc,
        )
        if captured
        else None
    )

    return CheckoutResult(
        draft=draft,
        outcome=outcome,
        attempts=tuple(attempts),
        placed_order=placed_order,
    )


def captured_payment_source_record(
    checkout: CheckoutResult,
) -> dict[str, object] | None:
    """Map the capture to Threadline's existing PAYMENT fields."""

    captured = [
        attempt
        for attempt in checkout.attempts
        if attempt.status == "CAPTURED"
    ]

    if not captured:
        return None

    attempt = captured[0]

    return {
        "payment_id": attempt.payment_id,
        "order_id": attempt.order_id,
        "attempt_number": attempt.attempt_number,
        "payment_method": "CARD",
        "status": "CAPTURED",
        "amount": cents_to_eur(
            attempt.captured_amount_cents
        ),
        "currency": "EUR",
        "effective_at_utc": (
            attempt.effective_at_utc
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        ),
        "available_on": attempt.available_on.isoformat(),
        "source_version": 1,
    }