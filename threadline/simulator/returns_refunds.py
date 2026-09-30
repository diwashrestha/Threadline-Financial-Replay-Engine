"""Deterministic item returns and capture-linked refunds."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from random import Random
from typing import Literal

from threadline.simulator.catalog import cents_to_eur
from threadline.simulator.orders_payments import (
    CheckoutResult,
    PaymentAttempt,
    add_business_days,
)


ReturnOutcome = Literal["NONE", "PARTIAL", "FULL"]


@dataclass(frozen=True, slots=True)
class ReturnPolicy:
    version: str = "returns-v1"
    return_percent: int = 12
    full_given_return_percent: int = 25
    max_first_return_delay_days: int = 21
    refund_processing_days: int = 2
    refund_settlement_business_days: int = 2

    def __post_init__(self) -> None:
        if not 0 <= self.return_percent <= 100:
            raise ValueError("return_percent must be 0..100")
        if not 0 <= self.full_given_return_percent <= 100:
            raise ValueError(
                "full_given_return_percent must be 0..100"
            )
        if self.max_first_return_delay_days < 1:
            raise ValueError("Return delay must be positive")
        if self.refund_processing_days < 1:
            raise ValueError("Refund processing delay must be positive")
        if self.refund_settlement_business_days < 0:
            raise ValueError("Settlement delay cannot be negative")


@dataclass(frozen=True, slots=True)
class ReturnedItem:
    order_item_id: str
    sku_id: str
    quantity: int
    gross_amount_cents: int


@dataclass(frozen=True, slots=True)
class ReturnEvent:
    return_id: str
    order_id: str
    returned_at_utc: datetime
    items: tuple[ReturnedItem, ...]


@dataclass(frozen=True, slots=True)
class RefundFact:
    refund_id: str
    payment_id: str
    amount_cents: int
    effective_at_utc: datetime
    available_on: date


@dataclass(frozen=True, slots=True)
class ReturnRefund:
    return_event: ReturnEvent
    refund: RefundFact


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("Business timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _capture(checkout: CheckoutResult) -> PaymentAttempt:
    captured = [
        attempt
        for attempt in checkout.attempts
        if attempt.status == "CAPTURED"
    ]

    if checkout.placed_order is None or len(captured) != 1:
        raise ValueError("Refund requires one placed, captured order")

    return captured[0]


def validate_return_history(
    checkout: CheckoutResult,
    history: Sequence[ReturnRefund],
) -> None:
    """Check item quantities, links, timing, and total refunded value."""

    if not history:
        return

    capture = _capture(checkout)
    purchased = {
        item.item_id: item
        for item in checkout.draft.items
    }
    returned_quantities: Counter[str] = Counter()
    refund_total = 0
    return_ids = set()
    refund_ids = set()

    for pair in history:
        event = pair.return_event
        refund = pair.refund

        if event.return_id in return_ids:
            raise ValueError("Duplicate return ID")
        if refund.refund_id in refund_ids:
            raise ValueError("Duplicate refund ID")

        return_ids.add(event.return_id)
        refund_ids.add(refund.refund_id)

        if event.order_id != checkout.draft.order_id:
            raise ValueError("Return belongs to another order")
        if refund.payment_id != capture.payment_id:
            raise ValueError("Refund is not linked to the capture")
        if event.returned_at_utc <= (
            checkout.placed_order.placed_at_utc
        ):
            raise ValueError("Return precedes purchase")
        if refund.effective_at_utc <= event.returned_at_utc:
            raise ValueError("Refund precedes return processing")
        if refund.available_on < refund.effective_at_utc.date():
            raise ValueError("Refund availability precedes refund")

        event_total = 0

        for returned in event.items:
            original = purchased.get(returned.order_item_id)

            if original is None:
                raise ValueError("Returned item is not in the order")
            if returned.sku_id != original.sku_id:
                raise ValueError("Returned SKU does not match order")
            if returned.quantity <= 0:
                raise ValueError("Returned quantity must be positive")

            expected_amount = (
                original.unit_price_gross_cents
                * returned.quantity
            )
            if returned.gross_amount_cents != expected_amount:
                raise ValueError("Returned item value is incorrect")

            returned_quantities[returned.order_item_id] += (
                returned.quantity
            )
            if (
                returned_quantities[returned.order_item_id]
                > original.quantity
            ):
                raise ValueError("An item was returned twice")

            event_total += returned.gross_amount_cents

        if event_total <= 0 or refund.amount_cents != event_total:
            raise ValueError("Refund does not equal returned value")

        refund_total += refund.amount_cents

    if refund_total > capture.captured_amount_cents:
        raise ValueError("Cumulative refunds exceed captured value")


def create_return_refund(
    *,
    checkout: CheckoutResult,
    existing: Sequence[ReturnRefund],
    quantities_by_item_id: Mapping[str, int],
    returned_at_utc: datetime,
    refund_at_utc: datetime,
    policy: ReturnPolicy = ReturnPolicy(),
) -> ReturnRefund:
    """Create one return and reject over-returns or over-refunds."""

    capture = _capture(checkout)

    if not quantities_by_item_id:
        raise ValueError("Return must contain at least one item")

    purchased = {
        item.item_id: item
        for item in checkout.draft.items
    }
    returned_items = []

    for item_id, quantity in sorted(quantities_by_item_id.items()):
        original = purchased.get(item_id)

        if original is None:
            raise ValueError(f"Unknown order item: {item_id}")
        if quantity <= 0:
            raise ValueError("Returned quantity must be positive")

        returned_items.append(
            ReturnedItem(
                order_item_id=item_id,
                sku_id=original.sku_id,
                quantity=quantity,
                gross_amount_cents=(
                    original.unit_price_gross_cents * quantity
                ),
            )
        )

    returned_at = _utc(returned_at_utc)
    refund_at = _utc(refund_at_utc)
    sequence_number = len(existing) + 1
    order_number = checkout.draft.order_id[4:]

    pair = ReturnRefund(
        return_event=ReturnEvent(
            return_id=(
                f"RET-{order_number}-{sequence_number:02d}"
            ),
            order_id=checkout.draft.order_id,
            returned_at_utc=returned_at,
            items=tuple(returned_items),
        ),
        refund=RefundFact(
            refund_id=(
                f"REF-{order_number}-{sequence_number:02d}"
            ),
            payment_id=capture.payment_id,
            amount_cents=sum(
                item.gross_amount_cents
                for item in returned_items
            ),
            effective_at_utc=refund_at,
            available_on=add_business_days(
                refund_at.date(),
                policy.refund_settlement_business_days,
            ),
        ),
    )

    validate_return_history(checkout, (*existing, pair))
    return pair


def generate_return_history(
    *,
    checkout: CheckoutResult,
    seed: int,
    policy: ReturnPolicy = ReturnPolicy(),
    forced_outcome: ReturnOutcome | None = None,
) -> tuple[ReturnRefund, ...]:
    """Generate zero, one, or two return/refund waves."""

    if seed < 0:
        raise ValueError("seed must be non-negative")

    if checkout.placed_order is None:
        if forced_outcome not in (None, "NONE"):
            raise ValueError("Abandoned checkout cannot be returned")
        return ()

    capture = _capture(checkout)

    material = (
        f"{policy.version}|{seed}|{checkout.draft.order_id}"
    ).encode("utf-8")
    rng = Random(
        int.from_bytes(sha256(material).digest(), "big")
    )

    if forced_outcome is None:
        if rng.randrange(100) >= policy.return_percent:
            outcome: ReturnOutcome = "NONE"
        elif (
            rng.randrange(100)
            < policy.full_given_return_percent
        ):
            outcome = "FULL"
        else:
            outcome = "PARTIAL"
    elif forced_outcome in ("NONE", "PARTIAL", "FULL"):
        outcome = forced_outcome
    else:
        raise ValueError("Unknown return outcome")

    if outcome == "NONE":
        return ()

    # Each entry represents one purchased unit. Sampling distinct
    # entries prevents refunding the same unit twice.
    units = [
        item.item_id
        for item in checkout.draft.items
        for _ in range(item.quantity)
    ]

    if outcome == "PARTIAL" and len(units) == 1:
        if forced_outcome == "PARTIAL":
            raise ValueError(
                "A one-unit order cannot have an item-based "
                "partial return"
            )
        outcome = "FULL"

    if outcome == "FULL":
        selected = units
    else:
        selected_count = rng.randrange(1, len(units))
        indexes = sorted(
            rng.sample(range(len(units)), selected_count)
        )
        selected = [units[index] for index in indexes]

    # Some partial returns arrive in two separate waves.
    if (
        outcome == "PARTIAL"
        and len(selected) >= 2
        and rng.randrange(4) == 0
    ):
        split = rng.randrange(1, len(selected))
        waves = (selected[:split], selected[split:])
    else:
        waves = (selected,)

    first_return_at = (
        capture.effective_at_utc
        + timedelta(
            days=1 + rng.randrange(
                policy.max_first_return_delay_days
            )
        )
    )

    history: list[ReturnRefund] = []

    for wave_index, wave in enumerate(waves):
        returned_at = first_return_at + timedelta(
            days=7 * wave_index
        )
        refund_at = returned_at + timedelta(
            days=policy.refund_processing_days
        )

        pair = create_return_refund(
            checkout=checkout,
            existing=history,
            quantities_by_item_id=Counter(wave),
            returned_at_utc=returned_at,
            refund_at_utc=refund_at,
            policy=policy,
        )
        history.append(pair)

    validate_return_history(checkout, history)
    return tuple(history)


def refund_source_record(
    pair: ReturnRefund,
) -> dict[str, object]:
    """Map a successful refund to the current REFUND contract."""

    refund = pair.refund

    return {
        "refund_id": refund.refund_id,
        "payment_id": refund.payment_id,
        "status": "SUCCEEDED",
        "amount": cents_to_eur(refund.amount_cents),
        "currency": "EUR",
        "effective_at_utc": (
            refund.effective_at_utc
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        ),
        "available_on": refund.available_on.isoformat(),
        "source_version": 1,
    }