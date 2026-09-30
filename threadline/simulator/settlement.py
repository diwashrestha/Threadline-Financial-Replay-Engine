"""Allocate financial movements to deterministic daily payouts."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date

from threadline.simulator.catalog import cents_to_eur
from threadline.simulator.fees import FeeFact
from threadline.simulator.orders_payments import CheckoutResult
from threadline.simulator.returns_refunds import RefundFact


MOVEMENT_ORDER = {
    "CAPTURE": 0,
    "FEE": 1,
    "REFUND": 2,
}


@dataclass(frozen=True, slots=True)
class Movement:
    movement_type: str
    movement_id: str
    available_on: date
    signed_amount_cents: int


@dataclass(frozen=True, slots=True)
class SettlementLineFact:
    settlement_line_id: str
    payout_id: str
    payout_date: date
    movement_type: str
    movement_id: str
    signed_amount_cents: int


@dataclass(frozen=True, slots=True)
class PayoutFact:
    payout_id: str
    payout_date: date
    reported_net_cents: int


@dataclass(frozen=True, slots=True)
class SettlementBook:
    movements: tuple[Movement, ...]
    lines: tuple[SettlementLineFact, ...]
    payouts: tuple[PayoutFact, ...]


def _signed_eur(cents: int) -> str:
    if cents < 0:
        return "-" + cents_to_eur(-cents)
    return cents_to_eur(cents)


def build_settlement_book(
    *,
    checkouts: Sequence[CheckoutResult],
    fees: Sequence[FeeFact],
    refunds: Sequence[RefundFact],
) -> SettlementBook:
    """Create one line per movement and one payout per active date."""

    captures = {}

    for checkout in checkouts:
        captured = [
            attempt
            for attempt in checkout.attempts
            if attempt.status == "CAPTURED"
        ]

        if len(captured) > 1:
            raise ValueError("Checkout has multiple captures")

        if not captured:
            continue

        capture = captured[0]

        if checkout.placed_order is None:
            raise ValueError("Capture has no placed order")
        if capture.available_on is None:
            raise ValueError("Capture has no available_on date")
        if capture.payment_id in captures:
            raise ValueError("Duplicate capture ID")

        captures[capture.payment_id] = capture

    fees_by_payment = {}

    for fee in fees:
        if fee.payment_id in fees_by_payment:
            raise ValueError("Multiple fees for one capture")
        if fee.payment_id not in captures:
            raise ValueError("Fee has no captured payment")
        if fee.amount_cents <= 0:
            raise ValueError("Fee must be positive")
        if (
            fee.available_on
            != captures[fee.payment_id].available_on
        ):
            raise ValueError(
                "Fee and capture have different settlement dates"
            )

        fees_by_payment[fee.payment_id] = fee

    if set(fees_by_payment) != set(captures):
        raise ValueError("Every capture needs exactly one fee")

    refund_ids = set()
    refunded_by_payment: Counter[str] = Counter()

    for refund in refunds:
        if refund.refund_id in refund_ids:
            raise ValueError("Duplicate refund ID")
        if refund.payment_id not in captures:
            raise ValueError("Refund has no captured payment")
        if refund.amount_cents <= 0:
            raise ValueError("Refund must be positive")
        if (
            refund.effective_at_utc
            <= captures[refund.payment_id].effective_at_utc
        ):
            raise ValueError("Refund precedes capture")
        if refund.available_on < (
            refund.effective_at_utc.date()
        ):
            raise ValueError(
                "Refund availability precedes refund"
            )

        refund_ids.add(refund.refund_id)
        refunded_by_payment[refund.payment_id] += (
            refund.amount_cents
        )

    for payment_id, refunded_cents in refunded_by_payment.items():
        if refunded_cents > (
            captures[payment_id].captured_amount_cents
        ):
            raise ValueError(
                "Cumulative refunds exceed capture"
            )

    movements = []

    for payment_id, capture in captures.items():
        fee = fees_by_payment[payment_id]

        movements.extend(
            (
                Movement(
                    movement_type="CAPTURE",
                    movement_id=payment_id,
                    available_on=capture.available_on,
                    signed_amount_cents=(
                        capture.captured_amount_cents
                    ),
                ),
                Movement(
                    movement_type="FEE",
                    movement_id=fee.fee_id,
                    available_on=capture.available_on,
                    signed_amount_cents=-fee.amount_cents,
                ),
            )
        )

    for refund in refunds:
        movements.append(
            Movement(
                movement_type="REFUND",
                movement_id=refund.refund_id,
                available_on=refund.available_on,
                signed_amount_cents=-refund.amount_cents,
            )
        )

    movements.sort(
        key=lambda movement: (
            movement.available_on,
            MOVEMENT_ORDER[movement.movement_type],
            movement.movement_id,
        )
    )

    movement_keys = [
        (movement.movement_type, movement.movement_id)
        for movement in movements
    ]
    if len(movement_keys) != len(set(movement_keys)):
        raise ValueError("A movement appears more than once")

    by_date = defaultdict(list)

    for movement in movements:
        by_date[movement.available_on].append(movement)

    lines = []
    payouts = []

    for payout_date in sorted(by_date):
        daily_movements = by_date[payout_date]
        daily_net_cents = sum(
            movement.signed_amount_cents
            for movement in daily_movements
        )

        if daily_net_cents < 0:
            raise ValueError(
                "Negative daily net on "
                f"{payout_date.isoformat()}; the current "
                "financial contract needs an explicit "
                "carryforward rule"
            )

        payout_id = f"OUT-{payout_date:%Y%m%d}"

        for movement in daily_movements:
            lines.append(
                SettlementLineFact(
                    settlement_line_id=(
                        f"SL-{movement.movement_type}-"
                        f"{movement.movement_id}"
                    ),
                    payout_id=payout_id,
                    payout_date=payout_date,
                    movement_type=movement.movement_type,
                    movement_id=movement.movement_id,
                    signed_amount_cents=(
                        movement.signed_amount_cents
                    ),
                )
            )

        payouts.append(
            PayoutFact(
                payout_id=payout_id,
                payout_date=payout_date,
                reported_net_cents=daily_net_cents,
            )
        )

    return SettlementBook(
        movements=tuple(movements),
        lines=tuple(lines),
        payouts=tuple(payouts),
    )


def settlement_line_source_record(
    line: SettlementLineFact,
) -> dict[str, object]:
    """Use the existing SETTLEMENT_LINE contract fields."""

    return {
        "settlement_line_id": line.settlement_line_id,
        "payout_id": line.payout_id,
        "movement_type": line.movement_type,
        "movement_id": line.movement_id,
        "signed_amount": _signed_eur(
            line.signed_amount_cents
        ),
        "currency": "EUR",
        "source_version": 1,
    }


def payout_source_record(
    payout: PayoutFact,
) -> dict[str, object]:
    """Use the existing PAYOUT contract fields."""

    return {
        "payout_id": payout.payout_id,
        "payout_date": payout.payout_date.isoformat(),
        "currency": "EUR",
        "reported_net_amount": cents_to_eur(
            payout.reported_net_cents
        ),
        "source_version": 1,
    }