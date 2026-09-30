"""Independent financial oracle for Stage 6 simulator exports.

This module reads raw report records. It does not call Threadline's
canonicalizer, reconciler, or fingerprint implementation.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Iterable, Mapping


ID_FIELD = {
    "ORDERS": "order_id",
    "PAYMENTS": "payment_id",
    "REFUNDS": "refund_id",
    "FEES": "fee_id",
    "SETTLEMENT_LINES": "settlement_line_id",
    "PAYOUTS": "payout_id",
}

MONEY_FIELD = {
    "ORDERS": "order_total",
    "PAYMENTS": "amount",
    "REFUNDS": "amount",
    "FEES": "amount",
    "SETTLEMENT_LINES": "signed_amount",
    "PAYOUTS": "reported_net_amount",
}

CENT = Decimal("0.01")


def cents(value: Any) -> int:
    """Parse an exact, two-decimal monetary value into integer cents."""
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid monetary value: {value!r}") from exc

    if not amount.is_finite():
        raise ValueError(f"Non-finite monetary value: {value!r}")

    scaled = amount * 100
    if scaled != scaled.to_integral_value():
        raise ValueError(f"More than two decimal places: {value!r}")

    return int(scaled)


def euros(value_cents: int) -> str:
    return format(Decimal(value_cents) / 100, ".2f")


def fee_cents(
    captured_cents: int,
    payment_method: str,
    fee_rules: Mapping[str, tuple[int, int]],
) -> int:
    """Calculate fee independently: basis points plus fixed cents."""
    basis_points, fixed_cents = fee_rules[payment_method]
    percentage_cents = (
        Decimal(captured_cents) * basis_points / 10_000
    ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(percentage_cents) + fixed_cents


def _report_name(value: Any) -> str:
    return str(getattr(value, "value", value)).upper()


def _payload_key(record: Mapping[str, Any]) -> str:
    return json.dumps(
        record,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


@dataclass(frozen=True)
class GroundTruth:
    selected_versions: dict[str, int]
    conflicted_ids: tuple[str, ...]
    quarantined_ids: tuple[str, ...]
    duplicate_count: int
    stale_count: int
    transactions: dict[str, dict[str, str]]
    payouts: dict[str, dict[str, str]]
    missing_movements: tuple[str, ...]
    payout_header_mismatches: tuple[str, ...]

    def financial_document(self) -> dict[str, Any]:
        """Fields that can be compared directly with published rows."""
        return {
            "transactions": self.transactions,
            "payouts": self.payouts,
        }

    def financial_fingerprint(self) -> str:
        encoded = json.dumps(
            self.financial_document(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


def calculate_ground_truth(
    report_specs: Iterable[Any],
    *,
    fee_rules: Mapping[str, tuple[int, int]],
) -> GroundTruth:
    """Resolve versions and calculate balances from raw ReportSpec records.

    `report_specs` may contain the initial exports, followed by any
    delivered fault reports. Each spec needs `report_type` and `records`.
    """
    grouped: dict[
        tuple[str, str],
        list[dict[str, Any]],
    ] = defaultdict(list)

    quarantined: set[str] = set()

    for spec in report_specs:
        report_type = _report_name(spec.report_type)
        id_field = ID_FIELD[report_type]
        money_field = MONEY_FIELD[report_type]

        for source_record in spec.records:
            record = dict(source_record)
            source_id = str(record[id_field])
            identity = f"{report_type}:{source_id}"

            try:
                cents(record[money_field])
            except ValueError:
                # The Step 7 malformed-payment fault targets this field.
                if report_type == "PAYMENTS":
                    quarantined.add(identity)
                    continue
                raise

            grouped[(report_type, source_id)].append(record)

    selected: dict[tuple[str, str], dict[str, Any]] = {}
    selected_versions: dict[str, int] = {}
    conflicted: set[str] = set()
    duplicates = 0
    stale = 0

    for (report_type, source_id), records in grouped.items():
        highest_version = max(int(r["source_version"]) for r in records)
        highest = [
            r for r in records
            if int(r["source_version"]) == highest_version
        ]
        stale += len(records) - len(highest)

        variants = {_payload_key(r): r for r in highest}
        duplicates += len(highest) - len(variants)

        identity = f"{report_type}:{source_id}"
        if len(variants) > 1:
            conflicted.add(identity)
            continue

        selected_record = next(iter(variants.values()))
        selected[(report_type, source_id)] = selected_record
        selected_versions[identity] = highest_version

    def records_of(report_type: str) -> list[dict[str, Any]]:
        return [
            record
            for (kind, _), record in selected.items()
            if kind == report_type
        ]

    orders = records_of("ORDERS")
    payments = records_of("PAYMENTS")
    refunds = records_of("REFUNDS")
    fees = records_of("FEES")
    lines = records_of("SETTLEMENT_LINES")
    payouts = records_of("PAYOUTS")

    captured = {
        p["payment_id"]: p
        for p in payments
        if p["status"] == "CAPTURED"
    }
    successful_refunds = [
        r for r in refunds if r["status"] == "SUCCEEDED"
    ]

    fees_by_payment: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for fee in fees:
        fees_by_payment[fee["payment_id"]].append(fee)

    refunds_by_payment: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for refund in successful_refunds:
        refunds_by_payment[refund["payment_id"]].append(refund)

    transactions: dict[str, dict[str, str]] = {}

    for order in orders:
        order_id = order["order_id"]
        order_payments = [
            p for p in captured.values()
            if p["order_id"] == order_id
        ]

        expected_collection = (
            cents(order["order_total"])
            if order["status"] == "PAID"
            else 0
        )
        captured_total = sum(
            cents(p["amount"]) for p in order_payments
        )
        refund_total = sum(
            cents(r["amount"])
            for p in order_payments
            for r in refunds_by_payment[p["payment_id"]]
        )
        expected_fee_total = sum(
            fee_cents(
                cents(p["amount"]),
                p["payment_method"],
                fee_rules,
            )
            for p in order_payments
        )
        reported_fee_total = sum(
            cents(f["amount"])
            for p in order_payments
            for f in fees_by_payment[p["payment_id"]]
        )

        transactions[order_id] = {
            "expected_collection": euros(expected_collection),
            "captured_total": euros(captured_total),
            "successful_refund_total": euros(refund_total),
            "expected_fee_total": euros(expected_fee_total),
            "reported_fee_total": euros(reported_fee_total),
            "lifetime_net_collection": euros(
                captured_total - refund_total - expected_fee_total
            ),
        }

    # Build expected provider movements independently from the selected
    # captures, the fee policy, and successful refunds.
    expected_by_date: dict[
        str,
        dict[tuple[str, str], int],
    ] = defaultdict(dict)

    for payment in captured.values():
        payment_id = payment["payment_id"]
        payout_date = payment["available_on"]
        amount = cents(payment["amount"])

        expected_by_date[payout_date][
            ("CAPTURE", payment_id)
        ] = amount

        related_fees = fees_by_payment[payment_id]
        fee_id = (
            related_fees[0]["fee_id"]
            if len(related_fees) == 1
            else f"expected-fee:{payment_id}"
        )
        expected_by_date[payout_date][
            ("FEE", fee_id)
        ] = -fee_cents(
            amount,
            payment["payment_method"],
            fee_rules,
        )

    for refund in successful_refunds:
        expected_by_date[refund["available_on"]][
            ("REFUND", refund["refund_id"])
        ] = -cents(refund["amount"])

    lines_by_payout: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for line in lines:
        lines_by_payout[line["payout_id"]].append(line)

    payout_balances: dict[str, dict[str, str]] = {}
    missing_movements: set[str] = set()
    payout_header_mismatches: set[str] = set()

    for payout in payouts:
        payout_id = payout["payout_id"]
        payout_date = payout["payout_date"]
        expected = expected_by_date[payout_date]
        payout_lines = lines_by_payout[payout_id]

        expected_total = sum(expected.values())
        line_total = sum(
            cents(line["signed_amount"]) for line in payout_lines
        )
        reported_total = cents(payout["reported_net_amount"])

        actual_movement_keys = {
            (line["movement_type"], line["movement_id"])
            for line in payout_lines
        }
        for movement_type, movement_id in (
            expected.keys() - actual_movement_keys
        ):
            missing_movements.add(
                f"{payout_id}:{movement_type}:{movement_id}"
            )

        if reported_total != line_total:
            payout_header_mismatches.add(payout_id)

        payout_balances[payout_id] = {
            "expected_payout": euros(expected_total),
            "reported_line_total": euros(line_total),
            "reported_net_amount": euros(reported_total),
        }

    return GroundTruth(
        selected_versions=selected_versions,
        conflicted_ids=tuple(sorted(conflicted)),
        quarantined_ids=tuple(sorted(quarantined)),
        duplicate_count=duplicates,
        stale_count=stale,
        transactions=dict(sorted(transactions.items())),
        payouts=dict(sorted(payout_balances.items())),
        missing_movements=tuple(sorted(missing_movements)),
        payout_header_mismatches=tuple(
            sorted(payout_header_mismatches)
        ),
    )