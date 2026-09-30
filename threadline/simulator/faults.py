"""Deterministic, isolated mutations of clean simulator reports."""

from __future__ import annotations

import json

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Literal

from threadline.contracts import ReportType
from threadline.simulator.exports import (
    REPORT_RULES,
    ExportedReport,
    ReportSpec,
    write_report,
)
from threadline.simulator.returns_refunds import (
    ReturnRefund,
    refund_source_record,
)
from threadline.simulator.settlement import (
    SettlementLineFact,
    settlement_line_source_record,
)


FaultKind = Literal[
    "DUPLICATE_DELIVERY",
    "LATE_REFUND",
    "HIGHER_VERSION_CORRECTION",
    "SAME_VERSION_CONFLICT",
    "MISSING_LINE",
    "MALFORMED_AMOUNT",
    "PAYOUT_MISMATCH",
]


@dataclass(frozen=True, slots=True)
class FaultPlan:
    fault_id: str
    kind: FaultKind
    target: str
    expected_effects: tuple[str, ...]
    initial_specs: tuple[ReportSpec, ...]
    followup_specs: tuple[ReportSpec, ...] = ()
    followup_arrives_at_utc: datetime | None = None
    before_hash: str | None = None
    after_hash: str | None = None
    # Only this report may bypass source-record validation.
    invalid_initial_report: tuple[ReportType, object] | None = None


def _copy_specs(
    specs: tuple[ReportSpec, ...],
) -> tuple[ReportSpec, ...]:
    return tuple(
        ReportSpec(
            report_type=spec.report_type,
            business_date=spec.business_date,
            records=tuple(
                dict(record)
                for record in spec.records
            ),
        )
        for spec in specs
    )


def _hash_record(record: dict[str, object]) -> str:
    encoded = json.dumps(
        record,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _find_record(
    specs: tuple[ReportSpec, ...],
    report_type: ReportType,
    source_id: str,
) -> tuple[ReportSpec, dict[str, object]]:
    _, id_field, _ = REPORT_RULES[report_type]

    matches = [
        (spec, record)
        for spec in specs
        if spec.report_type == report_type
        for record in spec.records
        if record[id_field] == source_id
    ]

    if len(matches) != 1:
        raise ValueError(
            f"Expected one {report_type.value} record "
            f"for {source_id}; found {len(matches)}"
        )

    spec, record = matches[0]
    return spec, dict(record)


def _replace_initial_record(
    specs: tuple[ReportSpec, ...],
    report_type: ReportType,
    source_id: str,
    replacement: dict[str, object] | None,
) -> tuple[ReportSpec, ...]:
    """Replace or remove exactly one record in copied reports."""

    _, id_field, _ = REPORT_RULES[report_type]
    changed = 0
    result = []

    for spec in specs:
        rows = []

        for record in spec.records:
            if (
                spec.report_type == report_type
                and record[id_field] == source_id
            ):
                changed += 1
                if replacement is not None:
                    rows.append(dict(replacement))
            else:
                rows.append(dict(record))

        result.append(
            ReportSpec(
                report_type=spec.report_type,
                business_date=spec.business_date,
                records=tuple(rows),
            )
        )

    if changed != 1:
        raise ValueError(
            f"Expected to change one record; changed {changed}"
        )

    return tuple(result)


def duplicate_delivery(
    clean: tuple[ReportSpec, ...],
    *,
    report_type: ReportType,
    business_date,
    arrives_at_utc: datetime,
) -> FaultPlan:
    matches = [
        spec
        for spec in clean
        if spec.report_type == report_type
        and spec.business_date == business_date
    ]
    if len(matches) != 1:
        raise ValueError("Duplicate target report is ambiguous")

    return FaultPlan(
        fault_id=(
            f"duplicate-{report_type.value}-"
            f"{business_date.isoformat()}"
        ),
        kind="DUPLICATE_DELIVERY",
        target=(
            f"{report_type.value}:{business_date.isoformat()}"
        ),
        expected_effects=(
            "new delivery evidence",
            "duplicate record dispositions",
            "unchanged logical fingerprint",
        ),
        initial_specs=_copy_specs(clean),
        followup_specs=_copy_specs((matches[0],)),
        followup_arrives_at_utc=arrives_at_utc,
    )


def higher_version_correction(
    clean: tuple[ReportSpec, ...],
    *,
    payment_id: str,
    arrives_at_utc: datetime,
) -> FaultPlan:
    """Correct a capture timestamp by one second.

    The amount and settlement date stay the same. This isolates
    source-version precedence from fee and payout arithmetic.
    """

    spec, original = _find_record(
        clean,
        ReportType.PAYMENTS,
        payment_id,
    )
    corrected = dict(original)

    old_time = datetime.fromisoformat(
        str(original["effective_at_utc"]).replace(
            "Z", "+00:00"
        )
    )
    corrected["effective_at_utc"] = (
        (old_time + timedelta(seconds=1))
        .astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    corrected["source_version"] = (
        int(original["source_version"]) + 1
    )

    return FaultPlan(
        fault_id=f"correction-{payment_id}",
        kind="HIGHER_VERSION_CORRECTION",
        target=f"PAYMENT:{payment_id}",
        expected_effects=(
            "version 2 accepted",
            "version 1 stale",
            "no financial exception",
        ),
        initial_specs=_copy_specs(clean),
        followup_specs=(
            ReportSpec(
                ReportType.PAYMENTS,
                spec.business_date,
                (corrected,),
            ),
        ),
        followup_arrives_at_utc=arrives_at_utc,
        before_hash=_hash_record(original),
        after_hash=_hash_record(corrected),
    )


def same_version_conflict(
    clean: tuple[ReportSpec, ...],
    *,
    payment_id: str,
    arrives_at_utc: datetime,
) -> FaultPlan:
    spec, original = _find_record(
        clean,
        ReportType.PAYMENTS,
        payment_id,
    )
    conflicting = dict(original)
    conflicting["amount"] = (
        f'{Decimal(str(original["amount"])) + Decimal("0.01"):.2f}'
    )
    # Deliberately keep the same source_version.

    return FaultPlan(
        fault_id=f"conflict-{payment_id}",
        kind="SAME_VERSION_CONFLICT",
        target=f"PAYMENT:{payment_id}",
        expected_effects=(
            "both payloads retained",
            "entity resolution CONFLICTED",
            "no selected canonical payment",
            "affected transaction not RECONCILED",
        ),
        initial_specs=_copy_specs(clean),
        followup_specs=(
            ReportSpec(
                ReportType.PAYMENTS,
                spec.business_date,
                (conflicting,),
            ),
        ),
        followup_arrives_at_utc=arrives_at_utc,
        before_hash=_hash_record(original),
        after_hash=_hash_record(conflicting),
    )


def missing_settlement_line(
    clean: tuple[ReportSpec, ...],
    *,
    settlement_line_id: str,
) -> FaultPlan:
    _, original = _find_record(
        clean,
        ReportType.SETTLEMENT_LINES,
        settlement_line_id,
    )

    return FaultPlan(
        fault_id=f"missing-line-{settlement_line_id}",
        kind="MISSING_LINE",
        target=(
            f"SETTLEMENT_LINE:{settlement_line_id}"
        ),
        expected_effects=(
            "MISSING_SETTLEMENT_LINE",
            "PAYOUT_TOTAL_MISMATCH",
        ),
        initial_specs=_replace_initial_record(
            clean,
            ReportType.SETTLEMENT_LINES,
            settlement_line_id,
            None,
        ),
        before_hash=_hash_record(original),
    )


def malformed_payment_amount(
    clean: tuple[ReportSpec, ...],
    *,
    payment_id: str,
) -> FaultPlan:
    spec, original = _find_record(
        clean,
        ReportType.PAYMENTS,
        payment_id,
    )
    malformed = dict(original)
    malformed["amount"] = "one hundred"

    return FaultPlan(
        fault_id=f"malformed-{payment_id}",
        kind="MALFORMED_AMOUNT",
        target=f"PAYMENT:{payment_id}",
        expected_effects=(
            "QUARANTINED",
            "INVALID_AMOUNT",
            "affected transaction not RECONCILED",
        ),
        initial_specs=_replace_initial_record(
            clean,
            ReportType.PAYMENTS,
            payment_id,
            malformed,
        ),
        before_hash=_hash_record(original),
        after_hash=_hash_record(malformed),
        invalid_initial_report=(
            ReportType.PAYMENTS,
            spec.business_date,
        ),
    )


def payout_total_mismatch(
    clean: tuple[ReportSpec, ...],
    *,
    payout_id: str,
) -> FaultPlan:
    _, original = _find_record(
        clean,
        ReportType.PAYOUTS,
        payout_id,
    )
    changed = dict(original)
    changed["reported_net_amount"] = (
        f'{Decimal(str(original["reported_net_amount"])) + Decimal("0.01"):.2f}'
    )

    return FaultPlan(
        fault_id=f"payout-mismatch-{payout_id}",
        kind="PAYOUT_MISMATCH",
        target=f"PAYOUT:{payout_id}",
        expected_effects=("PAYOUT_TOTAL_MISMATCH",),
        initial_specs=_replace_initial_record(
            clean,
            ReportType.PAYOUTS,
            payout_id,
            changed,
        ),
        before_hash=_hash_record(original),
        after_hash=_hash_record(changed),
    )


def late_refund(
    clean: tuple[ReportSpec, ...],
    *,
    return_refund: ReturnRefund,
    arrives_at_utc: datetime,
) -> FaultPlan:
    """Deliver a coherent late refund, settlement line, and payout v2.

    The clean baseline must already have a positive payout on the
    refund's available_on date.
    """

    refund = return_refund.refund
    payout_date = refund.available_on

    payout_specs = [
        spec
        for spec in clean
        if spec.report_type == ReportType.PAYOUTS
        and spec.business_date == payout_date
    ]
    if len(payout_specs) != 1:
        raise ValueError(
            "Late refund needs one existing payout date"
        )
    if len(payout_specs[0].records) != 1:
        raise ValueError(
            "Expected one payout record for that date"
        )

    original_payout = dict(
        payout_specs[0].records[0]
    )
    old_net = Decimal(
        str(original_payout["reported_net_amount"])
    )
    refund_amount = Decimal(refund.amount_cents) / 100
    new_net = old_net - refund_amount

    if new_net < 0:
        raise ValueError(
            "Late refund would need negative-balance "
            "carryforward, which the current contract lacks"
        )

    corrected_payout = dict(original_payout)
    corrected_payout["reported_net_amount"] = (
        f"{new_net:.2f}"
    )
    corrected_payout["source_version"] = (
        int(original_payout["source_version"]) + 1
    )

    refund_line = SettlementLineFact(
        settlement_line_id=(
            f"SL-REFUND-{refund.refund_id}"
        ),
        payout_id=str(original_payout["payout_id"]),
        payout_date=payout_date,
        movement_type="REFUND",
        movement_id=refund.refund_id,
        signed_amount_cents=-refund.amount_cents,
    )

    return FaultPlan(
        fault_id=f"late-refund-{refund.refund_id}",
        kind="LATE_REFUND",
        target=f"REFUND:{refund.refund_id}",
        expected_effects=(
            "refund accepted after initial publication",
            "order lifetime refund total increases",
            "payout version 2 selected",
            "final payout balances without exceptions",
        ),
        initial_specs=_copy_specs(clean),
        followup_specs=(
            ReportSpec(
                ReportType.REFUNDS,
                refund.effective_at_utc.date(),
                (refund_source_record(return_refund),),
            ),
            ReportSpec(
                ReportType.SETTLEMENT_LINES,
                payout_date,
                (settlement_line_source_record(refund_line),),
            ),
            ReportSpec(
                ReportType.PAYOUTS,
                payout_date,
                (corrected_payout,),
            ),
        ),
        followup_arrives_at_utc=arrives_at_utc,
        before_hash=_hash_record(original_payout),
        after_hash=_hash_record(corrected_payout),
    )


def export_fault_plan(
    plan: FaultPlan,
    root: Path,
) -> tuple[
    tuple[ExportedReport, ...],
    tuple[ExportedReport, ...],
]:
    """Write an isolated scenario and its audit log."""

    initial_dir = root / "initial"
    followup_dir = root / "followup"

    initial = tuple(
        write_report(
            spec,
            output_directory=initial_dir,
            dataset_id=f"{plan.fault_id}-initial",
            validate_records=(
                (spec.report_type, spec.business_date)
                != plan.invalid_initial_report
            ),
        )
        for spec in plan.initial_specs
    )

    followup = tuple(
        write_report(
            spec,
            output_directory=followup_dir,
            dataset_id=f"{plan.fault_id}-followup",
        )
        for spec in plan.followup_specs
    )

    log = {
        "fault_id": plan.fault_id,
        "kind": plan.kind,
        "target": plan.target,
        "expected_effects": list(plan.expected_effects),
        "before_hash": plan.before_hash,
        "after_hash": plan.after_hash,
        "followup_arrives_at_utc": (
            plan.followup_arrives_at_utc
            .astimezone(timezone.utc)
            .isoformat()
            if plan.followup_arrives_at_utc
            else None
        ),
    }

    root.mkdir(parents=True, exist_ok=True)
    (root / "fault_log.json").write_text(
        json.dumps(log, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return initial, followup