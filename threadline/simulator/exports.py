"""Export deterministic financial reports for Threadline ingestion."""

from __future__ import annotations

import json
import os

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from threadline.contracts import (
    EntityType,
    ReportType,
    SourceSystem,
    parse_manifest,
    parse_source_record,
)
from threadline.simulator.catalog import cents_to_eur
from threadline.simulator.fees import (
    FeeFact,
    fee_source_record,
)
from threadline.simulator.orders_payments import (
    CheckoutResult,
    captured_payment_source_record,
)
from threadline.simulator.returns_refunds import (
    ReturnRefund,
    refund_source_record,
)
from threadline.simulator.settlement import (
    SettlementBook,
    payout_source_record,
    settlement_line_source_record,
)


SCHEMA_VERSION = 1

REPORTS = (
    ReportType.ORDERS,
    ReportType.PAYMENTS,
    ReportType.REFUNDS,
    ReportType.FEES,
    ReportType.SETTLEMENT_LINES,
    ReportType.PAYOUTS,
)

# Report type -> entity type, identity field, source system.
REPORT_RULES = {
    ReportType.ORDERS: (
        EntityType.ORDER,
        "order_id",
        SourceSystem.THREADLINE_SHOP,
    ),
    ReportType.PAYMENTS: (
        EntityType.PAYMENT,
        "payment_id",
        SourceSystem.MOCKPAY,
    ),
    ReportType.REFUNDS: (
        EntityType.REFUND,
        "refund_id",
        SourceSystem.MOCKPAY,
    ),
    ReportType.FEES: (
        EntityType.FEE,
        "fee_id",
        SourceSystem.MOCKPAY,
    ),
    ReportType.SETTLEMENT_LINES: (
        EntityType.SETTLEMENT_LINE,
        "settlement_line_id",
        SourceSystem.MOCKPAY,
    ),
    ReportType.PAYOUTS: (
        EntityType.PAYOUT,
        "payout_id",
        SourceSystem.MOCKPAY,
    ),
}


@dataclass(frozen=True, slots=True)
class ReportSpec:
    report_type: ReportType
    business_date: date
    records: tuple[dict[str, object], ...]


@dataclass(frozen=True, slots=True)
class ExportedReport:
    data_path: Path
    manifest_path: Path
    row_count: int
    checksum: str


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("Timestamp must be timezone-aware")

    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def build_report_specs(
    *,
    checkouts: Sequence[CheckoutResult],
    fees: Sequence[FeeFact],
    return_refunds: Sequence[ReturnRefund],
    settlement: SettlementBook,
    start_date: date,
    end_date: date,
) -> tuple[ReportSpec, ...]:
    """Group the six source record types by their business dates."""

    if end_date < start_date:
        raise ValueError("end_date precedes start_date")

    days = (
        start_date + timedelta(days=offset)
        for offset in range((end_date - start_date).days + 1)
    )
    groups = {
        (report_type, day): []
        for day in days
        for report_type in REPORTS
    }

    def add(
        report_type: ReportType,
        business_date: date,
        record: dict[str, object],
    ) -> None:
        key = (report_type, business_date)

        if key not in groups:
            raise ValueError(
                f"{report_type.value} record on {business_date} "
                "is outside the export date range"
            )

        groups[key].append(record)

    for checkout in checkouts:
        placed = checkout.placed_order

        if placed is None:
            # Abandoned checkout attempts remain internal evidence.
            continue

        add(
            ReportType.ORDERS,
            placed.placed_at_utc.date(),
            {
                "order_id": placed.order_id,
                "created_at_utc": _utc_text(
                    placed.placed_at_utc
                ),
                "status": "PAID",
                "currency": "EUR",
                "order_total": cents_to_eur(
                    placed.amount_gross_cents
                ),
                "source_version": 1,
            },
        )

        payment_record = captured_payment_source_record(
            checkout
        )
        if payment_record is None:
            raise ValueError(
                "Placed order has no captured PAYMENT record"
            )

        capture = next(
            attempt
            for attempt in checkout.attempts
            if attempt.status == "CAPTURED"
        )
        add(
            ReportType.PAYMENTS,
            capture.effective_at_utc.date(),
            payment_record,
        )

    for fee in fees:
        add(
            ReportType.FEES,
            fee.effective_at_utc.date(),
            fee_source_record(fee),
        )

    for pair in return_refunds:
        add(
            ReportType.REFUNDS,
            pair.refund.effective_at_utc.date(),
            refund_source_record(pair),
        )

    for line in settlement.lines:
        add(
            ReportType.SETTLEMENT_LINES,
            line.payout_date,
            settlement_line_source_record(line),
        )

    for payout in settlement.payouts:
        add(
            ReportType.PAYOUTS,
            payout.payout_date,
            payout_source_record(payout),
        )

    specs = []

    for day in (
        start_date + timedelta(days=offset)
        for offset in range((end_date - start_date).days + 1)
    ):
        for report_type in REPORTS:
            _, id_field, _ = REPORT_RULES[report_type]
            records = sorted(
                groups[(report_type, day)],
                key=lambda record: (
                    str(record[id_field]),
                    int(record["source_version"]),
                ),
            )
            specs.append(
                ReportSpec(
                    report_type=report_type,
                    business_date=day,
                    records=tuple(records),
                )
            )

    return tuple(specs)


def _fsync_directory(directory: Path) -> None:
    # The project runs in WSL2/Linux. Persist directory renames too.
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_from_part(path: Path, contents: bytes) -> None:
    part_path = path.with_name(path.name + ".part")

    # A leftover .part is evidence of an interrupted producer.
    # Do not silently overwrite it.
    if part_path.exists():
        raise FileExistsError(
            f"Inspect interrupted write: {part_path}"
        )

    with part_path.open("xb") as handle:
        handle.write(contents)
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(part_path, path)
    _fsync_directory(path.parent)


def write_report(
    spec: ReportSpec,
    *,
    output_directory: Path,
    dataset_id: str,
    validate_records: bool = True,
) -> ExportedReport:
    """Publish data first, then publish its manifest last."""

    if not dataset_id:
        raise ValueError("dataset_id is required")

    entity_type, id_field, source_system = REPORT_RULES[
        spec.report_type
    ]

    identities = []

    for record in spec.records:
        if validate_records:
            parse_source_record(entity_type, record)
        identities.append(
            (
                str(record[id_field]),
                int(record["source_version"]),
            )
        )

    if len(identities) != len(set(identities)):
        raise ValueError(
            "Clean report contains duplicate identity/version"
        )

    data_bytes = (
        json.dumps(
            spec.records,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        + "\n"
    ).encode("utf-8")
    checksum = sha256(data_bytes).hexdigest()

    generated_at = datetime.combine(
        spec.business_date + timedelta(days=1),
        time(2, 0),
        tzinfo=timezone.utc,
    )

    batch_id = str(
        uuid5(
            NAMESPACE_URL,
            (
                f"threadline-sim-v1|{dataset_id}|"
                f"{spec.report_type.value}|"
                f"{spec.business_date.isoformat()}|{checksum}"
            ),
        )
    )

    manifest = {
        "batch_id": batch_id,
        "source_system": source_system.value,
        "report_type": spec.report_type.value,
        "business_date": spec.business_date.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": _utc_text(generated_at),
        "row_count": len(spec.records),
        "sha256": checksum,
    }
    parse_manifest(manifest)

    manifest_bytes = (
        json.dumps(
            manifest,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")

    output_directory.mkdir(parents=True, exist_ok=True)
    data_path = output_directory / (
        f"{spec.report_type.value}_"
        f"{spec.business_date.isoformat()}.json"
    )
    manifest_path = data_path.with_suffix(
        ".manifest.json"
    )

    if (
        data_path.with_name(data_path.name + ".part").exists()
        or manifest_path.with_name(
            manifest_path.name + ".part"
        ).exists()
    ):
        raise FileExistsError(
            "An interrupted .part file exists"
        )

    if manifest_path.exists():
        if (
            not data_path.exists()
            or data_path.read_bytes() != data_bytes
            or manifest_path.read_bytes() != manifest_bytes
        ):
            raise ValueError(
                "Existing report differs from this dataset"
            )
    else:
        # A previous attempt may have committed the data rename
        # but stopped before publishing its manifest.
        if data_path.exists():
            if data_path.read_bytes() != data_bytes:
                raise ValueError(
                    "Existing data file has another checksum"
                )
        else:
            _publish_from_part(data_path, data_bytes)

        _publish_from_part(
            manifest_path,
            manifest_bytes,
        )

    if sha256(data_path.read_bytes()).hexdigest() != checksum:
        raise ValueError("Final data checksum changed")

    return ExportedReport(
        data_path=data_path,
        manifest_path=manifest_path,
        row_count=len(spec.records),
        checksum=checksum,
    )


def export_history(
    specs: Sequence[ReportSpec],
    *,
    output_directory: Path,
    dataset_id: str,
) -> tuple[ExportedReport, ...]:
    return tuple(
        write_report(
            spec,
            output_directory=output_directory,
            dataset_id=dataset_id,
        )
        for spec in specs
    )