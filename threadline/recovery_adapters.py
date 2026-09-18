"""Reconstruct Stage 2 inputs from durable PostgreSQL evidence.

Manifest bodies are not available in the current ledger. Therefore,
report completeness remains PENDING or INCOMPLETE.

A committed batch is not treated as proof of a complete source report.
"""

from __future__ import annotations

import json

from collections.abc import Mapping
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, TypeVar

import psycopg

from psycopg.rows import dict_row

from threadline.canonicalize import RecordEnvelope
from threadline.completeness import (
    BERLIN,
    EXPECTED_REPORTS,
    CompletenessResult,
    DataFileEvidence,
    ReportEvidence,
    evaluate_report,
)
from threadline.contracts import (
    EntityType,
    QuarantinedRecord,
    ReportType,
    SourceCompleteness,
    SourceSystem,
)
from threadline.recovery_rebuild import make_full_rebuild_builder


EnumT = TypeVar("EnumT", bound=Enum)


def enum_member(
    enum_type: type[EnumT],
    value: Any,
) -> EnumT:
    """Accept an enum instance, member name, or member value."""
    if isinstance(value, enum_type):
        return value

    if not isinstance(value, str):
        raise TypeError(
            f"{enum_type.__name__} must be represented by a string"
        )

    member = enum_type.__members__.get(value)

    if member is not None:
        return member

    return enum_type(value)


def make_envelope(
    receipt: Mapping[str, Any],
    record: Any,
) -> RecordEnvelope:
    """Preserve physical receipt identity during logical replay."""
    if receipt["source_id"] != record.record_id:
        raise ValueError(
            "Stored receipt source_id does not match its parsed record"
        )

    if receipt["source_version"] != record.source_version:
        raise ValueError(
            "Stored receipt source_version does not match its parsed record"
        )

    # payload_hash is calculated by RecordEnvelope itself.
    return RecordEnvelope(
        receipt_id=str(receipt["receipt_id"]),
        record=record,
    )


def make_quarantine(
    receipt: Mapping[str, Any],
) -> QuarantinedRecord:
    """Reconstruct rejection evidence without reparsing the rejected row."""
    reason_code = receipt["reason_code"]

    if not isinstance(reason_code, str) or not reason_code.strip():
        raise ValueError(
            "A quarantined receipt must have a stored reason_code"
        )

    entity_type = enum_member(
        EntityType,
        receipt["entity_type"],
    )

    raw_payload_json = json.dumps(
        receipt["raw_payload"],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )

    return QuarantinedRecord(
        entity_type=entity_type.value,
        source_id=receipt["source_id"],
        reason_code=reason_code,
        # The current receipt query does not contain the original detail.
        reason_detail=f"Stored rejection code: {reason_code}",
        raw_payload_json=raw_payload_json,
    )


def load_completeness(
    connection: psycopg.Connection,
    as_of_utc: datetime,
) -> tuple[CompletenessResult, ...]:
    """Evaluate historical coverage without manufacturing manifest evidence.

    Coverage begins at the earliest finalized report date and extends
    through the later of:
    - the latest finalized report date;
    - yesterday in Europe/Berlin at the request's frozen evaluation time.

    Missing calendar days within that span are evaluated too.

    The existing Stage 2 engine accepts one result per report type.
    Consequently, each report type receives its worst daily result.
    """
    if (
        as_of_utc.tzinfo is None
        or as_of_utc.utcoffset() is None
    ):
        raise ValueError("as_of_utc must be timezone-aware")

    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT
                batch_id,
                source_system,
                report_type,
                report_date,
                observed_row_count,
                file_checksum,
                received_at_utc
            FROM ingestion_batch
            WHERE ingestion_status IN ('COMMITTED', 'FAILED')
            ORDER BY received_at_utc, batch_id
            """
        )
        batches = cursor.fetchall()

    normalized_batches = []

    for batch in batches:
        normalized_batches.append(
            (
                batch,
                enum_member(ReportType, batch["report_type"]),
                enum_member(SourceSystem, batch["source_system"]),
            )
        )

    yesterday = (
        as_of_utc.astimezone(BERLIN).date()
        - timedelta(days=1)
    )

    if batches:
        first_date = min(batch["report_date"] for batch in batches)
        last_date = max(
            yesterday,
            max(batch["report_date"] for batch in batches),
        )
    else:
        first_date = yesterday
        last_date = yesterday

    daily_results = {
        report_type: []
        for report_type in EXPECTED_REPORTS
    }

    business_date = first_date

    while business_date <= last_date:
        for report_type in EXPECTED_REPORTS:
            matching = [
                (batch, source_system)
                for batch, stored_report_type, source_system
                in normalized_batches
                if (
                    stored_report_type is report_type
                    and batch["report_date"] == business_date
                    and batch["received_at_utc"] <= as_of_utc
                )
            ]

            evidence = ReportEvidence()

            if matching:
                # Deterministic selection of the earliest visible observation.
                batch, source_system = min(
                    matching,
                    key=lambda item: (
                        item[0]["received_at_utc"],
                        str(item[0]["batch_id"]),
                    ),
                )

                data_file = DataFileEvidence(
                    batch_id=str(batch["batch_id"]),
                    source_system=source_system,
                    report_type=report_type,
                    business_date=business_date,
                    received_at_utc=batch["received_at_utc"],
                    parsed_row_count=batch["observed_row_count"],
                    sha256=batch["file_checksum"],
                )

                evidence = ReportEvidence(
                    data_file=data_file,

                    # A manifest checksum cannot reconstruct its contents.
                    # Keep the manifest missing until durable bodies exist.
                    manifest=None,
                )

            result = evaluate_report(
                report_type=report_type,
                business_date=business_date,
                as_of=as_of_utc,
                evidence=evidence,
            )

            daily_results[report_type].append(result)

        business_date += timedelta(days=1)

    severity = {
        SourceCompleteness.COMPLETE: 0,
        SourceCompleteness.PENDING: 1,
        SourceCompleteness.INCOMPLETE: 2,
    }

    combined = []

    for report_type in EXPECTED_REPORTS:
        # Highest severity wins. Earliest date breaks ties deterministically.
        worst = max(
            daily_results[report_type],
            key=lambda result: (
                severity[result.status.state],
                -result.status.business_date.toordinal(),
            ),
        )

        combined.append(worst)

    return tuple(
        sorted(
            combined,
            key=lambda result: result.sort_key,
        )
    )


# This module-level binding is importable by recovery_runtime.py.
build_candidate = make_full_rebuild_builder(
    make_envelope=make_envelope,
    make_quarantine=make_quarantine,
    load_completeness=load_completeness,
)