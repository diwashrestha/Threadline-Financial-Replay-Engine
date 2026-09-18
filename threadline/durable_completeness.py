"""Evaluate the file protocol using retained PostgreSQL evidence."""

from __future__ import annotations

from datetime import datetime

from psycopg.rows import dict_row

from threadline.completeness import (
    CompletenessIssue,
    CompletenessIssueCode,
    CompletenessResult,
    REPORT_SOURCES,
    evaluate_report,
)
from threadline.contracts import (
    ReportType,
    SourceCompleteness,
    SourceReportStatus,
)
from threadline.durable_ingestion import (
    enum_member,
    sha256_bytes,
    strict_json,
)


def _verify_stored_report(batch) -> str | None:
    if batch["data_bytes"] is None or batch["manifest_bytes"] is None:
        return "VERIFIED_EVIDENCE_MISSING"

    data_bytes = bytes(batch["data_bytes"])
    manifest_bytes = bytes(batch["manifest_bytes"])

    if sha256_bytes(data_bytes) != batch["file_checksum"]:
        return "DATA_CHECKSUM_MISMATCH"

    if sha256_bytes(manifest_bytes) != batch["manifest_checksum"]:
        return "MANIFEST_CHECKSUM_MISMATCH"

    try:
        records = strict_json(data_bytes)
        manifest = strict_json(manifest_bytes)

        if (
            not isinstance(records, list)
            or any(not isinstance(item, dict) for item in records)
        ):
            return "INVALID_DATA_STRUCTURE"

        report_type = enum_member(
            ReportType,
            batch["report_type"],
        )

        if (
            manifest["filename"] != batch["original_filename"]
            or enum_member(ReportType, manifest["report_type"])
            is not report_type
            or manifest["source_system"]
            not in {
                REPORT_SOURCES[report_type].name,
                REPORT_SOURCES[report_type].value,
            }
            or manifest["entity_type"]
            not in {
                {
                    ReportType.ORDERS: "ORDER",
                    ReportType.PAYMENTS: "PAYMENT",
                    ReportType.REFUNDS: "REFUND",
                    ReportType.FEES: "FEE",
                    ReportType.SETTLEMENT_LINES: "SETTLEMENT_LINE",
                    ReportType.PAYOUTS: "PAYOUT",
                }[report_type]
            }
            or manifest["report_date"] != batch["report_date"].isoformat()
            or manifest["schema_version"] != "1"
            or manifest["checksum_sha256"] != batch["file_checksum"]
            or type(manifest["row_count"]) is not int
            or manifest["row_count"] != len(records)
            or len(records) != batch["declared_row_count"]
            or len(records) != batch["observed_row_count"]
            or len(records) != batch["receipt_count"]
        ):
            return "STORED_REPORT_EVIDENCE_MISMATCH"

    except (KeyError, TypeError, ValueError):
        return "INVALID_STORED_MANIFEST"

    if batch["quarantine_count"]:
        return "QUARANTINED_ROWS"

    return None


def load_completeness(connection, as_of_utc: datetime):
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT business_date
            FROM expected_report_day
            UNION
            SELECT report_date
            FROM ingestion_batch
            WHERE ingestion_status IN ('COMMITTED', 'FAILED')
            ORDER BY 1
            """
        )
        days = [
            row["business_date"]
            for row in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT
                b.*,
                e.data_bytes,
                e.manifest_bytes,
                (
                    SELECT COUNT(*)
                    FROM source_receipt r
                    WHERE r.batch_id = b.batch_id
                ) AS receipt_count,
                (
                    SELECT COUNT(*)
                    FROM source_receipt r
                    WHERE r.batch_id = b.batch_id
                      AND r.disposition = 'QUARANTINED'
                ) AS quarantine_count
            FROM ingestion_batch b
            LEFT JOIN verified_delivery_evidence e
                ON e.batch_id = b.batch_id
            WHERE b.ingestion_status = 'COMMITTED'
              AND b.received_at_utc <= %s
            ORDER BY b.received_at_utc, b.batch_id
            """,
            (as_of_utc,),
        )
        batches = cursor.fetchall()

    if not days:
        raise ValueError("No expected report days have been declared")

    latest = {}

    for batch in batches:
        key = (
            batch["report_date"],
            enum_member(ReportType, batch["report_type"]),
        )
        latest[key] = batch

    results = []

    for business_date in days:
        for report_type in ReportType:
            missing = evaluate_report(
                report_type=report_type,
                business_date=business_date,
                as_of=as_of_utc,
            )

            batch = latest.get((business_date, report_type))

            if batch is None:
                results.append(missing)
                continue

            problem = _verify_stored_report(batch)

            if problem is None:
                state = SourceCompleteness.COMPLETE
                issues = ()
            else:
                state = (
                    SourceCompleteness.PENDING
                    if as_of_utc < missing.deadline_at_utc
                    else SourceCompleteness.INCOMPLETE
                )
                issues = (
                    CompletenessIssue(
                        code=CompletenessIssueCode.MANIFEST_INVALID,
                        message=(
                            "Cannot certify retained file-protocol evidence: "
                            f"{problem}"
                        ),
                    ),
                )

            results.append(
                CompletenessResult(
                    status=SourceReportStatus(
                        source_system=REPORT_SOURCES[report_type],
                        report_type=report_type,
                        business_date=business_date,
                        state=state,
                        batch_id=str(batch["batch_id"]),
                        reason=problem,
                    ),
                    deadline_at_utc=missing.deadline_at_utc,
                    issues=issues,
                )
            )

    severity = {
        SourceCompleteness.COMPLETE: 0,
        SourceCompleteness.PENDING: 1,
        SourceCompleteness.INCOMPLETE: 2,
    }

    return tuple(
        sorted(
            (
                max(
                    (
                        result
                        for result in results
                        if result.status.report_type is report_type
                    ),
                    key=lambda result: (
                        severity[result.status.state],
                        -result.status.business_date.toordinal(),
                    ),
                )
                for report_type in ReportType
            ),
            key=lambda result: result.sort_key,
        )
    )