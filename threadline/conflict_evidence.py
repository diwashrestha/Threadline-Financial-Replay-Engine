"""Stable source-variant references for financial conflict exceptions."""

from __future__ import annotations

from dataclasses import replace
from uuid import UUID

from psycopg.rows import dict_row

from threadline.contracts import (
    ExceptionCode,
    SourceSystem,
)
from threadline.recovery_fingerprint import document_fingerprint


def variant_reference(
    *,
    source_system: str,
    entity_type: str,
    source_id: str,
    source_version: int,
    payload_hash: str,
) -> str:
    """Identify a source variant independently of its delivery receipt."""

    fingerprint = document_fingerprint(
        {
            "source_system": source_system,
            "entity_type": entity_type,
            "source_id": source_id,
            "source_version": source_version,
            "payload_hash": payload_hash,
        }
    )

    return f"source-variant:{fingerprint}"


def is_conflict_exception(exception) -> bool:
    return (
        str(exception.exception_type)
        == ExceptionCode.CONFLICTING_SOURCE_VERSION.value
    )


def replace_conflict_evidence(
    candidate,
    evidence_by_receipt: dict[str, str],
):
    exceptions = []

    for exception in candidate.exceptions:
        if not is_conflict_exception(exception):
            exceptions.append(exception)
            continue

        references = set()

        for receipt_id in exception.supporting_source_record_ids:
            key = str(receipt_id)

            if key not in evidence_by_receipt:
                raise ValueError(
                    f"Conflict receipt evidence is missing: {key}"
                )

            references.add(evidence_by_receipt[key])

        exceptions.append(
            replace(
                exception,
                supporting_source_record_ids=tuple(
                    sorted(references)
                ),
            )
        )

    return replace(
        candidate,
        exceptions=tuple(exceptions),
    )


def stabilize_conflict_evidence(connection, candidate):
    receipt_ids = {
        str(receipt_id)
        for exception in candidate.exceptions
        if is_conflict_exception(exception)
        for receipt_id in exception.supporting_source_record_ids
    }

    if not receipt_ids:
        return candidate

    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT
                r.receipt_id,
                r.entity_type,
                r.source_id,
                r.source_version,
                r.payload_hash,
                b.source_system
            FROM source_receipt r
            JOIN ingestion_batch b
                ON b.batch_id = r.batch_id
            WHERE r.receipt_id = ANY(%s::uuid[])
              AND b.ingestion_status = 'COMMITTED'
            """,
            (
                [
                    UUID(receipt_id)
                    for receipt_id in sorted(receipt_ids)
                ],
            ),
        )
        rows = cursor.fetchall()

    evidence_by_receipt = {}

    for row in rows:
        label = row["source_system"]

        source_system = (
            SourceSystem[label]
            if label in SourceSystem.__members__
            else SourceSystem(label)
        )

        evidence_by_receipt[str(row["receipt_id"])] = variant_reference(
            source_system=source_system.value,
            entity_type=row["entity_type"],
            source_id=row["source_id"],
            source_version=row["source_version"],
            payload_hash=row["payload_hash"],
        )

    return replace_conflict_evidence(
        candidate,
        evidence_by_receipt,
    )