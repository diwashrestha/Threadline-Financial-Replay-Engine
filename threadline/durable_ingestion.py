"""Verified file ingestion with durable history and entity resolution."""

from __future__ import annotations

import hashlib
import json

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from threadline.canonicalize import RecordEnvelope
from threadline.completeness import REPORT_SOURCES
from threadline.contracts import (
    ContractViolation,
    EntityType,
    ReportType,
    SourceSystem,
    parse_source_record,
    quarantine_from_violation,
)
from threadline.full_rebuild_recovery import lock_financial_state
from threadline.recovery_fingerprint import (
    document_fingerprint,
    json_value,
)


FailureHook = Callable[[str], None]


REPORT_ENTITIES = {
    ReportType.ORDERS: EntityType.ORDER,
    ReportType.PAYMENTS: EntityType.PAYMENT,
    ReportType.REFUNDS: EntityType.REFUND,
    ReportType.FEES: EntityType.FEE,
    ReportType.SETTLEMENT_LINES: EntityType.SETTLEMENT_LINE,
    ReportType.PAYOUTS: EntityType.PAYOUT,
}


class DeliveryIntegrityError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class VerifiedDelivery:
    path: Path
    source_relative_path: str
    source_system: SourceSystem
    report_type: ReportType
    report_date: date
    entity_type: EntityType
    data_bytes: bytes
    manifest_bytes: bytes
    records: tuple[dict, ...]


@dataclass(frozen=True, slots=True)
class IngestionOutcome:
    batch_id: UUID
    request_id: UUID
    reused_existing_batch: bool


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def enum_member(enum_type, value):
    if isinstance(value, enum_type):
        return value

    if isinstance(value, str) and value in enum_type.__members__:
        return enum_type[value]

    return enum_type(value)


def _unique_object(pairs):
    result = {}

    for key, value in pairs:
        if key in result:
            raise DeliveryIntegrityError(
                f"Duplicate JSON key: {key}"
            )
        result[key] = value

    return result


def strict_json(value: bytes):
    def reject_constant(constant):
        raise DeliveryIntegrityError(
            f"Non-finite JSON number: {constant}"
        )

    return json.loads(
        value,
        object_pairs_hook=_unique_object,
        parse_constant=reject_constant,
    )


def verify_delivery(
    source_path: str | Path,
    *,
    inbox_root: str | Path,
) -> VerifiedDelivery | None:
    """Return None for an unfinished delivery; reject invalid evidence."""

    root = Path(inbox_root).resolve()
    path = Path(source_path).resolve()

    if not path.is_relative_to(root) or path == root:
        raise DeliveryIntegrityError("Source path escapes inbox")

    if path.suffix != ".json" or path.name.endswith(".manifest.json"):
        raise DeliveryIntegrityError("Expected a data JSON file")

    manifest_path = path.with_suffix(".manifest.json")

    temporary_paths = (
        Path(f"{path}.part"),
        Path(f"{manifest_path}.part"),
    )

    if any(candidate.exists() for candidate in temporary_paths):
        return None

    if not path.is_file() or not manifest_path.is_file():
        return None

    data_bytes = path.read_bytes()
    manifest_bytes = manifest_path.read_bytes()

    records = strict_json(data_bytes)
    manifest = strict_json(manifest_bytes)

    required = {
        "filename",
        "source_system",
        "report_type",
        "report_date",
        "entity_type",
        "schema_version",
        "row_count",
        "checksum_sha256",
    }

    if not isinstance(manifest, dict) or set(manifest) != required:
        raise DeliveryIntegrityError(
            "Manifest has missing or unexpected fields"
        )

    if manifest["filename"] != path.name:
        raise DeliveryIntegrityError("Manifest filename mismatch")

    if manifest["schema_version"] != "1":
        raise DeliveryIntegrityError("Unsupported schema version")

    if (
        not isinstance(records, list)
        or any(not isinstance(record, dict) for record in records)
    ):
        raise DeliveryIntegrityError(
            "Data must be a JSON array of objects"
        )

    if (
        type(manifest["row_count"]) is not int
        or manifest["row_count"] < 0
        or manifest["row_count"] != len(records)
    ):
        raise DeliveryIntegrityError("Row count mismatch")

    if manifest["checksum_sha256"] != sha256_bytes(data_bytes):
        raise DeliveryIntegrityError("Data checksum mismatch")

    report_type = enum_member(
        ReportType,
        manifest["report_type"],
    )
    entity_type = enum_member(
        EntityType,
        manifest["entity_type"],
    )
    source_system = enum_member(
        SourceSystem,
        manifest["source_system"],
    )

    if entity_type is not REPORT_ENTITIES[report_type]:
        raise DeliveryIntegrityError("Report/entity mismatch")

    if source_system is not REPORT_SOURCES[report_type]:
        raise DeliveryIntegrityError("Report/source mismatch")

    report_date = date.fromisoformat(manifest["report_date"])

    if report_date.isoformat() != manifest["report_date"]:
        raise DeliveryIntegrityError("Non-canonical report date")

    # Recheck after reading. Completed filenames must remain immutable.
    if any(candidate.exists() for candidate in temporary_paths):
        return None

    return VerifiedDelivery(
        path=path,
        source_relative_path=path.relative_to(root).as_posix(),
        source_system=source_system,
        report_type=report_type,
        report_date=report_date,
        entity_type=entity_type,
        data_bytes=data_bytes,
        manifest_bytes=manifest_bytes,
        records=tuple(records),
    )


def _resolve_entity(connection, entity_type: str, source_id: str):
    versions = connection.execute(
        """
        SELECT source_version, payload_hash
        FROM source_record_version
        WHERE entity_type = %s AND source_id = %s
        ORDER BY source_version DESC, payload_hash
        """,
        (entity_type, source_id),
    ).fetchall()

    winning_version = versions[0]["source_version"]

    winning_hashes = {
        row["payload_hash"]
        for row in versions
        if row["source_version"] == winning_version
    }

    conflicted = len(winning_hashes) > 1
    selected_hash = (
        None if conflicted else next(iter(winning_hashes))
    )

    connection.execute(
        """
        INSERT INTO entity_resolution (
            entity_type,
            source_id,
            winning_version,
            resolution_state,
            selected_payload_hash,
            updated_at_utc
        )
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (entity_type, source_id)
        DO UPDATE SET
            winning_version = EXCLUDED.winning_version,
            resolution_state = EXCLUDED.resolution_state,
            selected_payload_hash = EXCLUDED.selected_payload_hash,
            updated_at_utc = EXCLUDED.updated_at_utc
        """,
        (
            entity_type,
            source_id,
            winning_version,
            "CONFLICTED" if conflicted else "ACCEPTED",
            selected_hash,
        ),
    )

    # Reclassify every receipt for this identity. History is never changed.
    connection.execute(
        """
        WITH ranked AS (
            SELECT
                r.receipt_id,
                r.source_version,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        r.source_version,
                        r.payload_hash
                    ORDER BY
                        CASE
                            WHEN r.receipt_id = v.first_seen_receipt_id
                            THEN 0 ELSE 1
                        END,
                        r.received_at_utc,
                        r.receipt_id
                ) AS occurrence
            FROM source_receipt AS r
            JOIN ingestion_batch AS b
                ON b.batch_id = r.batch_id
            JOIN source_record_version AS v
                ON v.record_version_id = r.record_version_id
            WHERE r.entity_type = %s
              AND r.source_id = %s
              AND b.ingestion_status IN ('COMMITTED', 'PROCESSING')
        ),
        classified AS (
            SELECT
                receipt_id,
                CASE
                    WHEN source_version < %s THEN 'STALE'
                    WHEN %s THEN 'CONFLICTED'
                    WHEN occurrence = 1 THEN 'ACCEPTED'
                    ELSE 'DUPLICATE'
                END AS disposition
            FROM ranked
        )
        UPDATE source_receipt AS r
        SET
            disposition = c.disposition,
            reason_code = CASE c.disposition
                WHEN 'STALE' THEN 'LOWER_SOURCE_VERSION'
                WHEN 'CONFLICTED' THEN 'CONFLICTING_SOURCE_VERSION'
                WHEN 'DUPLICATE' THEN 'IDENTICAL_DUPLICATE'
                ELSE NULL
            END
        FROM classified AS c
        WHERE r.receipt_id = c.receipt_id
        """,
        (
            entity_type,
            source_id,
            winning_version,
            conflicted,
        ),
    )


def ingest_source_file(
    source_path: str | Path,
    *,
    database_url: str,
    inbox_root: str | Path,
    as_of_utc: datetime,
    failure_hook: FailureHook | None = None,
) -> IngestionOutcome | None:
    """Ingest a completed delivery; propagate failures to the task caller."""

    if as_of_utc.tzinfo is None or as_of_utc.utcoffset() is None:
        raise ValueError("as_of_utc must be timezone-aware")

    as_of_utc = as_of_utc.astimezone(timezone.utc)
    hook = failure_hook or (lambda point: None)

    ready = verify_delivery(source_path, inbox_root=inbox_root)

    if ready is None:
        return None

    delivery_key = document_fingerprint(
        {
            "source_system": ready.source_system.value,
            "report_type": ready.report_type.value,
            "report_date": ready.report_date.isoformat(),
            "source_relative_path": ready.source_relative_path,
            "file_checksum": sha256_bytes(ready.data_bytes),
            "manifest_checksum": sha256_bytes(ready.manifest_bytes),
        }
    )

    with psycopg.connect(
        database_url,
        autocommit=True,
        row_factory=dict_row,
        connect_timeout=3,
    ) as connection:
        with connection.transaction():
            lock_financial_state(connection)

            existing = connection.execute(
                """
                SELECT batch_id, ingestion_status
                FROM ingestion_batch
                WHERE delivery_key = %s
                """,
                (delivery_key,),
            ).fetchone()

            if existing is not None:
                if existing["ingestion_status"] != "COMMITTED":
                    raise RuntimeError(
                        "Existing delivery is not committed"
                    )

                request = connection.execute(
                    """
                    SELECT request_id
                    FROM recovery_request
                    WHERE trigger_batch_id = %s
                    """,
                    (existing["batch_id"],),
                ).fetchone()

                if request is None:
                    raise RuntimeError(
                        "Committed delivery lacks its recovery request"
                    )

                return IngestionOutcome(
                    batch_id=existing["batch_id"],
                    request_id=request["request_id"],
                    reused_existing_batch=True,
                )

            batch_id = uuid4()

            connection.execute(
                """
                INSERT INTO ingestion_batch (
                    batch_id,
                    delivery_key,
                    source_system,
                    report_type,
                    report_date,
                    original_filename,
                    file_checksum,
                    manifest_checksum,
                    schema_version,
                    declared_row_count,
                    observed_row_count,
                    ingestion_status,
                    archive_status,
                    source_relative_path,
                    received_at_utc
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    '1', %s, %s, 'PROCESSING', 'PENDING', %s, %s
                )
                """,
                (
                    batch_id,
                    delivery_key,
                    ready.source_system.value,
                    ready.report_type.value,
                    ready.report_date,
                    ready.path.name,
                    sha256_bytes(ready.data_bytes),
                    sha256_bytes(ready.manifest_bytes),
                    len(ready.records),
                    len(ready.records),
                    ready.source_relative_path,
                    as_of_utc,
                ),
            )

            connection.execute(
                """
                INSERT INTO verified_delivery_evidence (
                    batch_id, data_bytes, manifest_bytes
                )
                VALUES (%s, %s, %s)
                """,
                (
                    batch_id,
                    ready.data_bytes,
                    ready.manifest_bytes,
                ),
            )

            touched = set()
            receipt_ids = []

            for row_number, payload in enumerate(ready.records, start=1):
                receipt_id = uuid4()
                receipt_ids.append(receipt_id)

                try:
                    record = parse_source_record(
                        ready.entity_type,
                        payload,
                    )
                except ContractViolation as violation:
                    rejected = quarantine_from_violation(
                        ready.entity_type,
                        payload,
                        violation,
                    )

                    connection.execute(
                        """
                        INSERT INTO source_receipt (
                            receipt_id,
                            batch_id,
                            row_number,
                            entity_type,
                            source_id,
                            source_version,
                            payload_hash,
                            raw_payload,
                            disposition,
                            reason_code,
                            received_at_utc
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, NULL, %s,
                            %s, 'QUARANTINED', %s, %s
                        )
                        """,
                        (
                            receipt_id,
                            batch_id,
                            row_number,
                            ready.entity_type.value,
                            rejected.source_id,
                            document_fingerprint(payload),
                            Jsonb(payload),
                            rejected.reason_code,
                            as_of_utc,
                        ),
                    )
                    continue

                envelope = RecordEnvelope(
                    receipt_id=str(receipt_id),
                    record=record,
                )

                connection.execute(
                    """
                    INSERT INTO source_receipt (
                        receipt_id,
                        batch_id,
                        row_number,
                        entity_type,
                        source_id,
                        source_version,
                        payload_hash,
                        raw_payload,
                        disposition,
                        received_at_utc
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, 'PENDING', %s
                    )
                    """,
                    (
                        receipt_id,
                        batch_id,
                        row_number,
                        ready.entity_type.value,
                        record.record_id,
                        record.source_version,
                        envelope.payload_hash,
                        Jsonb(payload),
                        as_of_utc,
                    ),
                )

                connection.execute(
                    """
                    INSERT INTO source_record_version (
                        entity_type,
                        source_id,
                        source_version,
                        payload_hash,
                        canonical_payload,
                        first_seen_batch_id,
                        first_seen_receipt_id,
                        first_seen_at_utc
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (
                        entity_type,
                        source_id,
                        source_version,
                        payload_hash
                    )
                    DO NOTHING
                    """,
                    (
                        ready.entity_type.value,
                        record.record_id,
                        record.source_version,
                        envelope.payload_hash,
                        Jsonb(json_value(record)),
                        batch_id,
                        receipt_id,
                        as_of_utc,
                    ),
                )

                version = connection.execute(
                    """
                    SELECT record_version_id
                    FROM source_record_version
                    WHERE entity_type = %s
                      AND source_id = %s
                      AND source_version = %s
                      AND payload_hash = %s
                    """,
                    (
                        ready.entity_type.value,
                        record.record_id,
                        record.source_version,
                        envelope.payload_hash,
                    ),
                ).fetchone()

                connection.execute(
                    """
                    UPDATE source_receipt
                    SET record_version_id = %s
                    WHERE receipt_id = %s
                    """,
                    (version["record_version_id"], receipt_id),
                )

                touched.add(
                    (ready.entity_type.value, record.record_id)
                )

            for entity_type, source_id in sorted(touched):
                _resolve_entity(connection, entity_type, source_id)

            hook("after_entity_resolution")

            connection.execute(
                """
                INSERT INTO expected_report_day (business_date)
                VALUES (%s)
                ON CONFLICT DO NOTHING
                """,
                (ready.report_date,),
            )

            connection.execute(
                """
                UPDATE ingestion_batch
                SET
                    ingestion_status = 'COMMITTED',
                    committed_at_utc = CURRENT_TIMESTAMP
                WHERE batch_id = %s
                """,
                (batch_id,),
            )

            # Schedule for report-evidence changes too, including empty reports.
            # This avoids incorrectly treating canonical_changed=False as
            # proof that report completeness cannot have changed.
            request = connection.execute(
                """
                INSERT INTO recovery_request (
                    request_key,
                    trigger_receipt_id,
                    trigger_batch_id,
                    reason_code,
                    recovery_scope,
                    as_of_utc
                )
                VALUES (%s, %s, %s, 'SOURCE_REPORT_AVAILABLE', 'FULL', %s)
                RETURNING request_id
                """,
                (
                    document_fingerprint(
                        {"verified_delivery": delivery_key}
                    ),
                    receipt_ids[0] if receipt_ids else None,
                    batch_id,
                    as_of_utc,
                ),
            ).fetchone()

            hook("before_ingestion_commit")

            outcome = IngestionOutcome(
                batch_id=batch_id,
                request_id=request["request_id"],
                reused_existing_batch=False,
            )

        # A lost acknowledgement here must not undo or downgrade the batch.
        hook("after_ingestion_commit")

        return outcome