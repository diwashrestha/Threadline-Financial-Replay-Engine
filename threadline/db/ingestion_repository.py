"""PostgreSQL repository for the ingestion ledger."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any
from uuid import UUID

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from threadline.ingestion.models import PreparedReceipt


@dataclass(frozen=True, slots=True)
class StoredBatch:
    batch_id: UUID
    delivery_key: str
    ingestion_status: str
    observed_row_count: int
    file_checksum: str
    manifest_checksum: str


@dataclass(frozen=True, slots=True)
class ReceiptCounts:
    total: int
    processable: int
    quarantined: int


class IngestionRepository:
    """SQL operations that participate in a caller-owned transaction."""

    def __init__(self, connection: Connection):
        self._connection = connection

    def create_batch(
        self,
        *,
        batch_id: UUID,
        delivery_key: str,
        source_system: str,
        report_type: str,
        report_date: date,
        original_filename: str,
        file_checksum: str,
        manifest_checksum: str,
        schema_version: str,
        declared_row_count: int,
        observed_row_count: int,
    ) -> bool:
        """Insert a batch.

        Returns True when inserted and False when delivery_key
        already exists.
        """

        with self._connection.cursor() as cursor:
            cursor.execute(
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
                    archive_status
                )
                VALUES (
                    %(batch_id)s,
                    %(delivery_key)s,
                    %(source_system)s,
                    %(report_type)s,
                    %(report_date)s,
                    %(original_filename)s,
                    %(file_checksum)s,
                    %(manifest_checksum)s,
                    %(schema_version)s,
                    %(declared_row_count)s,
                    %(observed_row_count)s,
                    'RECEIVED',
                    'PENDING'
                )
                ON CONFLICT (delivery_key)
                DO NOTHING
                RETURNING batch_id
                """,
                {
                    "batch_id": batch_id,
                    "delivery_key": delivery_key,
                    "source_system": source_system,
                    "report_type": report_type,
                    "report_date": report_date,
                    "original_filename": original_filename,
                    "file_checksum": file_checksum,
                    "manifest_checksum": manifest_checksum,
                    "schema_version": schema_version,
                    "declared_row_count": (
                        declared_row_count
                    ),
                    "observed_row_count": (
                        observed_row_count
                    ),
                },
            )

            return cursor.fetchone() is not None

    def get_batch_by_delivery_key(
        self,
        delivery_key: str,
    ) -> StoredBatch:
        with self._connection.cursor(
            row_factory=dict_row
        ) as cursor:
            cursor.execute(
                """
                SELECT
                    batch_id,
                    delivery_key,
                    ingestion_status,
                    observed_row_count,
                    file_checksum,
                    manifest_checksum
                FROM ingestion_batch
                WHERE delivery_key = %s
                """,
                (delivery_key,),
            )

            row = cursor.fetchone()

        if row is None:
            raise LookupError(
                "ingestion batch was not found"
            )

        return StoredBatch(
            batch_id=row["batch_id"],
            delivery_key=row["delivery_key"],
            ingestion_status=row["ingestion_status"],
            observed_row_count=row[
                "observed_row_count"
            ],
            file_checksum=row["file_checksum"],
            manifest_checksum=row[
                "manifest_checksum"
            ],
        )

    def insert_receipts(
        self,
        receipts: Sequence[PreparedReceipt],
    ) -> None:
        if not receipts:
            return

        parameters = [
            {
                "receipt_id": receipt.receipt_id,
                "batch_id": receipt.batch_id,
                "row_number": receipt.row_number,
                "entity_type": receipt.entity_type,
                "source_id": receipt.source_id,
                "source_version": (
                    receipt.source_version
                ),
                "payload_hash": receipt.payload_hash,
                "raw_payload": Jsonb(
                    dict(receipt.raw_payload)
                ),
                "disposition": receipt.disposition,
                "reason_code": receipt.reason_code,
            }
            for receipt in receipts
        ]

        with self._connection.cursor() as cursor:
            cursor.executemany(
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
                    reason_code
                )
                VALUES (
                    %(receipt_id)s,
                    %(batch_id)s,
                    %(row_number)s,
                    %(entity_type)s,
                    %(source_id)s,
                    %(source_version)s,
                    %(payload_hash)s,
                    %(raw_payload)s,
                    %(disposition)s,
                    %(reason_code)s
                )
                ON CONFLICT (
                    batch_id,
                    row_number
                )
                DO NOTHING
                """,
                parameters,
            )

    def set_batch_status(
        self,
        *,
        batch_id: UUID,
        status: str,
        error_message: str | None,
    ) -> None:
        if status not in {
            "PROCESSING",
            "FAILED",
        }:
            raise ValueError(
                "Step 4 can only produce PROCESSING or FAILED"
            )

        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ingestion_batch
                SET
                    ingestion_status = %(status)s,
                    error_message = %(error_message)s
                WHERE batch_id = %(batch_id)s
                """,
                {
                    "batch_id": batch_id,
                    "status": status,
                    "error_message": error_message,
                },
            )

            if cursor.rowcount != 1:
                raise LookupError(
                    "batch status update affected "
                    f"{cursor.rowcount} rows"
                )

    def receipt_counts(
        self,
        batch_id: UUID,
    ) -> ReceiptCounts:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (
                        WHERE disposition = 'PENDING'
                    ) AS processable,
                    COUNT(*) FILTER (
                        WHERE disposition = 'QUARANTINED'
                    ) AS quarantined
                FROM source_receipt
                WHERE batch_id = %s
                """,
                (batch_id,),
            )

            row = cursor.fetchone()

        return ReceiptCounts(
            total=int(row[0]),
            processable=int(row[1]),
            quarantined=int(row[2]),
        )