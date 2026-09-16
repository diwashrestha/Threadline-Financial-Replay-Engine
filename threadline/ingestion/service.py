"""Transactional service for recording source deliveries."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from threadline.contracts import (
    ContractViolation,
    parse_source_record,
    quarantine_from_violation,
)
from threadline.db.connection import transaction_connection
from threadline.db.ingestion_repository import (
    IngestionRepository,
)
from threadline.ingestion.hashing import (
    calculate_delivery_key,
    normalize_filename,
    payload_sha256,
    sha256_bytes,
)
from threadline.ingestion.models import (
    IngestionLedgerResult,
    PreparedReceipt,
    SourceDelivery,
)

from threadline.source_files import (
    ReadySourceFile,
    ingest_ready_source,
)

class IngestionLedgerService:
    def __init__(
        self,
        database_url: str | None = None,
    ):
        self._database_url = database_url

    def record_delivery(
        self,
        delivery: SourceDelivery,
    ) -> IngestionLedgerResult:
        records = tuple(delivery.records)

        file_checksum = sha256_bytes(
            delivery.file_bytes
        )
        manifest_checksum = sha256_bytes(
            delivery.manifest_bytes
        )

        delivery_key = calculate_delivery_key(
            source_system=delivery.source_system,
            report_type=delivery.report_type,
            report_date=delivery.report_date,
            entity_type=delivery.entity_type,
            original_filename=delivery.original_filename,
            schema_version=delivery.schema_version,
            file_checksum=file_checksum,
            manifest_checksum=manifest_checksum,
        )

        batch_issues = self._batch_issues(
            delivery=delivery,
            file_checksum=file_checksum,
            observed_row_count=len(records),
        )

        proposed_batch_id = uuid4()

        with transaction_connection(
            self._database_url
        ) as connection:
            repository = IngestionRepository(connection)

            inserted = repository.create_batch(
                batch_id=proposed_batch_id,
                delivery_key=delivery_key,
                source_system=delivery.source_system,
                report_type=delivery.report_type.value,
                report_date=delivery.report_date,
                original_filename=normalize_filename(
                    delivery.original_filename
                ),
                file_checksum=file_checksum,
                manifest_checksum=manifest_checksum,
                schema_version=delivery.schema_version,
                declared_row_count=(
                    delivery.declared_row_count
                ),
                observed_row_count=len(records),
            )

            stored_batch = (
                repository.get_batch_by_delivery_key(
                    delivery_key
                )
            )

            if inserted:
                receipts = self._prepare_receipts(
                    batch_id=stored_batch.batch_id,
                    delivery=delivery,
                    records=records,
                    batch_issues=batch_issues,
                )

                repository.insert_receipts(receipts)

                repository.set_batch_status(
                    batch_id=stored_batch.batch_id,
                    status=(
                        "FAILED"
                        if batch_issues
                        else "PROCESSING"
                    ),
                    error_message=(
                        "; ".join(batch_issues)
                        if batch_issues
                        else None
                    ),
                )

                stored_batch = (
                    repository.get_batch_by_delivery_key(
                        delivery_key
                    )
                )

            counts = repository.receipt_counts(
                stored_batch.batch_id
            )

            if counts.total != stored_batch.observed_row_count:
                raise RuntimeError(
                    "evidence conservation failed: "
                    f"batch declares "
                    f"{stored_batch.observed_row_count} "
                    f"observed rows but stores "
                    f"{counts.total} receipts"
                )

            return IngestionLedgerResult(
                batch_id=stored_batch.batch_id,
                delivery_key=delivery_key,
                ingestion_status=(
                    stored_batch.ingestion_status
                ),
                was_replay=not inserted,
                observed_row_count=counts.total,
                processable_receipt_count=(
                    counts.processable
                ),
                quarantined_receipt_count=(
                    counts.quarantined
                ),
                file_checksum=file_checksum,
                manifest_checksum=manifest_checksum,
            )

    @staticmethod
    def _batch_issues(
        *,
        delivery: SourceDelivery,
        file_checksum: str,
        observed_row_count: int,
    ) -> tuple[str, ...]:
        issues: list[str] = []

        if (
            delivery.declared_row_count
            != observed_row_count
        ):
            issues.append(
                "ROW_COUNT_MISMATCH: "
                f"manifest={delivery.declared_row_count}, "
                f"observed={observed_row_count}"
            )

        if (
            delivery.expected_file_checksum
            is not None
            and delivery.expected_file_checksum.lower()
            != file_checksum
        ):
            issues.append(
                "FILE_CHECKSUM_MISMATCH: "
                f"manifest="
                f"{delivery.expected_file_checksum.lower()}, "
                f"observed={file_checksum}"
            )

        return tuple(issues)

    @staticmethod
    def _receipt_id(
        *,
        batch_id: UUID,
        row_number: int,
        payload_hash: str,
    ) -> UUID:
        return uuid5(
            NAMESPACE_URL,
            (
                f"threadline:{batch_id}:"
                f"{row_number}:{payload_hash}"
            ),
        )

    def _prepare_receipts(
        self,
        *,
        batch_id: UUID,
        delivery: SourceDelivery,
        records: tuple[Mapping[str, Any], ...],
        batch_issues: tuple[str, ...],
    ) -> tuple[PreparedReceipt, ...]:
        prepared: list[PreparedReceipt] = []

        for row_number, raw_payload in enumerate(
            records,
            start=1,
        ):
            payload_hash = payload_sha256(
                raw_payload
            )

            receipt_id = self._receipt_id(
                batch_id=batch_id,
                row_number=row_number,
                payload_hash=payload_hash,
            )

            try:
                parsed = parse_source_record(
                    delivery.entity_type,
                    raw_payload,
                )
            except ContractViolation as violation:
                quarantined = quarantine_from_violation(
                    delivery.entity_type,
                    raw_payload,
                    violation,
                )

                prepared.append(
                    PreparedReceipt(
                        receipt_id=receipt_id,
                        batch_id=batch_id,
                        row_number=row_number,
                        entity_type=(
                            delivery.entity_type.value
                        ),
                        source_id=(
                            quarantined.source_id
                        ),
                        source_version=None,
                        payload_hash=payload_hash,
                        raw_payload=raw_payload,
                        disposition="QUARANTINED",
                        reason_code=(
                            quarantined.reason_code
                        ),
                    )
                )
                continue

            if batch_issues:
                disposition = "QUARANTINED"
                reason_code = "BATCH_VALIDATION_FAILED"
            else:
                disposition = "PENDING"
                reason_code = None

            prepared.append(
                PreparedReceipt(
                    receipt_id=receipt_id,
                    batch_id=batch_id,
                    row_number=row_number,
                    entity_type=(
                        delivery.entity_type.value
                    ),
                    source_id=parsed.record_id,
                    source_version=(
                        parsed.source_version
                    ),
                    payload_hash=payload_hash,
                    raw_payload=raw_payload,
                    disposition=disposition,
                    reason_code=reason_code,
                )
            )

        return tuple(prepared)