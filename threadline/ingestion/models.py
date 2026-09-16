"""Input and output models for the durable ingestion ledger."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any
from uuid import UUID

from threadline.contracts import EntityType, ReportType


@dataclass(frozen=True, slots=True)
class SourceDelivery:
    source_system: str
    report_type: ReportType
    report_date: date
    entity_type: EntityType

    original_filename: str
    schema_version: str

    declared_row_count: int
    records: tuple[Mapping[str, Any], ...]

    file_bytes: bytes
    manifest_bytes: bytes

    # File checksum declared inside the manifest.
    expected_file_checksum: str | None = None

    def __post_init__(self) -> None:
        if (
            not self.source_system
            or self.source_system
            != self.source_system.strip()
        ):
            raise ValueError(
                "source_system must be a non-empty "
                "canonical string"
            )

        if (
            not self.original_filename
            or not self.original_filename.strip()
        ):
            raise ValueError(
                "original_filename must not be empty"
            )

        if (
            not self.schema_version
            or self.schema_version
            != self.schema_version.strip()
        ):
            raise ValueError(
                "schema_version must be a non-empty "
                "canonical string"
            )

        if self.declared_row_count < 0:
            raise ValueError(
                "declared_row_count must be non-negative"
            )

        if not isinstance(self.file_bytes, bytes):
            raise TypeError("file_bytes must be bytes")

        if not isinstance(self.manifest_bytes, bytes):
            raise TypeError("manifest_bytes must be bytes")


@dataclass(frozen=True, slots=True)
class PreparedReceipt:
    receipt_id: UUID
    batch_id: UUID
    row_number: int

    entity_type: str | None
    source_id: str | None
    source_version: int | None

    payload_hash: str
    raw_payload: Mapping[str, Any]

    disposition: str
    reason_code: str | None


@dataclass(frozen=True, slots=True)
class IngestionLedgerResult:
    batch_id: UUID
    delivery_key: str
    ingestion_status: str
    was_replay: bool

    observed_row_count: int
    processable_receipt_count: int
    quarantined_receipt_count: int

    file_checksum: str
    manifest_checksum: str