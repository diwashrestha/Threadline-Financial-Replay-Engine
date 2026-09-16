"""Register complete, verified source deliveries in the ingestion ledger.

Incomplete or invalid files never reach the database registration method.

This module does not:
- Resolve canonical records.
- Mark ingestion batches COMMITTED.
- Publish financial results.
- Archive source files.

Those operations remain in the existing ingestion pipeline.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from threadline.completeness import ReportType
from threadline.contracts import EntityType
from threadline.ingestion_ledger import SourceDelivery

from threadline.source_files import (
    SUPPORTED_SCHEMA_VERSIONS,
    FileGateState,
    ReadySourceFile,
    inspect_source_file,
)


class LedgerService(Protocol):
    """Interface required from the existing ingestion ledger service."""

    def record_delivery(self, delivery: SourceDelivery) -> Any:
        ...


@dataclass(frozen=True)
class FileIngestionResult:
    """File readiness plus the result returned by ledger registration.

    READY means the source passed validation and registration returned.
    It does not mean the business ingestion transaction is complete.
    """

    state: FileGateState
    reason_code: str
    ledger_result: Any | None = None


def register_verified_delivery(
    ready: ReadySourceFile,
    *,
    ledger_service: LedgerService,
) -> Any:
    """Register the exact bytes and records verified by the file gate."""
    manifest = ready.manifest

    delivery = SourceDelivery(
        source_system=manifest.source_system,
        report_type=ReportType[manifest.report_type],
        report_date=manifest.report_date,
        entity_type=EntityType[manifest.entity_type],
        original_filename=ready.data_path.name,
        schema_version=manifest.schema_version,
        declared_row_count=manifest.row_count,
        records=ready.records,
        file_bytes=ready.file_bytes,
        manifest_bytes=ready.manifest_bytes,
    )

    return ledger_service.record_delivery(delivery)


def ingest_file(
    source_path: str | Path,
    *,
    ledger_service: LedgerService,
    supported_schema_versions: Collection[str] = (
        SUPPORTED_SCHEMA_VERSIONS
    ),
) -> FileIngestionResult:
    """Validate a source file before registering its delivery.

    WAITING:
        Missing files or temporary files prevent registration.

    REJECTED:
        Invalid manifests, checksums, counts, structures, or schema
        versions prevent registration.

    READY:
        The verified delivery is passed to record_delivery().

    Filesystem and database exceptions propagate to the caller so
    the task can fail and retry.
    """
    gate = inspect_source_file(
        source_path,
        supported_schema_versions=supported_schema_versions,
    )

    if gate.state is not FileGateState.READY:
        return FileIngestionResult(
            state=gate.state,
            reason_code=gate.reason_code,
        )

    if gate.ready is None:
        raise RuntimeError(
            "File gate returned READY without a verified delivery"
        )

    ledger_result = register_verified_delivery(
        gate.ready,
        ledger_service=ledger_service,
    )

    return FileIngestionResult(
        state=FileGateState.READY,
        reason_code=gate.reason_code,
        ledger_result=ledger_result,
    )