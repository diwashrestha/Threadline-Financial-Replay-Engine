"""Publish and validate complete source files before database ingestion.

Producer contract:
- One producer per delivery filename.
- Every delivery uses a unique filename.
- Published data and manifests are immutable.
- Data and temporary files live in the same directory.
- Intended for the project's WSL/Linux filesystem.
"""

from __future__ import annotations

import hashlib
import json
import os
import re

from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any


SUPPORTED_SCHEMA_VERSIONS = frozenset({"1"})

# Manifest labels use enum MEMBER NAMES, not enum values.
REPORT_ENTITIES = {
    "ORDERS": "ORDER",
    "PAYMENTS": "PAYMENT",
    "REFUNDS": "REFUND",
    "FEES": "FEE",
    "SETTLEMENT_LINES": "SETTLEMENT_LINE",
    "PAYOUTS": "PAYOUT",
}


class FileGateState(str, Enum):
    WAITING = "WAITING"
    REJECTED = "REJECTED"
    READY = "READY"


@dataclass(frozen=True)
class SourceFileManifest:
    filename: str
    source_system: str
    report_type: str
    report_date: date
    entity_type: str
    schema_version: str
    row_count: int
    checksum_sha256: str


@dataclass(frozen=True)
class ReadySourceFile:
    data_path: Path
    manifest_path: Path
    manifest: SourceFileManifest
    records: tuple[dict[str, Any], ...]
    file_bytes: bytes
    manifest_bytes: bytes


@dataclass(frozen=True)
class FileGateResult:
    state: FileGateState
    reason_code: str
    ready: ReadySourceFile | None = None


def source_file_paths(
    data_path: str | Path,
) -> tuple[Path, Path, Path, Path]:
    data = Path(data_path)

    if data.suffix != ".json" or data.name.endswith(".manifest.json"):
        raise ValueError("Expected a data filename such as payments.json")

    data_part = data.with_name(f"{data.name}.part")
    manifest = data.with_name(f"{data.stem}.manifest.json")
    manifest_part = manifest.with_name(f"{manifest.name}.part")

    return data, data_part, manifest, manifest_part


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _reject_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON constant: {value}")


def _unique_object(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}

    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key: {key}")

        result[key] = value

    return result


def _load_json(content: bytes) -> Any:
    return json.loads(
        content,
        parse_constant=_reject_constant,
        object_pairs_hook=_unique_object,
    )


def _parse_manifest(
    content: bytes,
    *,
    expected_filename: str,
) -> SourceFileManifest:
    document = _load_json(content)

    if not isinstance(document, dict):
        raise ValueError("Manifest must be a JSON object")

    text_fields = (
        "filename",
        "source_system",
        "report_type",
        "report_date",
        "entity_type",
        "schema_version",
        "checksum_sha256",
    )

    for field in text_fields:
        value = document.get(field)

        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Manifest field {field} must be a string")

    if document["filename"] != expected_filename:
        raise ValueError("Manifest filename does not match the data file")

    expected_entity = REPORT_ENTITIES.get(document["report_type"])

    if (
        expected_entity is None
        or document["entity_type"] != expected_entity
    ):
        raise ValueError("Unsupported report/entity combination")

    row_count = document.get("row_count")

    # Reject bool: Python otherwise considers it an integer.
    if type(row_count) is not int or row_count < 0:
        raise ValueError("Manifest row_count must be a nonnegative integer")

    checksum = document["checksum_sha256"]

    if re.fullmatch(r"[0-9a-f]{64}", checksum) is None:
        raise ValueError("Manifest checksum must be lowercase SHA-256")

    report_date = date.fromisoformat(document["report_date"])

    if report_date.isoformat() != document["report_date"]:
        raise ValueError("Manifest report_date must use YYYY-MM-DD")

    return SourceFileManifest(
        filename=document["filename"],
        source_system=document["source_system"],
        report_type=document["report_type"],
        report_date=report_date,
        entity_type=document["entity_type"],
        schema_version=document["schema_version"],
        row_count=row_count,
        checksum_sha256=checksum,
    )


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(
        directory,
        os.O_RDONLY | os.O_DIRECTORY,
    )

    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_and_fsync(path: Path, content: bytes) -> None:
    # Exclusive creation prevents overwriting a previous temporary file.
    with path.open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


def publish_source_file(
    data_path: str | Path,
    *,
    records: Sequence[Mapping[str, Any]],
    source_system: str,
    report_type: str,
    report_date: date,
    entity_type: str,
    schema_version: str = "1",
    failure_hook: Callable[[str], None] | None = None,
) -> Path:
    """Publish data first and its completion manifest last.

    Failure leaves evidence on disk. Do not reuse a failed filename
    automatically; inspect it or retry under a new delivery filename.
    """
    if os.name != "posix":
        raise RuntimeError("This producer requires WSL/Linux")

    data, data_part, manifest, manifest_part = source_file_paths(
        data_path
    )

    if not data.parent.is_dir():
        raise FileNotFoundError(
            f"Create the inbox directory first: {data.parent}"
        )

    if (
        not isinstance(report_date, date)
        or isinstance(report_date, datetime)
    ):
        raise TypeError("report_date must be a date")

    if any(
        path.exists()
        for path in (data, data_part, manifest, manifest_part)
    ):
        raise FileExistsError(
            "Delivery filename already exists; use a unique filename"
        )

    rows = [dict(record) for record in records]
    file_bytes = _json_bytes(rows)

    manifest_document = {
        "filename": data.name,
        "source_system": source_system,
        "report_type": report_type,
        "report_date": report_date.isoformat(),
        "entity_type": entity_type,
        "schema_version": schema_version,
        "row_count": len(rows),
        "checksum_sha256": sha256_bytes(file_bytes),
    }

    manifest_bytes = _json_bytes(manifest_document)

    # Validate metadata before creating any files.
    _parse_manifest(
        manifest_bytes,
        expected_filename=data.name,
    )

    visit = failure_hook if failure_hook is not None else lambda _: None

    # Claim the filename using exclusive creation.
    with data_part.open("xb") as stream:
        # Recheck after claiming, in case another producer finished
        # between the initial check and exclusive creation.
        if any(
            path.exists()
            for path in (data, manifest, manifest_part)
        ):
            raise FileExistsError("Delivery filename was already published")

        stream.write(file_bytes)
        stream.flush()
        os.fsync(stream.fileno())

    visit("after_data_fsync")

    os.rename(data_part, data)
    _fsync_directory(data.parent)
    visit("after_data_rename")

    _write_and_fsync(manifest_part, manifest_bytes)
    visit("after_manifest_fsync")

    # The final manifest is the completion marker.
    os.rename(manifest_part, manifest)
    _fsync_directory(manifest.parent)
    visit("after_manifest_rename")

    return manifest


def inspect_source_file(
    data_path: str | Path,
    *,
    supported_schema_versions: Collection[str] = (
        SUPPORTED_SCHEMA_VERSIONS
    ),
) -> FileGateResult:
    """Read and verify a source delivery without touching the database."""
    data, data_part, manifest, manifest_part = source_file_paths(
        data_path
    )

    if data_part.exists() or manifest_part.exists():
        return FileGateResult(
            FileGateState.WAITING,
            "TEMPORARY_FILE_PRESENT",
        )

    if not manifest.is_file():
        return FileGateResult(
            FileGateState.WAITING,
            "MANIFEST_MISSING",
        )

    try:
        manifest_bytes = manifest.read_bytes()
    except FileNotFoundError:
        return FileGateResult(
            FileGateState.WAITING,
            "MANIFEST_MISSING",
        )

    try:
        parsed_manifest = _parse_manifest(
            manifest_bytes,
            expected_filename=data.name,
        )
    except ValueError:
        return FileGateResult(
            FileGateState.REJECTED,
            "INVALID_MANIFEST",
        )

    if parsed_manifest.schema_version not in supported_schema_versions:
        return FileGateResult(
            FileGateState.REJECTED,
            "UNSUPPORTED_SCHEMA_VERSION",
        )

    try:
        file_bytes = data.read_bytes()
    except FileNotFoundError:
        return FileGateResult(
            FileGateState.WAITING,
            "DATA_FILE_MISSING",
        )

    # Verify exactly the bytes that will be passed to ingestion.
    if sha256_bytes(file_bytes) != parsed_manifest.checksum_sha256:
        return FileGateResult(
            FileGateState.REJECTED,
            "CHECKSUM_MISMATCH",
        )

    try:
        records = _load_json(file_bytes)
    except ValueError:
        return FileGateResult(
            FileGateState.REJECTED,
            "INVALID_DATA_JSON",
        )

    if (
        not isinstance(records, list)
        or any(not isinstance(record, dict) for record in records)
    ):
        return FileGateResult(
            FileGateState.REJECTED,
            "INVALID_DATA_STRUCTURE",
        )

    if len(records) != parsed_manifest.row_count:
        return FileGateResult(
            FileGateState.REJECTED,
            "ROW_COUNT_MISMATCH",
        )

    # Recheck before allowing ingestion.
    if data_part.exists() or manifest_part.exists():
        return FileGateResult(
            FileGateState.WAITING,
            "TEMPORARY_FILE_PRESENT",
        )

    return FileGateResult(
        state=FileGateState.READY,
        reason_code="VERIFIED",
        ready=ReadySourceFile(
            data_path=data,
            manifest_path=manifest,
            manifest=parsed_manifest,
            records=tuple(records),
            file_bytes=file_bytes,
            manifest_bytes=manifest_bytes,
        ),
    )


def ingest_ready_source(
    data_path: str | Path,
    *,
    ingest: Callable[[ReadySourceFile], Any],
    supported_schema_versions: Collection[str] = (
        SUPPORTED_SCHEMA_VERSIONS
    ),
) -> FileGateResult:
    """Call ingestion only after the complete delivery passes validation."""
    result = inspect_source_file(
        data_path,
        supported_schema_versions=supported_schema_versions,
    )

    if result.state is FileGateState.READY:
        assert result.ready is not None
        ingest(result.ready)

    # Database failures propagate from ingest; they are not swallowed.
    return result