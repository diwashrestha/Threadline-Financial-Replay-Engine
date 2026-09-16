"""Deterministic hashing for Threadline source deliveries."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date
from typing import Any

from threadline.contracts import EntityType, ReportType

from threadline.source_files import (
    ReadySourceFile,
    ingest_ready_source,
)

def sha256_bytes(content: bytes) -> str:
    if not isinstance(content, bytes):
        raise TypeError("content must be bytes")

    return hashlib.sha256(content).hexdigest()


def canonical_json_bytes(
    payload: Mapping[str, Any],
) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def payload_sha256(
    payload: Mapping[str, Any],
) -> str:
    return sha256_bytes(
        canonical_json_bytes(payload)
    )


def normalize_filename(filename: str) -> str:
    """Return only the final path component.

    Both POSIX and Windows separators are accepted.
    """

    normalized = filename.replace("\\", "/")
    result = normalized.rsplit("/", maxsplit=1)[-1]

    if not result:
        raise ValueError("filename must not be empty")

    return result


def calculate_delivery_key(
    *,
    source_system: str,
    report_type: ReportType,
    report_date: date,
    entity_type: EntityType,
    original_filename: str,
    schema_version: str,
    file_checksum: str,
    manifest_checksum: str,
) -> str:
    """Calculate the identity of one transport delivery.

    Filename is included deliberately. The same content delivered
    under another filename becomes another auditable batch while
    row-level identity rules will later prevent financial duplication.
    """

    identity = {
        "source_system": source_system,
        "report_type": report_type.value,
        "report_date": report_date.isoformat(),
        "entity_type": entity_type.value,
        "original_filename": normalize_filename(
            original_filename
        ),
        "schema_version": schema_version,
        "file_checksum": file_checksum,
        "manifest_checksum": manifest_checksum,
    }

    return sha256_bytes(
        canonical_json_bytes(identity)
    )