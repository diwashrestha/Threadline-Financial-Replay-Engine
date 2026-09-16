from __future__ import annotations

import hashlib
import json

from datetime import date

import pytest

from threadline.source_files import (
    FileGateState,
    ingest_ready_source,
    publish_source_file,
    source_file_paths,
)


@pytest.fixture
def completed_file(tmp_path):
    path = tmp_path / "payments.json"

    publish_source_file(
        path,
        records=[
            {
                "payment_id": "PAY-FILE-001",
                "order_id": "ORD-FILE-001",
                "attempt_number": 1,
                "payment_method": "CARD",
                "status": "CAPTURED",
                "amount": "100.00",
                "currency": "EUR",
                "effective_at_utc": "2026-09-15T08:00:00Z",
                "available_on": "2026-09-15",
                "source_version": 1,
            }
        ],
        source_system="PAYMENT_PROVIDER",
        report_type="PAYMENTS",
        report_date=date(2026, 9, 15),
        entity_type="PAYMENT",
    )

    return path


def change_manifest(data_path, **changes):
    _, _, manifest_path, _ = source_file_paths(data_path)

    document = json.loads(manifest_path.read_bytes())
    document.update(changes)

    manifest_path.write_text(
        json.dumps(document),
        encoding="utf-8",
    )


def assert_not_ingested(data_path, expected_state, expected_reason):
    calls = []

    result = ingest_ready_source(
        data_path,
        ingest=calls.append,
    )

    assert result.state is expected_state
    assert result.reason_code == expected_reason
    assert result.ready is None
    assert calls == []


def test_completed_file_reaches_ingestion(completed_file):
    calls = []

    result = ingest_ready_source(
        completed_file,
        ingest=calls.append,
    )

    assert result.state is FileGateState.READY
    assert len(calls) == 1

    ready = calls[0]

    assert ready.manifest.row_count == 1
    assert ready.records[0]["payment_id"] == "PAY-FILE-001"
    assert ready.file_bytes == completed_file.read_bytes()


def test_missing_manifest_does_not_reach_ingestion(completed_file):
    _, _, manifest, _ = source_file_paths(completed_file)
    manifest.unlink()

    assert_not_ingested(
        completed_file,
        FileGateState.WAITING,
        "MANIFEST_MISSING",
    )


@pytest.mark.parametrize("temporary_kind", ["data", "manifest"])
def test_temporary_file_blocks_ingestion(completed_file, temporary_kind):
    _, data_part, _, manifest_part = source_file_paths(completed_file)

    temporary = (
        data_part
        if temporary_kind == "data"
        else manifest_part
    )

    temporary.write_bytes(b"unfinished")

    assert_not_ingested(
        completed_file,
        FileGateState.WAITING,
        "TEMPORARY_FILE_PRESENT",
    )


def test_missing_data_does_not_reach_ingestion(completed_file):
    completed_file.unlink()

    assert_not_ingested(
        completed_file,
        FileGateState.WAITING,
        "DATA_FILE_MISSING",
    )


def test_partial_json_without_manifest_waits(tmp_path):
    path = tmp_path / "payments.json"
    path.write_bytes(b'[{"payment_id":')

    assert_not_ingested(
        path,
        FileGateState.WAITING,
        "MANIFEST_MISSING",
    )


def test_checksum_mismatch_does_not_reach_ingestion(completed_file):
    completed_file.write_bytes(b"[]")

    assert_not_ingested(
        completed_file,
        FileGateState.REJECTED,
        "CHECKSUM_MISMATCH",
    )


def test_row_count_mismatch_does_not_reach_ingestion(completed_file):
    change_manifest(completed_file, row_count=2)

    assert_not_ingested(
        completed_file,
        FileGateState.REJECTED,
        "ROW_COUNT_MISMATCH",
    )


def test_unsupported_schema_does_not_reach_ingestion(completed_file):
    change_manifest(completed_file, schema_version="999")

    assert_not_ingested(
        completed_file,
        FileGateState.REJECTED,
        "UNSUPPORTED_SCHEMA_VERSION",
    )


def test_wrong_manifest_filename_is_rejected(completed_file):
    change_manifest(completed_file, filename="another-file.json")

    assert_not_ingested(
        completed_file,
        FileGateState.REJECTED,
        "INVALID_MANIFEST",
    )


def test_boolean_row_count_is_rejected(completed_file):
    change_manifest(completed_file, row_count=True)

    assert_not_ingested(
        completed_file,
        FileGateState.REJECTED,
        "INVALID_MANIFEST",
    )


def test_invalid_json_with_matching_checksum_is_rejected(completed_file):
    content = b'[{"payment_id":'

    completed_file.write_bytes(content)

    change_manifest(
        completed_file,
        checksum_sha256=hashlib.sha256(content).hexdigest(),
    )

    assert_not_ingested(
        completed_file,
        FileGateState.REJECTED,
        "INVALID_DATA_JSON",
    )


def test_wrong_data_structure_is_rejected(completed_file):
    content = b'{"payment_id":"PAY-FILE-001"}'

    completed_file.write_bytes(content)

    change_manifest(
        completed_file,
        checksum_sha256=hashlib.sha256(content).hexdigest(),
    )

    assert_not_ingested(
        completed_file,
        FileGateState.REJECTED,
        "INVALID_DATA_STRUCTURE",
    )


@pytest.mark.parametrize(
    ("failure_point", "expected_state"),
    [
        ("after_data_fsync", FileGateState.WAITING),
        ("after_data_rename", FileGateState.WAITING),
        ("after_manifest_fsync", FileGateState.WAITING),
        ("after_manifest_rename", FileGateState.READY),
    ],
)
def test_producer_failure_respects_completion_marker(
    tmp_path,
    failure_point,
    expected_state,
):
    path = tmp_path / "payments.json"

    def fail(point):
        if point == failure_point:
            raise RuntimeError(f"Producer failure at {point}")

    with pytest.raises(RuntimeError, match="Producer failure"):
        publish_source_file(
            path,
            records=[{"payment_id": "PAY-FILE-001"}],
            source_system="PAYMENT_PROVIDER",
            report_type="PAYMENTS",
            report_date=date(2026, 9, 15),
            entity_type="PAYMENT",
            failure_hook=fail,
        )

    calls = []

    result = ingest_ready_source(
        path,
        ingest=calls.append,
    )

    assert result.state is expected_state

    if expected_state is FileGateState.READY:
        # Publication finished even though the producer lost its
        # success acknowledgement.
        assert len(calls) == 1
    else:
        assert calls == []


def test_producer_cannot_overwrite_published_delivery(completed_file):
    original_bytes = completed_file.read_bytes()

    with pytest.raises(FileExistsError):
        publish_source_file(
            completed_file,
            records=[],
            source_system="PAYMENT_PROVIDER",
            report_type="PAYMENTS",
            report_date=date(2026, 9, 15),
            entity_type="PAYMENT",
        )

    assert completed_file.read_bytes() == original_bytes