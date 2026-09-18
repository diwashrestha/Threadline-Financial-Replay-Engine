import json
from pathlib import Path

import pytest

from threadline.contracts import ReportType
from threadline.durable_ingestion import DeliveryIntegrityError
from tests.integration.recovery_scenarios import payment


pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "unfinished_state",
    [
        "manifest_missing",
        "data_part_exists",
        "manifest_part_exists",
    ],
)
def test_unfinished_delivery_creates_no_database_batch(
    scenario,
    unfinished_state,
):
    path = scenario.make_file(
        ReportType.PAYMENTS,
        [payment()],
    )

    manifest_path = path.with_suffix(".manifest.json")

    if unfinished_state == "manifest_missing":
        manifest_path.unlink()
    elif unfinished_state == "data_part_exists":
        Path(f"{path}.part").write_bytes(b"unfinished")
    else:
        Path(f"{manifest_path}.part").write_bytes(b"unfinished")

    outcome = scenario.ingest(path)

    assert outcome is None
    assert path.exists()

    for table in (
        "ingestion_batch",
        "source_receipt",
        "source_record_version",
        "entity_resolution",
        "recovery_request",
        "verified_delivery_evidence",
    ):
        assert scenario.scalar(
            f"SELECT COUNT(*) FROM {table}"
        ) == 0


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("checksum_sha256", "0" * 64),
        ("row_count", 999),
        ("schema_version", "999"),
    ],
)
def test_invalid_manifest_cannot_create_committed_batch(
    scenario,
    field,
    bad_value,
):
    path = scenario.make_file(
        ReportType.PAYMENTS,
        [payment()],
    )

    manifest_path = path.with_suffix(".manifest.json")
    manifest = json.loads(manifest_path.read_text())
    manifest[field] = bad_value

    manifest_path.write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    with pytest.raises(DeliveryIntegrityError):
        scenario.ingest(path)

    assert path.exists()
    assert scenario.scalar(
        "SELECT COUNT(*) FROM ingestion_batch"
    ) == 0
    assert scenario.scalar(
        "SELECT COUNT(*) FROM recovery_request"
    ) == 0