from pathlib import Path

import pytest

import threadline.archival as archival
from threadline.contracts import ReportType
from tests.integration.recovery_scenarios import payment


pytestmark = pytest.mark.integration


def test_f03_archive_copy_failure_is_retryable(
    scenario,
    monkeypatch,
):
    scenario.clean_baseline()

    path, delivery = scenario.deliver(
        ReportType.PAYMENTS,
        [payment()],
    )
    scenario.recover()

    previous = scenario.published()

    def interrupted_copy(reader, writer, *args, **kwargs):
        writer.write(reader.read(8))
        raise OSError("Injected archive copy interruption")

    with monkeypatch.context() as patch:
        patch.setattr(
            archival.shutil,
            "copyfileobj",
            interrupted_copy,
        )

        with pytest.raises(OSError, match="copy interruption"):
            scenario.archive_batch(delivery.batch_id)

    assert path.exists()

    batch = scenario.rows(
        """
        SELECT ingestion_status, archive_status, archive_error_message
        FROM ingestion_batch
        WHERE batch_id = %s
        """,
        (delivery.batch_id,),
    )[0]

    assert batch["ingestion_status"] == "COMMITTED"
    assert batch["archive_status"] == "FAILED"
    assert batch["archive_error_message"]

    assert (
        scenario.published()["current_run_id"]
        == previous["current_run_id"]
    )
    assert (
        scenario.published()["logical_fingerprint"]
        == previous["logical_fingerprint"]
    )

    # Any retained temporary object must not be treated as the final archive.
    destination = (
        scenario.archive
        / archival.archive_relative_path(
            ReportType.PAYMENTS,
            scenario.rows(
                """
                SELECT report_date
                FROM ingestion_batch
                WHERE batch_id = %s
                """,
                (delivery.batch_id,),
            )[0]["report_date"],
            scenario.rows(
                """
                SELECT file_checksum
                FROM ingestion_batch
                WHERE batch_id = %s
                """,
                (delivery.batch_id,),
            )[0]["file_checksum"],
        )
    )

    assert not destination.exists()

    outcome = scenario.archive_batch(delivery.batch_id)

    assert Path(outcome.archive_path).is_file()
    assert not path.exists()

    assert scenario.rows(
        """
        SELECT archive_status
        FROM ingestion_batch
        WHERE batch_id = %s
        """,
        (delivery.batch_id,),
    )[0]["archive_status"] == "ARCHIVED"

    assert (
        scenario.published()["logical_fingerprint"]
        == previous["logical_fingerprint"]
    )