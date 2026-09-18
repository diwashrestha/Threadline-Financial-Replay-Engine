import subprocess
import time

from decimal import Decimal
from pathlib import Path

import psycopg
import pytest

from threadline.contracts import ReportType
from tests.integration.recovery_scenarios import payment


pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTAGE_SERVICE = "threadline-outage-postgres"


class InjectedIngestionFailure(RuntimeError):
    pass


def fail_at(expected_point):
    def hook(point):
        if point == expected_point:
            raise InjectedIngestionFailure(point)

    return hook


def test_f01_failure_before_commit_rolls_back_resolution(scenario):
    previous = scenario.clean_baseline()

    previous_resolution = scenario.rows(
        """
        SELECT *
        FROM entity_resolution
        WHERE entity_type = 'PAYMENT' AND source_id = 'PAY-001'
        """
    )[0]

    batch_count = scenario.scalar(
        "SELECT COUNT(*) FROM ingestion_batch"
    )
    history_count = scenario.scalar(
        "SELECT COUNT(*) FROM source_record_version"
    )
    request_count = scenario.scalar(
        "SELECT COUNT(*) FROM recovery_request"
    )

    path = scenario.make_file(
        ReportType.PAYMENTS,
        [payment(amount="120.00", version=2)],
    )

    with pytest.raises(InjectedIngestionFailure):
        scenario.ingest(
            path,
            failure_hook=fail_at("after_entity_resolution"),
        )

    assert path.exists()
    assert scenario.scalar(
        "SELECT COUNT(*) FROM ingestion_batch"
    ) == batch_count
    assert scenario.scalar(
        "SELECT COUNT(*) FROM source_record_version"
    ) == history_count
    assert scenario.scalar(
        "SELECT COUNT(*) FROM recovery_request"
    ) == request_count

    assert scenario.scalar(
        """
        SELECT COUNT(*)
        FROM ingestion_batch
        WHERE original_filename = %s
          AND ingestion_status = 'COMMITTED'
        """,
        (path.name,),
    ) == 0

    assert scenario.rows(
        """
        SELECT *
        FROM entity_resolution
        WHERE entity_type = 'PAYMENT' AND source_id = 'PAY-001'
        """
    )[0] == previous_resolution

    assert (
        scenario.published()["current_run_id"]
        == previous["current_run_id"]
    )

    retry = scenario.ingest(path)
    assert retry is not None

    scenario.recover()

    assert Decimal(
        scenario.transaction()["captured_total"]
    ) == Decimal("120.00")


def test_f02_failure_after_commit_has_one_ingestion_effect(scenario):
    scenario.clean_baseline()

    path = scenario.make_file(
        ReportType.PAYMENTS,
        [payment(amount="120.00", version=2)],
    )

    with pytest.raises(InjectedIngestionFailure):
        scenario.ingest(
            path,
            failure_hook=fail_at("after_ingestion_commit"),
        )

    batch = scenario.rows(
        """
        SELECT *
        FROM ingestion_batch
        WHERE original_filename = %s
        """,
        (path.name,),
    )[0]

    assert batch["ingestion_status"] == "COMMITTED"
    assert batch["archive_status"] == "PENDING"
    assert path.exists()

    assert scenario.rows(
        """
        SELECT winning_version, resolution_state
        FROM entity_resolution
        WHERE entity_type = 'PAYMENT' AND source_id = 'PAY-001'
        """
    )[0] == {
        "winning_version": 2,
        "resolution_state": "ACCEPTED",
    }

    retry = scenario.ingest(path)

    assert retry is not None
    assert retry.reused_existing_batch
    assert retry.batch_id == batch["batch_id"]

    assert scenario.scalar(
        """
        SELECT COUNT(*)
        FROM source_receipt
        WHERE batch_id = %s
        """,
        (batch["batch_id"],),
    ) == 1

    assert scenario.scalar(
        """
        SELECT COUNT(*)
        FROM recovery_request
        WHERE trigger_batch_id = %s
        """,
        (batch["batch_id"],),
    ) == 1

    scenario.recover()

    assert Decimal(
        scenario.transaction()["captured_total"]
    ) == Decimal("120.00")

    outcome = scenario.archive_batch(batch["batch_id"])

    assert Path(outcome.archive_path).is_file()
    assert not path.exists()

    assert scenario.rows(
        """
        SELECT archive_status
        FROM ingestion_batch
        WHERE batch_id = %s
        """,
        (batch["batch_id"],),
    )[0]["archive_status"] == "ARCHIVED"


def compose(*arguments):
    subprocess.run(
        [
            "docker",
            "compose",
            "--profile",
            "outage-test",
            *arguments,
        ],
        cwd=PROJECT_ROOT,
        check=True,
        timeout=60,
    )


def wait_for_postgres(database_url):
    deadline = time.monotonic() + 45

    while time.monotonic() < deadline:
        try:
            with psycopg.connect(
                database_url,
                connect_timeout=1,
                autocommit=True,
            ) as connection:
                connection.execute("SELECT 1")
            return
        except psycopg.OperationalError:
            time.sleep(0.5)

    raise AssertionError("Outage database did not recover")


def test_f05_real_database_outage_then_ingestion_retry(outage_scenario):
    scenario = outage_scenario
    previous = scenario.clean_baseline()

    path = scenario.make_file(
        ReportType.PAYMENTS,
        [payment(amount="120.00", version=2)],
    )

    try:
        compose("stop", OUTAGE_SERVICE)

        with pytest.raises(psycopg.OperationalError):
            scenario.ingest(path)

        assert path.exists()
        assert path.with_suffix(".manifest.json").exists()

    finally:
        compose("start", OUTAGE_SERVICE)
        wait_for_postgres(scenario.database_url)

    # The published result survived the actual PostgreSQL restart.
    assert (
        scenario.published()["current_run_id"]
        == previous["current_run_id"]
    )

    first = scenario.ingest(path)
    second = scenario.ingest(path)

    assert first is not None
    assert second is not None

    assert first.batch_id == second.batch_id
    assert first.request_id == second.request_id

    assert scenario.scalar(
        """
        SELECT COUNT(*)
        FROM source_receipt
        WHERE batch_id = %s
        """,
        (first.batch_id,),
    ) == 1

    scenario.recover()

    assert Decimal(
        scenario.transaction()["captured_total"]
    ) == Decimal("120.00")