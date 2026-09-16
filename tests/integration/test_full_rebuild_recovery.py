from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from psycopg.rows import dict_row

from threadline.archival import archive_relative_path
from threadline.completeness import ReportType

from threadline.full_rebuild_recovery import (
    FullRebuildRecovery,
    enqueue_full_rebuild,
    lock_financial_state,
    read_published_result,
)


from threadline.recovery_fingerprint import (
    verify_stored_financial_result,
)


from threadline.failure_injection import (
    FailOnce,
    FailurePoint,
    InjectedFailure,
)


from threadline.recovery_behavior import (
    RecoveryAction,
    RecoveryStateError,
    inspect_recovery,
)

from uuid import uuid4

import hashlib
import json

from psycopg.types.json import Jsonb

from threadline.archival import (
    ArchiveService,
    ArchiveIntegrityError,
    archive_relative_path,
    sha256_file,
)

pytestmark = pytest.mark.integration

AS_OF = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)
PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def database_url():
    url = os.environ.get("THREADLINE_TEST_DATABASE_URL")
    if not url:
        pytest.skip("THREADLINE_TEST_DATABASE_URL is not configured")

    with psycopg.connect(url, autocommit=True) as connection:
        database_name = connection.execute(
            "SELECT current_database()"
        ).fetchone()[0]

        if not database_name.endswith("_test"):
            raise RuntimeError("Refusing to reset a non-test database")

        connection.execute("DROP SCHEMA public CASCADE")
        connection.execute("CREATE SCHEMA public")

    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.set_main_option(
        "script_location",
        str(PROJECT_ROOT / "migrations"),
    )
    config.attributes["database_url"] = url
    command.upgrade(config, "head")

    return url


def enqueue_test_request(database_url, *, label):
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        lock_financial_state(connection)

        batch = connection.execute(
            """
            INSERT INTO ingestion_batch (
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
                committed_at_utc
            )
            VALUES (
                %s, 'TEST', 'PAYMENTS', '2026-09-14',
                %s, %s, %s, '1', 1, 1,
                'COMMITTED', CURRENT_TIMESTAMP
            )
            RETURNING batch_id
            """,
            (
                label * 64,
                f"{label}.json",
                "a" * 64,
                "b" * 64,
            ),
        ).fetchone()

        receipt = connection.execute(
            """
            INSERT INTO source_receipt (
                batch_id,
                row_number,
                entity_type,
                source_id,
                source_version,
                payload_hash,
                raw_payload,
                disposition
            )
            VALUES (
                %s, 1, 'PAYMENT', %s, 1, %s,
                '{}'::jsonb, 'ACCEPTED'
            )
            RETURNING receipt_id
            """,
            (
                batch["batch_id"],
                f"PAY-{label}",
                "c" * 64,
            ),
        ).fetchone()

        return enqueue_full_rebuild(
            connection,
            trigger_receipt_id=receipt["receipt_id"],
            canonical_changed=True,
            affects_published_history=True,
            change_key=f"test-change-{label}",
            reason_code="LATE_NEW_FACT",
            as_of_utc=AS_OF,
        )


def empty_candidate(connection, run_id, as_of_utc):
    # Isolates publication. This intentionally bypasses the rebuild adapter.
    return {
        "run_id": run_id,
        "contract_version": "test-v1",
        "detected_at_utc": as_of_utc,
        "transactions": [],
        "expected_movements": [],
        "payouts": [],
        "exceptions": [],
        "source_completeness": [],
        "quarantine_records": [],
    }


def worker(database_url, *, failure_hook=None):
    return FullRebuildRecovery(
        database_url=database_url,
        build_candidate=empty_candidate,
        # Domain reconciliation is covered by separate golden-scenario tests.
        validate_domain=lambda candidate: None,
        failure_hook=failure_hook,
    )


def published(database_url):
    with psycopg.connect(database_url) as connection:
        return read_published_result(connection)


def test_success_publishes_and_completes_request(database_url):
    request_id = enqueue_test_request(database_url, label="a")

    outcome = worker(database_url).run_next()

    assert outcome is not None
    assert outcome.request_id == request_id
    assert published(database_url)["current_run_id"] == outcome.run_id
    
    stored_publication = published(database_url)

    assert verify_stored_financial_result(stored_publication) == (
        outcome.logical_fingerprint
    )

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        request = connection.execute(
            """
            SELECT status, attempt_count, result_run_id
            FROM recovery_request
            WHERE request_id = %s
            """,
            (request_id,),
        ).fetchone()

    assert request["status"] == "SUCCEEDED"
    assert request["attempt_count"] == 1
    assert request["result_run_id"] == outcome.run_id


def test_failure_before_commit_preserves_previous_publication(database_url):
    enqueue_test_request(database_url, label="a")
    worker(database_url).run_next()
    previous = published(database_url)

    request_id = enqueue_test_request(database_url, label="b")

    def fail(point):
        if point == "before_commit":
            raise RuntimeError("Injected publication failure")

    with pytest.raises(RuntimeError, match="Injected"):
        worker(database_url, failure_hook=fail).run_next()

    current = published(database_url)
    assert current == previous

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        request = connection.execute(
            """
            SELECT status, attempt_count, last_error
            FROM recovery_request
            WHERE request_id = %s
            """,
            (request_id,),
        ).fetchone()

        run_count = connection.execute(
            "SELECT COUNT(*) AS count FROM reconciliation_run"
        ).fetchone()["count"]

        batch_count = connection.execute(
            "SELECT COUNT(*) AS count FROM ingestion_batch"
        ).fetchone()["count"]

    assert request["status"] == "PENDING"
    assert request["attempt_count"] == 1
    assert "Injected publication failure" in request["last_error"]

    # Candidate run rolled back; previously committed source evidence survived.
    assert run_count == 1
    assert batch_count == 2


def test_failed_request_can_be_retried(database_url):
    request_id = enqueue_test_request(database_url, label="a")

    def fail(point):
        if point == "before_pointer_update":
            raise RuntimeError("Injected failure")

    with pytest.raises(RuntimeError):
        worker(database_url, failure_hook=fail).run_next()
    
    # Make the scheduled retry eligible without waiting 30 seconds.
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            UPDATE recovery_request
            SET next_attempt_at_utc =
                clock_timestamp() - interval '1 second'
            WHERE request_id = %s
            """,
            (request_id,),
        )

    outcome = worker(database_url).run_next()

    assert outcome is not None
    assert outcome.request_id == request_id
    assert published(database_url)["current_run_id"] == outcome.run_id

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        request = connection.execute(
            """
            SELECT status, attempt_count, last_error
            FROM recovery_request
            WHERE request_id = %s
            """,
            (request_id,),
        ).fetchone()

    assert request["status"] == "SUCCEEDED"
    assert request["attempt_count"] == 2
    assert request["last_error"] is None


def test_different_runs_have_equal_logical_fingerprints(database_url):
    enqueue_test_request(database_url, label="a")
    first = worker(database_url).run_next()

    enqueue_test_request(database_url, label="b")
    second = worker(database_url).run_next()

    assert first.run_id != second.run_id
    assert first.logical_fingerprint == second.logical_fingerprint
    assert first.input_fingerprint != second.input_fingerprint


def test_completed_request_is_not_reprocessed(database_url):
    enqueue_test_request(database_url, label="a")
    assert worker(database_url).run_next() is not None
    assert worker(database_url).run_next() is None
    
    
    
PRE_COMMIT_FAILURE_POINTS = tuple(
    point
    for point in FailurePoint
    if point != FailurePoint.AFTER_COMMIT
)


def _failure_request(database_url, request_id):
    with psycopg.connect(
        database_url,
        row_factory=dict_row,
    ) as connection:
        return connection.execute(
            """
            SELECT
                status,
                attempt_count,
                last_error,
                result_run_id,
                completed_at_utc
            FROM recovery_request
            WHERE request_id = %s
            """,
            (request_id,),
        ).fetchone()


def _make_retry_eligible(database_url, request_id):
    # Test-only time advancement. Production backoff is unchanged.
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            UPDATE recovery_request
            SET next_attempt_at_utc =
                clock_timestamp() - interval '1 second'
            WHERE request_id = %s
            """,
            (request_id,),
        )


@pytest.mark.parametrize(
    "point",
    PRE_COMMIT_FAILURE_POINTS,
    ids=lambda point: point.value,
)
def test_controlled_failure_rolls_back_and_recovers(
    database_url,
    point,
):
    # Publish a baseline that the failed candidate must preserve.
    enqueue_test_request(database_url, label="a")
    baseline_outcome = worker(database_url).run_next()

    assert baseline_outcome is not None
    previous_publication = published(database_url)

    # The new source evidence commits before recovery begins.
    request_id = enqueue_test_request(database_url, label="b")
    injector = FailOnce(point)

    with pytest.raises(InjectedFailure, match=point.value):
        worker(
            database_url,
            failure_hook=injector,
        ).run_next()

    assert injector.fired is True

    # Includes the previous run ID, financial hash, and complete payload.
    assert published(database_url) == previous_publication

    request = _failure_request(database_url, request_id)

    assert request["status"] == "PENDING"
    assert request["attempt_count"] == 1
    assert request["result_run_id"] is None
    assert request["completed_at_utc"] is None
    assert point.value in request["last_error"]
    
    observation = inspect_recovery(database_url, request_id)

    assert observation.request_status == "PENDING"
    assert observation.result_run_id is None
    assert observation.action in {
        RecoveryAction.RETRY_NOW,
        RecoveryAction.WAIT_FOR_RETRY,
    }

    with psycopg.connect(database_url) as connection:
        run_count = connection.execute(
            "SELECT COUNT(*) FROM reconciliation_run"
        ).fetchone()[0]

        artifact_count = connection.execute(
            "SELECT COUNT(*) FROM recovery_result"
        ).fetchone()[0]

        committed_batches = connection.execute(
            """
            SELECT COUNT(*)
            FROM ingestion_batch
            WHERE ingestion_status = 'COMMITTED'
            """
        ).fetchone()[0]

        receipt_count = connection.execute(
            "SELECT COUNT(*) FROM source_receipt"
        ).fetchone()[0]

        attempts = connection.execute(
            """
            SELECT attempt_number, outcome
            FROM recovery_attempt
            WHERE request_id = %s
            ORDER BY attempt_number
            """,
            (request_id,),
        ).fetchall()

    # Every provisional candidate write rolled back.
    assert run_count == 1
    assert artifact_count == 1

    # Both previously committed source deliveries survived.
    assert committed_batches == 2
    assert receipt_count == 2

    # Any provisional success history rolled back too.
    assert attempts == [(1, "FAILED")]

    _make_retry_eligible(database_url, request_id)

    # Reuse the same injector. It has already fired once.
    retry_outcome = worker(
        database_url,
        failure_hook=injector,
    ).run_next()

    assert retry_outcome is not None
    assert retry_outcome.request_id == request_id

    request = _failure_request(database_url, request_id)

    assert request["status"] == "SUCCEEDED"
    assert request["attempt_count"] == 2
    assert request["last_error"] is None
    assert request["result_run_id"] == retry_outcome.run_id

    assert published(database_url)["current_run_id"] == (
        retry_outcome.run_id
    )

    with psycopg.connect(database_url) as connection:
        attempts = connection.execute(
            """
            SELECT attempt_number, outcome
            FROM recovery_attempt
            WHERE request_id = %s
            ORDER BY attempt_number
            """,
            (request_id,),
        ).fetchall()

    assert attempts == [
        (1, "FAILED"),
        (2, "SUCCEEDED"),
    ]

    assert sum(
        visit.injected
        for visit in injector.visits
    ) == 1


def test_failure_after_commit_does_not_repeat_publication(database_url):
    enqueue_test_request(database_url, label="a")
    assert worker(database_url).run_next() is not None

    previous_publication = published(database_url)
    request_id = enqueue_test_request(database_url, label="b")

    injector = FailOnce(FailurePoint.AFTER_COMMIT)

    # Simulates an application failure after commit but before
    # the caller receives the successful return value.
    with pytest.raises(InjectedFailure, match="after_commit"):
        worker(
            database_url,
            failure_hook=injector,
        ).run_next()

    current_publication = published(database_url)
    request = _failure_request(database_url, request_id)

    # The new result really committed.
    assert current_publication["current_run_id"] != (
        previous_publication["current_run_id"]
    )
    assert request["result_run_id"] == (
        current_publication["current_run_id"]
    )

    assert request["status"] == "SUCCEEDED"
    assert request["attempt_count"] == 1
    assert request["last_error"] is None

    
    observation = inspect_recovery(database_url, request_id)

    assert observation.action == RecoveryAction.DO_NOT_REPEAT
    assert observation.result_run_id == (
        current_publication["current_run_id"]
    )
    
    with psycopg.connect(database_url) as connection:
        attempts = connection.execute(
            """
            SELECT attempt_number, outcome
            FROM recovery_attempt
            WHERE request_id = %s
            ORDER BY attempt_number
            """,
            (request_id,),
        ).fetchall()

        run_count = connection.execute(
            "SELECT COUNT(*) FROM reconciliation_run"
        ).fetchone()[0]

    assert attempts == [(1, "SUCCEEDED")]
    assert run_count == 2

    # There are no remaining requests. The completed request is skipped.
    assert worker(
        database_url,
        failure_hook=injector,
    ).run_next() is None

    assert published(database_url) == current_publication
    assert _failure_request(
        database_url,
        request_id,
    )["attempt_count"] == 1
    
    

def test_recovery_behavior_waits_for_future_retry(database_url):
    request_id = enqueue_test_request(database_url, label="a")

    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            UPDATE recovery_request
            SET next_attempt_at_utc =
                clock_timestamp() + interval '1 day'
            WHERE request_id = %s
            """,
            (request_id,),
        )

    observation = inspect_recovery(database_url, request_id)

    assert observation.request_status == "PENDING"
    assert observation.action == RecoveryAction.WAIT_FOR_RETRY
    
    assert worker(database_url).run_next() is None
    assert _failure_request(database_url, request_id)["status"] == "PENDING"


def test_recovery_behavior_requires_manual_requeue_at_limit(database_url):
    request_id = enqueue_test_request(database_url, label="a")

    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            UPDATE recovery_request
            SET max_attempts = 1
            WHERE request_id = %s
            """,
            (request_id,),
        )

    injector = FailOnce(FailurePoint.BEFORE_COMMIT)

    with pytest.raises(InjectedFailure):
        worker(database_url, failure_hook=injector).run_next()

    observation = inspect_recovery(database_url, request_id)

    assert observation.request_status == "FAILED"
    assert observation.attempt_count == 1
    assert observation.action == RecoveryAction.MANUAL_REQUEUE
    assert observation.result_run_id is None


def test_older_success_is_not_repeated_after_pointer_advances(database_url):
    first_request = enqueue_test_request(database_url, label="a")
    first_outcome = worker(database_url).run_next()
    assert first_outcome is not None

    enqueue_test_request(database_url, label="b")
    second_outcome = worker(database_url).run_next()
    assert second_outcome is not None

    assert published(database_url)["current_run_id"] == (
        second_outcome.run_id
    )

    observation = inspect_recovery(database_url, first_request)

    assert observation.action == RecoveryAction.DO_NOT_REPEAT
    assert observation.result_run_id == first_outcome.run_id


def test_missing_request_requires_investigation(database_url):
    observation = inspect_recovery(database_url, uuid4())

    assert observation.request_status is None
    assert observation.action == (
        RecoveryAction.INVESTIGATE_MISSING_REQUEST
    )


def test_unavailable_database_does_not_assume_rollback(
    database_url,
    monkeypatch,
):
    def unavailable(*args, **kwargs):
        raise psycopg.OperationalError("Simulated connection failure")

    monkeypatch.setattr(
        "threadline.recovery_behavior.psycopg.connect",
        unavailable,
    )

    observation = inspect_recovery(database_url, uuid4())

    assert observation.request_status is None
    assert observation.action == RecoveryAction.CHECK_AGAIN


def test_corrupted_committed_artifact_requires_investigation(database_url):
    request_id = enqueue_test_request(database_url, label="a")
    outcome = worker(database_url).run_next()
    assert outcome is not None

    # Deliberately corrupt test data without updating its stored hash.
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            UPDATE recovery_result
            SET result_payload = jsonb_set(
                result_payload,
                '{contract_version}',
                '"tampered"'::jsonb
            )
            WHERE run_id = %s
            """,
            (outcome.run_id,),
        )

    with pytest.raises(
        RecoveryStateError,
        match="failed verification",
    ):
        inspect_recovery(database_url, request_id)
        

def test_rejected_candidate_preserves_previous_publication(database_url):
    enqueue_test_request(database_url, label="a")
    assert worker(database_url).run_next() is not None
    previous = published(database_url)

    request_id = enqueue_test_request(database_url, label="b")

    def reject_candidate(candidate):
        raise ValueError("Candidate violates publication invariants")

    rejecting_worker = FullRebuildRecovery(
        database_url=database_url,
        build_candidate=empty_candidate,
        validate_domain=reject_candidate,
    )

    with pytest.raises(ValueError, match="publication invariants"):
        rejecting_worker.run_next()

    assert published(database_url) == previous

    request = _failure_request(database_url, request_id)
    assert request["status"] == "PENDING"
    assert request["attempt_count"] == 1
    assert request["result_run_id"] is None

    with psycopg.connect(database_url) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM reconciliation_run"
        ).fetchone()[0] == 1

        assert connection.execute(
            "SELECT COUNT(*) FROM recovery_result"
        ).fetchone()[0] == 1
        

ARCHIVE_FAILURE_POINTS = (
    "after_copy",
    "after_temp_fsync",
    "after_temp_checksum",
    "after_rename",
    "after_final_checksum",
    "after_source_remove",
    "before_status_update",
)


class ArchiveFailOnce:
    def __init__(self, target):
        assert target in ARCHIVE_FAILURE_POINTS
        self.target = target
        self.fired = False

    def __call__(self, point):
        if point == self.target and not self.fired:
            self.fired = True
            raise RuntimeError(f"Archive injected failure: {point}")


def _archive_case(
    database_url,
    tmp_path,
    *,
    filename="payments-a.json",
    failure_hook=None,
):
    inbox = tmp_path / "inbox"
    archive = tmp_path / "archive"
    inbox.mkdir(exist_ok=True)

    payment = {
        "payment_id": "PAY-ARCH-001",
        "order_id": "ORD-ARCH-001",
        "attempt_number": 1,
        "payment_method": "CARD",
        "status": "CAPTURED",
        "amount": "100.00",
        "currency": "EUR",
        "effective_at_utc": "2026-09-15T08:01:00Z",
        "available_on": "2026-09-15",
        "source_version": 1,
    }

    content = json.dumps(
        [payment],
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    checksum = hashlib.sha256(content).hexdigest()
    source = inbox / filename
    source.write_bytes(content)

    payload_checksum = hashlib.sha256(
        json.dumps(
            payment,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    with psycopg.connect(
        database_url,
        row_factory=dict_row,
    ) as connection:
        lock_financial_state(connection)

        batch = connection.execute(
            """
            INSERT INTO ingestion_batch (
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
                committed_at_utc,
                source_relative_path
            )
            VALUES (
                %s, 'ARCHIVE_TEST', 'PAYMENTS', '2026-09-15',
                %s, %s, %s, '1', 1, 1,
                'COMMITTED', CURRENT_TIMESTAMP, %s
            )
            RETURNING batch_id
            """,
            (
                hashlib.sha256(filename.encode()).hexdigest(),
                filename,
                checksum,
                "b" * 64,
                filename,
            ),
        ).fetchone()

        connection.execute(
            """
            INSERT INTO source_receipt (
                batch_id,
                row_number,
                entity_type,
                source_id,
                source_version,
                payload_hash,
                raw_payload,
                disposition
            )
            VALUES (
                %s, 1, 'PAYMENT', 'PAY-ARCH-001', 1,
                %s, %s, 'ACCEPTED'
            )
            """,
            (
                batch["batch_id"],
                payload_checksum,
                Jsonb(payment),
            ),
        )

    service = ArchiveService(
        database_url=database_url,
        inbox_root=inbox,
        archive_root=archive,
        failure_hook=failure_hook,
    )

    destination = archive / archive_relative_path(
        "PAYMENTS",
        AS_OF.date(),
        checksum,
    )

    return service, batch["batch_id"], source, destination, checksum


def _archive_batch_row(database_url, batch_id):
    with psycopg.connect(
        database_url,
        row_factory=dict_row,
    ) as connection:
        return connection.execute(
            """
            SELECT *
            FROM ingestion_batch
            WHERE batch_id = %s
            """,
            (batch_id,),
        ).fetchone()


def test_archival_success_and_repeated_delivery(database_url, tmp_path):
    service, batch_id, source, destination, checksum = _archive_case(
        database_url,
        tmp_path,
    )

    content = source.read_bytes()
    first = service.archive_batch(batch_id)

    assert first.reused_existing_object is False
    assert destination.read_bytes() == content
    assert sha256_file(destination) == checksum
    assert not source.exists()

    batch = _archive_batch_row(database_url, batch_id)

    assert batch["ingestion_status"] == "COMMITTED"
    assert batch["archive_status"] == "ARCHIVED"
    assert batch["archived_at_utc"] is not None

    # Redelivery of the same bytes can be cleaned up using the same object.
    source.write_bytes(content)
    second = service.archive_batch(batch_id)

    assert second.reused_existing_object is True
    assert destination.read_bytes() == content
    assert not source.exists()


@pytest.mark.parametrize("point", ARCHIVE_FAILURE_POINTS)
def test_archival_failure_preserves_evidence_and_retries(
    database_url,
    tmp_path,
    point,
):
    injector = ArchiveFailOnce(point)

    service, batch_id, source, destination, checksum = _archive_case(
        database_url,
        tmp_path,
        failure_hook=injector,
    )
    content = source.read_bytes()

    with pytest.raises(RuntimeError, match=point):
        service.archive_batch(batch_id)

    assert injector.fired
    assert source.exists()
    assert source.read_bytes() == content

    batch = _archive_batch_row(database_url, batch_id)

    assert batch["ingestion_status"] == "COMMITTED"
    assert batch["archive_status"] == "FAILED"
    assert batch["archive_attempt_count"] == 1
    assert point in batch["archive_error_message"]
    assert batch["file_checksum"] == checksum

    with psycopg.connect(database_url) as connection:
        receipt_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM source_receipt
            WHERE batch_id = %s
            """,
            (batch_id,),
        ).fetchone()[0]

    assert receipt_count == 1

    # Same injector has already fired. Retry uses the persisted source path.
    service.archive_batch(batch_id)

    assert sha256_file(destination) == checksum
    assert not source.exists()

    batch = _archive_batch_row(database_url, batch_id)

    assert batch["archive_status"] == "ARCHIVED"
    assert batch["archive_attempt_count"] == 2
    assert batch["archive_error_message"] is None


@pytest.mark.parametrize(
    "report_type",
    [
        ReportType.PAYMENTS,
        ReportType.PAYMENTS.name,
        ReportType.PAYMENTS.value,
    ],
)
def test_archive_path_accepts_enum_name_and_value(report_type):
    from datetime import date
    from pathlib import Path

    checksum = "ab" + "0" * 62

    actual = archive_relative_path(
        report_type,
        date(2026, 9, 15),
        checksum,
    )

    expected = (
        Path("PAYMENTS")
        / "2026-09-15"
        / "ab"
        / f"{checksum}.json"
    )

    assert actual == expected

def test_mismatching_archive_object_is_not_overwritten(
    database_url,
    tmp_path,
):
    service, batch_id, source, destination, checksum = _archive_case(
        database_url,
        tmp_path,
    )
    original = source.read_bytes()

    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"CORRUPTED ARCHIVE")

    with pytest.raises(ArchiveIntegrityError):
        service.archive_batch(batch_id)

    assert source.read_bytes() == original
    assert destination.read_bytes() == b"CORRUPTED ARCHIVE"

    batch = _archive_batch_row(database_url, batch_id)
    assert batch["ingestion_status"] == "COMMITTED"
    assert batch["archive_status"] == "FAILED"


def test_retry_can_finish_when_source_is_already_missing(
    database_url,
    tmp_path,
):
    service, batch_id, source, destination, checksum = _archive_case(
        database_url,
        tmp_path,
    )

    # Arrange the durable filesystem state left by a crash after deletion.
    destination.parent.mkdir(parents=True)
    destination.write_bytes(source.read_bytes())
    source.unlink()

    outcome = service.archive_batch(batch_id)

    assert outcome.reused_existing_object is True
    assert sha256_file(destination) == checksum

    batch = _archive_batch_row(database_url, batch_id)
    assert batch["archive_status"] == "ARCHIVED"


def test_different_filenames_reuse_the_same_content_object(
    database_url,
    tmp_path,
):
    first = _archive_case(
        database_url,
        tmp_path,
        filename="payments-a.json",
    )
    second = _archive_case(
        database_url,
        tmp_path,
        filename="payments-b.json",
    )

    first_service, first_id, first_source, first_destination, checksum = first
    second_service, second_id, second_source, second_destination, _ = second

    assert first_destination == second_destination

    first_service.archive_batch(first_id)
    outcome = second_service.archive_batch(second_id)

    assert outcome.reused_existing_object is True
    assert sha256_file(first_destination) == checksum
    assert not first_source.exists()
    assert not second_source.exists()
    

