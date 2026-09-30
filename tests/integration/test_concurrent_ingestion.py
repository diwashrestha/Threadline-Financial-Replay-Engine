"""Concurrent same-entity ingestion through production transactions."""

from __future__ import annotations

import time

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from threading import Event

import pytest

from threadline.contracts import ReportType
from threadline.full_rebuild_recovery import FINANCIAL_STATE_LOCK

from tests.integration.conftest import reset_scenario_database
from tests.integration.recovery_scenarios import (
    Scenario,
    baseline_reports,
    payment,
)


pytestmark = pytest.mark.integration


def prepare_scenario(database_url, directory):
    reset_scenario_database(
        database_url,
        expected_name="threadline_test",
    )

    scenario = Scenario(database_url, directory)

    # Complete all supporting reports, but let the concurrent workers
    # provide the two payment variants.
    for report_type, records in baseline_reports().items():
        if report_type is not ReportType.PAYMENTS:
            scenario.deliver(report_type, records)

    return scenario


def wait_for_financial_lock_waiter(scenario):
    """Observe actual PostgreSQL contention, rather than assuming it."""

    deadline = time.monotonic() + 10

    while time.monotonic() < deadline:
        waiting = scenario.scalar(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_locks
                WHERE locktype = 'advisory'
                  AND database = (
                      SELECT oid
                      FROM pg_database
                      WHERE datname = current_database()
                  )
                  AND classid = 0::oid
                  AND objid = %s::oid
                  AND objsubid = 1
                  AND NOT granted
            )
            """,
            (FINANCIAL_STATE_LOCK,),
        )

        if waiting:
            return

        time.sleep(0.05)

    raise AssertionError(
        "The second ingestion worker never waited "
        "for the financial lock"
    )


def ingest_in_commit_order(
    scenario,
    *,
    payload_a,
    payload_b,
    first_worker,
):
    paths = {
        "A": scenario.make_file(
            ReportType.PAYMENTS,
            [payload_a],
        ),
        "B": scenario.make_file(
            ReportType.PAYMENTS,
            [payload_b],
        ),
    }

    second_worker = "B" if first_worker == "A" else "A"

    first_resolved = Event()
    release_first = Event()
    first_commit_acknowledged = Event()
    second_started = Event()

    commit_order = []

    def first_hook(point):
        if point == "after_entity_resolution":
            first_resolved.set()

            if not release_first.wait(timeout=20):
                raise AssertionError(
                    "Timed out waiting to release first worker"
                )

        if point == "after_ingestion_commit":
            commit_order.append(first_worker)
            first_commit_acknowledged.set()

    def second_hook(point):
        if point == "after_entity_resolution":
            # The first database commit has already released its lock.
            # Waiting for its acknowledgement also makes the recorded
            # hook order unambiguous.
            if not first_commit_acknowledged.wait(timeout=10):
                raise AssertionError(
                    "First worker did not acknowledge its commit"
                )

        if point == "after_ingestion_commit":
            commit_order.append(second_worker)

    def run_first():
        return scenario.ingest(
            paths[first_worker],
            failure_hook=first_hook,
        )

    def run_second():
        second_started.set()

        return scenario.ingest(
            paths[second_worker],
            failure_hook=second_hook,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(run_first)

        try:
            assert first_resolved.wait(timeout=10)

            second_future = executor.submit(run_second)

            assert second_started.wait(timeout=10)

            wait_for_financial_lock_waiter(scenario)

            # First worker has resolved the entity inside its transaction,
            # but none of that payment history is externally visible yet.
            assert scenario.scalar(
                """
                SELECT COUNT(*)
                FROM source_record_version
                WHERE entity_type = 'PAYMENT'
                  AND source_id = 'PAY-001'
                """
            ) == 0

            assert scenario.scalar(
                """
                SELECT COUNT(*)
                FROM ingestion_batch
                WHERE original_filename = %s
                """,
                (paths[second_worker].name,),
            ) == 0

        finally:
            release_first.set()

        first_outcome = first_future.result(timeout=30)
        second_outcome = second_future.result(timeout=30)

    assert first_outcome is not None
    assert second_outcome is not None

    assert commit_order == [
        first_worker,
        second_worker,
    ]

    assert first_outcome.batch_id != second_outcome.batch_id

    # Register fixture inputs independently of database reconstruction.
    scenario.remember_inputs(
        ReportType.PAYMENTS,
        [payload_a, payload_b],
    )

    return {
        first_worker: first_outcome,
        second_worker: second_outcome,
    }


def test_payment_versions_are_safe_in_both_commit_orders(
    scenario_database_url,
    tmp_path,
):
    fingerprints = []

    for first_worker in ("A", "B"):
        scenario = prepare_scenario(
            scenario_database_url,
            tmp_path / f"versions-{first_worker}-first",
        )

        ingest_in_commit_order(
            scenario,
            payload_a=payment(
                amount="100.00",
                version=1,
            ),
            payload_b=payment(
                amount="120.00",
                version=2,
            ),
            first_worker=first_worker,
        )

        history = scenario.rows(
            """
            SELECT source_version, payload_hash
            FROM source_record_version
            WHERE entity_type = 'PAYMENT'
              AND source_id = 'PAY-001'
            ORDER BY source_version
            """
        )

        assert len(history) == 2
        assert [
            row["source_version"]
            for row in history
        ] == [1, 2]

        resolution = scenario.rows(
            """
            SELECT
                winning_version,
                resolution_state,
                selected_payload_hash
            FROM entity_resolution
            WHERE entity_type = 'PAYMENT'
              AND source_id = 'PAY-001'
            """
        )[0]

        assert resolution["winning_version"] == 2
        assert resolution["resolution_state"] == "ACCEPTED"

        assert (
            resolution["selected_payload_hash"]
            == history[1]["payload_hash"]
        )

        receipts = scenario.rows(
            """
            SELECT source_version, disposition
            FROM source_receipt
            WHERE entity_type = 'PAYMENT'
              AND source_id = 'PAY-001'
            ORDER BY source_version
            """
        )

        assert [
            (row["source_version"], row["disposition"])
            for row in receipts
        ] == [
            (1, "STALE"),
            (2, "ACCEPTED"),
        ]

        scenario.recover()

        assert Decimal(
            scenario.transaction()["captured_total"]
        ) == Decimal("120.00")

        scenario.assert_oracle()

        fingerprints.append(
            scenario.published()["logical_fingerprint"]
        )

    assert len(set(fingerprints)) == 1


def test_same_version_conflict_is_safe_in_both_commit_orders(
    scenario_database_url,
    tmp_path,
):
    fingerprints = []

    for first_worker in ("A", "B"):
        scenario = prepare_scenario(
            scenario_database_url,
            tmp_path / f"conflict-{first_worker}-first",
        )

        deliveries = ingest_in_commit_order(
            scenario,
            payload_a=payment(
                amount="100.00",
                version=2,
            ),
            payload_b=payment(
                amount="120.00",
                version=2,
            ),
            first_worker=first_worker,
        )

        history = scenario.rows(
            """
            SELECT source_version, payload_hash, canonical_payload
            FROM source_record_version
            WHERE entity_type = 'PAYMENT'
              AND source_id = 'PAY-001'
            """
        )

        assert len(history) == 2
        assert all(
            row["source_version"] == 2
            for row in history
        )
        assert len({
            row["payload_hash"]
            for row in history
        }) == 2

        assert {
            Decimal(row["canonical_payload"]["amount"])
            for row in history
        } == {
            Decimal("100.00"),
            Decimal("120.00"),
        }

        resolutions = scenario.rows(
            """
            SELECT *
            FROM entity_resolution
            WHERE entity_type = 'PAYMENT'
              AND source_id = 'PAY-001'
            """
        )

        assert len(resolutions) == 1

        resolution = resolutions[0]

        assert resolution["winning_version"] == 2
        assert resolution["resolution_state"] == "CONFLICTED"
        assert resolution["selected_payload_hash"] is None

        receipts = scenario.rows(
            """
            SELECT disposition, record_version_id
            FROM source_receipt
            WHERE entity_type = 'PAYMENT'
              AND source_id = 'PAY-001'
            """
        )

        assert len(receipts) == 2
        assert all(
            row["disposition"] == "CONFLICTED"
            for row in receipts
        )
        assert all(
            row["record_version_id"] is not None
            for row in receipts
        )

        scenario.recover()

        published = scenario.published()
        transaction = scenario.transaction()

        assert Decimal(
            transaction["captured_total"]
        ) == Decimal("0.00")

        assert transaction["state"] != "RECONCILED"

        conflicts = [
            exception
            for exception in published["result_payload"]["exceptions"]
            if exception["exception_type"]
            == "CONFLICTING_SOURCE_VERSION"
            and exception["entity_type"] == "PAYMENT"
            and exception["entity_id"] == "PAY-001"
        ]

        assert len(conflicts) == 1

        references = conflicts[0]["supporting_source_record_ids"]

        assert len(references) == 2
        assert all(
            reference.startswith("source-variant:")
            for reference in references
        )

        # Both deliveries retain their own request and attempt evidence.
        # They must produce one logical financial result.
        request_ids = [
            delivery.request_id
            for delivery in deliveries.values()
        ]

        results = scenario.rows(
            """
            SELECT
                q.request_id,
                q.status,
                r.logical_fingerprint
            FROM recovery_request q
            JOIN recovery_result r
                ON r.run_id = q.result_run_id
            WHERE q.request_id = ANY(%s::uuid[])
            """,
            (request_ids,),
        )

        assert len(results) == 2
        assert all(
            row["status"] == "SUCCEEDED"
            for row in results
        )
        assert len({
            row["logical_fingerprint"]
            for row in results
        }) == 1

        scenario.assert_oracle()

        fingerprints.append(
            published["logical_fingerprint"]
        )

    # A-first and B-first must agree financially.
    assert len(set(fingerprints)) == 1