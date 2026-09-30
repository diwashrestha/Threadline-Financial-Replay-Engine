"""Benchmark Threadline ingestion and full-rebuild recovery.

Run from the repository root:

    ./.venv/bin/python -m benchmarks.benchmark_recovery

The script resets only a database named threadline_benchmark_test.
It uses the Scenario helper from tests/integration/recovery_scenarios.py.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import tempfile
import time

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row

from threadline.contracts import ReportType
from threadline.full_rebuild_recovery import FullRebuildRecovery
from threadline.publication import validate_for_publication
from threadline.recovery_adapters import build_candidate
from threadline.recovery_fingerprint import logical_fingerprint

from tests.integration.conftest import reset_scenario_database
from tests.integration.recovery_scenarios import (
    DAY,
    LATE_DAY,
    Scenario,
    fee,
    line,
    order,
    payment,
    payout,
    refund,
)


from datetime import date, timedelta

base_payout_date = date(2026, 9, 14)



BENCHMARK_DATABASE_NAME = "threadline_benchmark_test"
ORDER_COUNT = 1_999
PAYOUT_COUNT = 5
BASELINE_RECORD_COUNT = 10_000

# Each specification is:
# (report type, business date, source records)
DeliverySpec = tuple[ReportType, object, list[dict]]


def ensure_benchmark_database(database_url: str) -> None:
    """Refuse to reset a development or production database."""

    with psycopg.connect(
        database_url,
        autocommit=True,
        connect_timeout=3,
    ) as connection:
        actual_name = connection.execute(
            "SELECT current_database()"
        ).fetchone()[0]

        if actual_name != BENCHMARK_DATABASE_NAME:
            raise RuntimeError(
                "Benchmark database must be named "
                f"{BENCHMARK_DATABASE_NAME!r}; got {actual_name!r}"
            )

        libraries = connection.execute(
            "SHOW shared_preload_libraries"
        ).fetchone()[0]

        if "pg_stat_statements" not in libraries:
            raise RuntimeError(
                "Benchmark PostgreSQL must preload "
                "pg_stat_statements"
            )


def install_statement_counter(database_url: str) -> None:
    with psycopg.connect(
        database_url,
        autocommit=True,
    ) as connection:
        connection.execute(
            "CREATE EXTENSION IF NOT EXISTS pg_stat_statements"
        )

        connection.execute(
            "SELECT COUNT(*) FROM pg_stat_statements"
        ).fetchone()


def statement_count(database_url: str) -> int:
    """Count statements for this dedicated benchmark database.

    The telemetry query itself is excluded. Counts include SQL issued by
    Python services and PostgreSQL transaction-control statements that
    pg_stat_statements records.
    """

    with psycopg.connect(
        database_url,
        autocommit=True,
    ) as connection:
        result = connection.execute(
            """
            SELECT COALESCE(SUM(calls), 0)::bigint
            FROM pg_stat_statements
            WHERE dbid = (
                SELECT oid
                FROM pg_database
                WHERE datname = current_database()
            )
              AND query NOT ILIKE '%pg_stat_statements%'
            """
        ).fetchone()

    return int(result[0])


def measure(database_url: str, operation):
    before_statements = statement_count(database_url)
    started = time.perf_counter()

    result = operation()

    duration = time.perf_counter() - started
    used_statements = (
        statement_count(database_url) - before_statements
    )

    return result, duration, used_statements


def baseline_reports() -> list[DeliverySpec]:
    """Create exactly 10,000 consistent financial source records.

    1,999 orders
    1,999 payments
    1,999 fees
    3,998 settlement lines
        5 payouts
        0 refunds
    --------------------
   10,000 records

    The empty refund report still has a manifest.
    """

    orders = []
    payments = []
    fees = []
    settlement_lines = []
    payout_order_counts = [0] * PAYOUT_COUNT

    for number in range(ORDER_COUNT):
        order_id = f"ORD-{number:05d}"
        payment_id = f"PAY-{number:05d}"
        fee_id = f"FEE-{number:05d}"

        payout_number = number % PAYOUT_COUNT
        payout_id = f"OUT-{payout_number:02d}"
        payout_day = base_payout_date + timedelta(days=payout_number)
        payout_order_counts[payout_number] += 1

        orders.append(
            order(
                order_id=order_id,
                amount="100.00",
                day=DAY,
            )
        )

        payments.append(
            payment(
                payment_id=payment_id,
                order_id=order_id,
                amount="100.00",
                day=payout_day,
            )
        )

        fees.append(
            fee(
                fee_id=fee_id,
                payment_id=payment_id,
                amount="2.00",
                day=payout_day,
            )
        )

        settlement_lines.append(
            line(
                f"LINE-CAP-{number:05d}",
                payout_id,
                "CAPTURE",
                payment_id,
                "100.00",
            )
        )

        settlement_lines.append(
            line(
                f"LINE-FEE-{number:05d}",
                payout_id,
                "FEE",
                fee_id,
                "-2.00",
            )
        )

    payouts = []

    for payout_number, order_count in enumerate(payout_order_counts):
        total = Decimal("98.00") * order_count
        payout_day = base_payout_date + timedelta(days=payout_number)

        payouts.append(
            payout(
                payout_id=f"OUT-{payout_number:02d}",
                amount=f"{total:.2f}",
                day=payout_day,
            )
        )


    assert len({p["payout_date"] for p in payouts}) == PAYOUT_COUNT

    reports = [
        (ReportType.ORDERS, DAY, orders),
        (ReportType.PAYMENTS, DAY, payments),
        (ReportType.REFUNDS, DAY, []),
        (ReportType.FEES, DAY, fees),
        (
            ReportType.SETTLEMENT_LINES,
            DAY,
            settlement_lines,
        ),
        (ReportType.PAYOUTS, DAY, payouts),
    ]

    count = sum(
        len(records)
        for _, _, records in reports
    )

    if count != BASELINE_RECORD_COUNT:
        raise AssertionError(
            f"Expected 10,000 records, generated {count}"
        )

    return reports


def late_refund_report() -> list[DeliverySpec]:
    value = refund(amount="25.00")
    value["refund_id"] = "REF-LATE-00000"
    value["payment_id"] = "PAY-00000"

    return [
        (
            ReportType.REFUNDS,
            LATE_DAY,
            [value],
        )
    ]


def correction_report() -> list[DeliverySpec]:
    """Correct 100 of the original 10,000 records: 1%."""

    corrected = [
        payment(
            payment_id=f"PAY-{number:05d}",
            order_id=f"ORD-{number:05d}",
            amount="101.00",
            version=2,
            day=DAY,
        )
        for number in range(100)
    ]

    return [
        (
            ReportType.PAYMENTS,
            DAY,
            corrected,
        )
    ]


def conflict_report() -> list[DeliverySpec]:
    """Conflict with 10 version-2 payments: 0.1% of baseline."""

    conflicting = [
        payment(
            payment_id=f"PAY-{number:05d}",
            order_id=f"ORD-{number:05d}",
            amount="102.00",
            version=2,
            day=DAY,
        )
        for number in range(10)
    ]

    return [
        (
            ReportType.PAYMENTS,
            DAY,
            conflicting,
        )
    ]


def recover_pending(scenario: Scenario) -> dict:
    """Measure all recovery requests created by one workload."""

    database_url = scenario.database_url

    outcomes = []
    reconciliation_seconds = 0.0
    publication_seconds = 0.0
    recovery_seconds = 0.0
    database_statements = 0
    queue_delays = []

    while True:
        marks = {}

        def mark(point: str) -> None:
            marks[point] = time.perf_counter()

        worker = FullRebuildRecovery(
            database_url=database_url,
            build_candidate=build_candidate,
            validate_domain=validate_for_publication,
            failure_hook=mark,
        )

        before_statements = statement_count(database_url)
        worker_started_at = datetime.now(timezone.utc)
        started = time.perf_counter()

        outcome = worker.run_next()

        duration = time.perf_counter() - started
        used_statements = (
            statement_count(database_url) - before_statements
        )

        if outcome is None:
            break

        if (
            "after_validation" not in marks
            or "after_commit" not in marks
        ):
            raise AssertionError(
                "Recovery timing hooks did not run"
            )

        outcomes.append(outcome)
        recovery_seconds += duration
        database_statements += used_statements

        # This includes queue claim, rebuilding, validation, and
        # fingerprint calculation.
        reconciliation_seconds += (
            marks["after_validation"] - started
        )

        # This includes candidate writes and transaction commit.
        publication_seconds += (
            marks["after_commit"] - marks["after_validation"]
        )

        created_at = scenario.rows(
            """
            SELECT created_at_utc
            FROM recovery_request
            WHERE request_id = %s
            """,
            (outcome.request_id,),
        )[0]["created_at_utc"]

        queue_delays.append(
            max(
                0.0,
                (
                    worker_started_at - created_at
                ).total_seconds(),
            )
        )

    return {
        "outcomes": outcomes,
        "recovery_seconds": recovery_seconds,
        "reconciliation_seconds": reconciliation_seconds,
        "publication_seconds": publication_seconds,
        "database_statements": database_statements,
        "queue_delays": queue_delays,
    }


def candidate_fingerprint(
    database_url: str,
    builder,
    as_of_utc: datetime,
) -> str:
    """Build a complete result without changing publication state."""

    with psycopg.connect(
        database_url,
        autocommit=True,
        row_factory=dict_row,
    ) as connection:
        candidate = builder(
            connection,
            str(uuid4()),
            as_of_utc,
        )

        validate_for_publication(candidate)

        return logical_fingerprint(candidate)


def retry_counts(
    scenario: Scenario,
    request_ids,
    batch_ids,
) -> dict[str, int]:
    recovery_retries = scenario.scalar(
        """
        SELECT COALESCE(
            SUM(GREATEST(attempt_count - 1, 0)),
            0
        )::integer
        FROM recovery_request
        WHERE request_id = ANY(%s::uuid[])
        """,
        (list(request_ids),),
    )

    archive_retries = scenario.scalar(
        """
        SELECT COALESCE(
            SUM(GREATEST(archive_attempt_count - 1, 0)),
            0
        )::integer
        FROM ingestion_batch
        WHERE batch_id = ANY(%s::uuid[])
        """,
        (list(batch_ids),),
    )

    return {
        "recovery_retries": int(recovery_retries),
        "archive_retries": int(archive_retries),
    }


def run_workload(
    scenario: Scenario,
    *,
    name: str,
    reports: list[DeliverySpec],
    incremental_builder,
    previous_fingerprint: str | None = None,
) -> dict:
    database_url = scenario.database_url

    ingestion_seconds = 0.0
    archive_seconds = 0.0
    ingestion_statements = 0
    archive_statements = 0

    batch_ids = []
    request_ids = []

    record_count = sum(
        len(records)
        for _, _, records in reports
    )

    for report_type, business_date, records in reports:
        # File generation is outside the ingestion timer.
        source_path = scenario.make_file(
            report_type,
            records,
            day=business_date,
        )

        outcome, seconds, statements = measure(
            database_url,
            lambda path=source_path: scenario.ingest(path),
        )

        if outcome is None:
            raise AssertionError(
                f"Completed file was not ingested: {source_path}"
            )

        if outcome.reused_existing_batch:
            raise AssertionError(
                "Expected a new delivery under a new filename"
            )

        ingestion_seconds += seconds
        ingestion_statements += statements
        batch_ids.append(outcome.batch_id)
        request_ids.append(outcome.request_id)

        _, seconds, statements = measure(
            database_url,
            lambda batch_id=outcome.batch_id:
                scenario.archive_batch(batch_id),
        )

        archive_seconds += seconds
        archive_statements += statements

    recovery = recover_pending(scenario)

    completed_request_ids = {
        outcome.request_id
        for outcome in recovery["outcomes"]
    }

    if completed_request_ids != set(request_ids):
        raise AssertionError(
            "Recovery did not complete exactly the requests "
            "created by this workload"
        )

    published = scenario.published()
    published_fingerprint = published["logical_fingerprint"]

    full_fingerprint, full_seconds, full_statements = measure(
        database_url,
        lambda: candidate_fingerprint(
            database_url,
            build_candidate,
            scenario.as_of,
        ),
    )

    if incremental_builder is None:
        incremental_fingerprint = None
        incremental_seconds = None
        incremental_statements = 0
        incremental_matches_full = None
    else:
        (
            incremental_fingerprint,
            incremental_seconds,
            incremental_statements,
        ) = measure(
            database_url,
            lambda: candidate_fingerprint(
                database_url,
                incremental_builder,
                scenario.as_of,
            ),
        )

        incremental_matches_full = (
            incremental_fingerprint == full_fingerprint
        )

    delays = recovery["queue_delays"]
    retries = retry_counts(
        scenario,
        request_ids,
        batch_ids,
    )

    return {
        "workload": name,
        "records_ingested": record_count,
        "files_ingested": len(reports),
        "ingestion_seconds": round(ingestion_seconds, 6),
        "reconciliation_seconds": round(
            recovery["reconciliation_seconds"],
            6,
        ),
        "publication_seconds": round(
            recovery["publication_seconds"],
            6,
        ),
        "archive_seconds": round(archive_seconds, 6),
        "full_rebuild_seconds": round(full_seconds, 6),
        "incremental_seconds": (
            None
            if incremental_seconds is None
            else round(incremental_seconds, 6)
        ),
        "ingestion_rows_per_second": round(
            record_count / ingestion_seconds,
            2,
        ) if ingestion_seconds else None,
        "recovery_runs": len(recovery["outcomes"]),
        "recovery_queue_delay_seconds_average": round(
            sum(delays) / len(delays),
            6,
        ) if delays else None,
        "recovery_queue_delay_seconds_max": round(
            max(delays),
            6,
        ) if delays else None,
        "database_statements": {
            "ingestion": ingestion_statements,
            "recovery": recovery["database_statements"],
            "archive": archive_statements,
            "full_rebuild_check": full_statements,
            "incremental_check": incremental_statements,
        },
        **retries,
        "logical_fingerprint": published_fingerprint,
        "full_rebuild_fingerprint": full_fingerprint,
        "incremental_fingerprint": incremental_fingerprint,
        "correctness": {
            "published_equals_full_rebuild": (
                published_fingerprint == full_fingerprint
            ),
            "incremental_equals_full_rebuild": (
                incremental_matches_full
            ),
            "duplicate_preserved_previous_result": (
                None
                if previous_fingerprint is None
                else (
                    published_fingerprint
                    == previous_fingerprint
                )
            ),
        },
    }


def load_incremental_builder(specification: str | None):
    if specification is None:
        return None

    if specification == (
        "threadline.recovery_adapters:build_candidate"
    ):
        raise ValueError(
            "The full-rebuild builder cannot be used as "
            "the incremental builder"
        )

    module_name, separator, attribute_name = (
        specification.partition(":")
    )

    if not separator or not module_name or not attribute_name:
        raise ValueError(
            "Expected --incremental-builder module:function"
        )

    module = importlib.import_module(module_name)
    builder = getattr(module, attribute_name)

    if not callable(builder):
        raise TypeError(
            "Incremental builder must be callable"
        )

    return builder


def check_clean_baseline(scenario: Scenario) -> None:
    document = scenario.published()["result_payload"]

    if len(document["transactions"]) != ORDER_COUNT:
        raise AssertionError(
            "Baseline has the wrong transaction count"
        )

    if len(document["payouts"]) != PAYOUT_COUNT:
        raise AssertionError(
            "Baseline has the wrong payout count"
        )

    if document["exceptions"]:
        raise AssertionError(
            "The 10,000-record baseline is not clean"
        )

    if any(
        row["state"] != "RECONCILED"
        for row in document["transactions"]
    ):
        raise AssertionError(
            "A baseline transaction is not reconciled"
        )

    if any(
        row["state"] != "RECONCILED"
        for row in document["payouts"]
    ):
        raise AssertionError(
            "A baseline payout is not reconciled"
        )

    if any(
        row["state"] != "COMPLETE"
        for row in document["source_completeness"]
    ):
        raise AssertionError(
            "A baseline source report is incomplete"
        )


def main() -> int:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--output",
        default="benchmarks/recovery_results.json",
    )

    parser.add_argument(
        "--incremental-builder",
        help=(
            "module:function returning the complete publication "
            "after replacing affected-window rows"
        ),
    )

    arguments = parser.parse_args()

    database_url = os.environ.get(
        "THREADLINE_BENCHMARK_DATABASE_URL"
    )

    if not database_url:
        parser.error(
            "Set THREADLINE_BENCHMARK_DATABASE_URL"
        )

    ensure_benchmark_database(database_url)

    reset_scenario_database(
        database_url,
        expected_name=BENCHMARK_DATABASE_NAME,
    )

    install_statement_counter(database_url)

    incremental_builder = load_incremental_builder(
        arguments.incremental_builder
    )

    with tempfile.TemporaryDirectory(
        prefix="threadline-benchmark-"
    ) as directory:
        scenario = Scenario(database_url, directory)

        baseline = baseline_reports()

        rows = [
            run_workload(
                scenario,
                name="10,000 clean records",
                reports=baseline,
                incremental_builder=incremental_builder,
            )
        ]

        check_clean_baseline(scenario)

        history = list(baseline)

        scenario.as_of = datetime(
            2026,
            9,
            18,
            15,
            tzinfo=timezone.utc,
        )

        late = late_refund_report()

        rows.append(
            run_workload(
                scenario,
                name="one late refund",
                reports=late,
                incremental_builder=incremental_builder,
            )
        )

        history.extend(late)

        corrections = correction_report()

        rows.append(
            run_workload(
                scenario,
                name="1% higher-version corrections",
                reports=corrections,
                incremental_builder=incremental_builder,
            )
        )

        history.extend(corrections)

        fingerprint_before_duplicate = (
            scenario.published()["logical_fingerprint"]
        )

        # Redeliver every source file seen so far with new filenames.
        # This includes the 10,000-record baseline, late refund,
        # and 100 corrections: 10,101 duplicate records.
        rows.append(
            run_workload(
                scenario,
                name="complete duplicate redelivery",
                reports=list(history),
                incremental_builder=incremental_builder,
                previous_fingerprint=(
                    fingerprint_before_duplicate
                ),
            )
        )

        rows.append(
            run_workload(
                scenario,
                name="0.1% same-version conflicts",
                reports=conflict_report(),
                incremental_builder=incremental_builder,
            )
        )

        conflict_count = scenario.scalar(
            """
            SELECT COUNT(*)
            FROM entity_resolution
            WHERE entity_type = 'PAYMENT'
              AND resolution_state = 'CONFLICTED'
              AND selected_payload_hash IS NULL
            """
        )

        if conflict_count != 10:
            raise AssertionError(
                "Expected ten excluded payment conflicts"
            )

    full_checks_pass = all(
        row["correctness"]["published_equals_full_rebuild"]
        for row in rows
    )

    duplicate_check_pass = (
        rows[3]["correctness"][
            "duplicate_preserved_previous_result"
        ]
        is True
    )

    incremental_checks_pass = (
        incremental_builder is not None
        and all(
            row["correctness"][
                "incremental_equals_full_rebuild"
            ] is True
            for row in rows
        )
    )

    report = {
        "generated_at_utc": (
            datetime.now(timezone.utc).isoformat()
        ),
        "database": BENCHMARK_DATABASE_NAME,
        "benchmark_result": (
            "PASS"
            if (
                full_checks_pass
                and duplicate_check_pass
                and incremental_checks_pass
            )
            else "NOT_VERIFIED"
        ),
        "workloads": rows,
    }

    output = Path(arguments.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    output.write_text(
        json.dumps(
            report,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print(f"Results: {output}")
    print(f"Benchmark result: {report['benchmark_result']}")

    if not full_checks_pass or not duplicate_check_pass:
        return 1

    if incremental_builder is None:
        print(
            "Incremental comparison unavailable: "
            "no window executor was supplied."
        )
        return 2

    return 0 if incremental_checks_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())