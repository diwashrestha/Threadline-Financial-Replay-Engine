"""Shared real financial scenarios for recovery integration tests."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from threadline.archival import ArchiveService
from threadline.canonicalize import RecordEnvelope, canonicalize
from threadline.completeness import REPORT_SOURCES
from threadline.contracts import (
    EntityType,
    ReportType,
    parse_source_record,
)
from threadline.durable_completeness import load_completeness
from threadline.durable_ingestion import ingest_source_file
from threadline.full_rebuild_recovery import (
    FullRebuildRecovery,
    read_published_result,
)
from threadline.publication import validate_for_publication
from threadline.reconcile import reconcile
from threadline.recovery_adapters import build_candidate
from threadline.recovery_fingerprint import (
    assert_financial_equal,
    logical_fingerprint,
)
from threadline.source_files import publish_source_file


DAY = date(2026, 9, 14)
LATE_DAY = date(2026, 9, 17)


ENTITY_BY_REPORT = {
    ReportType.ORDERS: EntityType.ORDER,
    ReportType.PAYMENTS: EntityType.PAYMENT,
    ReportType.REFUNDS: EntityType.REFUND,
    ReportType.FEES: EntityType.FEE,
    ReportType.SETTLEMENT_LINES: EntityType.SETTLEMENT_LINE,
    ReportType.PAYOUTS: EntityType.PAYOUT,
}


def order(
    order_id: str = "ORD-001",
    amount: str = "100.00",
    day: date = DAY,
) -> dict[str, Any]:
    """Create a paid order matching the source contract."""

    return {
        "order_id": order_id,
        "created_at_utc": f"{day.isoformat()}T08:00:00Z",
        "status": "PAID",
        "currency": "EUR",
        "order_total": amount,
        "source_version": 1,
    }


def payment(
    *,
    payment_id: str = "PAY-001",
    order_id: str = "ORD-001",
    amount: str = "100.00",
    version: int = 1,
    day: date = DAY,
) -> dict[str, Any]:
    """Create a captured CARD payment."""

    return {
        "payment_id": payment_id,
        "order_id": order_id,
        "attempt_number": 1,
        "payment_method": "CARD",
        "status": "CAPTURED",
        "amount": amount,
        "currency": "EUR",
        "effective_at_utc": f"{day.isoformat()}T08:01:00Z",
        "available_on": day.isoformat(),
        "source_version": version,
    }


def fee(
    fee_id: str = "FEE-001",
    payment_id: str = "PAY-001",
    amount: str = "2.00",
    day: date = DAY,
) -> dict[str, Any]:
    """Create a processing-fee record."""

    return {
        "fee_id": fee_id,
        "payment_id": payment_id,
        "fee_type": "PROCESSING",
        "amount": amount,
        "currency": "EUR",
        "effective_at_utc": f"{day.isoformat()}T08:02:00Z",
        "available_on": day.isoformat(),
        "source_version": 1,
    }


def refund(
    amount: str = "25.00",
    version: int = 1,
) -> dict[str, Any]:
    """Create a refund effective earlier but settled on the later day."""

    return {
        "refund_id": "REF-001",
        "payment_id": "PAY-001",
        "status": "SUCCEEDED",
        "amount": amount,
        "currency": "EUR",
        "effective_at_utc": "2026-09-14T10:00:00Z",
        "available_on": LATE_DAY.isoformat(),
        "source_version": version,
    }


def line(
    line_id: str,
    payout_id: str,
    movement_type: str,
    movement_id: str,
    amount: str,
) -> dict[str, Any]:
    """Create one signed provider settlement movement."""

    return {
        "settlement_line_id": line_id,
        "payout_id": payout_id,
        "movement_type": movement_type,
        "movement_id": movement_id,
        "signed_amount": amount,
        "currency": "EUR",
        "source_version": 1,
    }


def payout(
    payout_id: str = "OUT-001",
    amount: str = "98.00",
    day: date = DAY,
) -> dict[str, Any]:
    """Create a provider payout total."""

    return {
        "payout_id": payout_id,
        "payout_date": day.isoformat(),
        "currency": "EUR",
        "reported_net_amount": amount,
        "source_version": 1,
    }


def baseline_reports() -> dict[ReportType, list[dict[str, Any]]]:
    """Return six complete reports for the initial clean publication.

    Financial expectations:
        Capture: EUR 100.00
        CARD fee: EUR 0.20 + 1.8% = EUR 2.00
        Refunds: EUR 0.00
        Payout: EUR 98.00

    The empty refunds report is intentional. Its manifest proves that
    the provider delivered a report containing zero refunds.
    """

    return {
        ReportType.ORDERS: [
            order(),
        ],
        ReportType.PAYMENTS: [
            payment(),
        ],
        ReportType.REFUNDS: [],
        ReportType.FEES: [
            fee(),
        ],
        ReportType.SETTLEMENT_LINES: [
            line(
                "LINE-CAP-001",
                "OUT-001",
                "CAPTURE",
                "PAY-001",
                "100.00",
            ),
            line(
                "LINE-FEE-001",
                "OUT-001",
                "FEE",
                "FEE-001",
                "-2.00",
            ),
        ],
        ReportType.PAYOUTS: [
            payout(),
        ],
    }


def late_reports() -> dict[ReportType, list[dict[str, Any]]]:
    """Return complete later-day reports including the late refund.

    The second order keeps the later provider payout positive:
        Capture: EUR 50.00
        CARD fee: EUR 0.20 + 1.8% = EUR 1.10
        Refund of the first payment: EUR 25.00
        Later payout: EUR 23.90

    First order lifetime net after the refund:
        EUR 100.00 - EUR 25.00 - EUR 2.00 = EUR 73.00
    """

    return {
        ReportType.ORDERS: [
            order(
                order_id="ORD-002",
                amount="50.00",
                day=LATE_DAY,
            ),
        ],
        ReportType.PAYMENTS: [
            payment(
                payment_id="PAY-002",
                order_id="ORD-002",
                amount="50.00",
                day=LATE_DAY,
            ),
        ],
        ReportType.REFUNDS: [
            refund(),
        ],
        ReportType.FEES: [
            fee(
                fee_id="FEE-002",
                payment_id="PAY-002",
                amount="1.10",
                day=LATE_DAY,
            ),
        ],
        ReportType.SETTLEMENT_LINES: [
            line(
                "LINE-CAP-002",
                "OUT-002",
                "CAPTURE",
                "PAY-002",
                "50.00",
            ),
            line(
                "LINE-FEE-002",
                "OUT-002",
                "FEE",
                "FEE-002",
                "-1.10",
            ),
            line(
                "LINE-REF-001",
                "OUT-002",
                "REFUND",
                "REF-001",
                "-25.00",
            ),
        ],
        ReportType.PAYOUTS: [
            payout(
                payout_id="OUT-002",
                amount="23.90",
                day=LATE_DAY,
            ),
        ],
    }


class Scenario:
    """Exercise production ingestion, recovery and archival services.

    The constructor creates filesystem directories only. Database reset
    and Alembic migrations belong to the integration fixtures.

    oracle_inputs retains independently parsed fixture inputs delivered
    through deliver(). Tests using make_file() and ingest() directly must
    register their inputs separately before calling assert_oracle().
    """

    def __init__(
        self,
        database_url: str,
        directory: str | Path,
    ) -> None:
        self.database_url = database_url

        self.inbox = Path(directory) / "inbox"
        self.archive = Path(directory) / "archive"

        self.inbox.mkdir(parents=True, exist_ok=True)

        self.as_of = datetime(
            2026,
            9,
            15,
            15,
            tzinfo=timezone.utc,
        )

        self.sequence = 0

        self.oracle_inputs: list[
            tuple[EntityType, dict[str, Any]]
        ] = []

    def rows(
        self,
        statement: str,
        parameters: tuple = (),
    ) -> list[dict[str, Any]]:
        """Execute a read using an independent database connection."""

        with psycopg.connect(
            self.database_url,
            autocommit=True,
            row_factory=dict_row,
            connect_timeout=3,
        ) as connection:
            return connection.execute(
                statement,
                parameters,
            ).fetchall()

    def scalar(
        self,
        statement: str,
        parameters: tuple = (),
    ) -> Any:
        """Return the sole value from a one-row, one-column query."""

        results = self.rows(statement, parameters)

        if len(results) != 1 or len(results[0]) != 1:
            raise AssertionError(
                "scalar() requires exactly one row and one column"
            )

        return next(iter(results[0].values()))

    def execute(
        self,
        statement: str,
        parameters: tuple = (),
    ) -> None:
        """Execute explicit test-control SQL."""

        with psycopg.connect(
            self.database_url,
            autocommit=True,
            connect_timeout=3,
        ) as connection:
            connection.execute(statement, parameters)

    def make_file(
        self,
        report_type: ReportType,
        records: list[dict[str, Any]],
        *,
        day: date = DAY,
    ) -> Path:
        """Publish a completed data file and its real manifest."""

        self.sequence += 1

        path = self.inbox / (
            f"{self.sequence:04d}-{report_type.name}.json"
        )

        publish_source_file(
            path,
            records=records,
            source_system=REPORT_SOURCES[report_type].value,
            report_type=report_type.name,
            report_date=day,
            entity_type=ENTITY_BY_REPORT[report_type].name,
            schema_version="1",
        )

        return path

    def ingest(
        self,
        path: str | Path,
        *,
        failure_hook=None,
    ):
        """Invoke the actual durable ingestion transaction."""

        return ingest_source_file(
            path,
            database_url=self.database_url,
            inbox_root=self.inbox,
            as_of_utc=self.as_of,
            failure_hook=failure_hook,
        )

    def remember_inputs(
        self,
        report_type: ReportType,
        records: list[dict[str, Any]],
    ) -> None:
        """Retain validated inputs for the independent financial oracle."""

        entity_type = ENTITY_BY_REPORT[report_type]

        for payload in records:
            # Store a normalized source-shaped copy independently of
            # PostgreSQL receipts and canonical history.
            record = parse_source_record(entity_type, payload)

            self.oracle_inputs.append(
                (
                    entity_type,
                    self._copy_payload(payload),
                )
            )

            # Parsing above validates the fixture itself.
            assert record.ENTITY_TYPE is entity_type

    @staticmethod
    def _copy_payload(value):
        """Copy JSON fixture structures without sharing mutable containers."""

        if isinstance(value, Mapping):
            return {
                key: Scenario._copy_payload(item)
                for key, item in value.items()
            }

        if isinstance(value, list):
            return [
                Scenario._copy_payload(item)
                for item in value
            ]

        return value

    def deliver(
        self,
        report_type: ReportType,
        records: list[dict[str, Any]],
        *,
        day: date = DAY,
    ):
        """Publish and ingest a valid financial fixture delivery."""

        # Validate before publishing so fixture mistakes are caught early.
        for payload in records:
            parse_source_record(
                ENTITY_BY_REPORT[report_type],
                payload,
            )

        path = self.make_file(
            report_type,
            records,
            day=day,
        )

        outcome = self.ingest(path)

        if outcome is None:
            raise AssertionError(
                "Completed fixture delivery was not ingested"
            )

        self.remember_inputs(report_type, records)

        return path, outcome

    def worker(
        self,
        *,
        failure_hook=None,
    ) -> FullRebuildRecovery:
        """Construct a worker using the real builder and validator."""

        return FullRebuildRecovery(
            database_url=self.database_url,
            build_candidate=build_candidate,
            validate_domain=validate_for_publication,
            failure_hook=failure_hook,
        )

    def recover(self):
        """Process eligible requests until the worker reports none ready.

        Future-scheduled retries remain pending. This helper does not
        override retry scheduling or retry budgets.
        """

        outcomes = []

        while True:
            outcome = self.worker().run_next()

            if outcome is None:
                return outcomes

            outcomes.append(outcome)

    def published(self) -> dict[str, Any]:
        """Read the complete result selected by the publication pointer."""

        with psycopg.connect(
            self.database_url,
            autocommit=True,
            connect_timeout=3,
        ) as connection:
            result = read_published_result(connection)

        if result is None:
            raise AssertionError(
                "No published result exists for this scenario"
            )

        return result

    def transaction(
        self,
        order_id: str = "ORD-001",
    ) -> dict[str, Any]:
        """Return an order's transaction from the current publication."""

        transactions = self.published()["result_payload"]["transactions"]

        matches = [
            transaction
            for transaction in transactions
            if transaction["order_id"] == order_id
        ]

        if len(matches) != 1:
            raise AssertionError(
                f"Expected one transaction for {order_id!r}; "
                f"found {len(matches)}"
            )

        return matches[0]

    def assert_oracle(self) -> None:
        """Compare publication with replay of independently retained inputs.

        Financial inputs do not come from PostgreSQL reconstruction.
        Completeness uses the same durable evidence loader; separate
        completeness tests are required to validate that loader itself.
        """

        envelopes = [
            RecordEnvelope(
                receipt_id=f"oracle-{index}",
                record=parse_source_record(entity_type, payload),
            )
            for index, (entity_type, payload)
            in enumerate(self.oracle_inputs)
        ]

        with psycopg.connect(
            self.database_url,
            autocommit=True,
            connect_timeout=3,
        ) as connection:
            completeness = load_completeness(
                connection,
                self.as_of,
            )

        oracle = reconcile(
            run_id="oracle",
            detected_at=self.as_of,
            canonicalization=canonicalize(envelopes),
            completeness_results=completeness,
            quarantine_records=(),
        )

        published = self.published()

        assert_financial_equal(
            published["result_payload"],
            oracle,
        )

        assert (
            published["logical_fingerprint"]
            == logical_fingerprint(oracle)
        )

    def clean_baseline(self) -> dict[str, Any]:
        """Ingest all six initial reports and verify a clean publication."""

        for report_type, records in baseline_reports().items():
            self.deliver(report_type, records)

        outcomes = self.recover()

        assert outcomes, "Baseline scheduled no eligible recovery"

        published = self.published()
        document = published["result_payload"]
        transaction = self.transaction()

        assert document["exceptions"] == []
        assert transaction["state"] == "RECONCILED"

        assert Decimal(
            transaction["expected_collection"]
        ) == Decimal("100.00")

        assert Decimal(
            transaction["captured_total"]
        ) == Decimal("100.00")

        assert Decimal(
            transaction["collection_variance"]
        ) == Decimal("0.00")

        assert Decimal(
            transaction["successful_refund_total"]
        ) == Decimal("0.00")

        assert Decimal(
            transaction["expected_fee_total"]
        ) == Decimal("2.00")

        assert Decimal(
            transaction["reported_fee_total"]
        ) == Decimal("2.00")

        assert Decimal(
            transaction["lifetime_net_collection"]
        ) == Decimal("98.00")

        assert len(document["payouts"]) == 1

        payout_result = document["payouts"][0]

        assert payout_result["payout_id"] == "OUT-001"
        assert payout_result["state"] == "RECONCILED"

        for field in (
            "expected_payout",
            "reported_line_total",
            "reported_net_amount",
        ):
            assert Decimal(
                payout_result[field]
            ) == Decimal("98.00")

        assert Decimal(
            payout_result["provider_report_variance"]
        ) == Decimal("0.00")

        assert Decimal(
            payout_result["end_to_end_payout_variance"]
        ) == Decimal("0.00")

        statuses = document["source_completeness"]

        assert {
            status["report_type"]
            for status in statuses
        } == {
            report_type.value
            for report_type in ReportType
        }

        assert all(
            status["state"] == "COMPLETE"
            for status in statuses
        )

        self.assert_oracle()

        return published

    def allow_retry_now(self, request_id) -> None:
        """Advance eligibility explicitly without changing attempt history."""

        self.execute(
            """
            UPDATE recovery_request
            SET
                next_attempt_at_utc =
                    CURRENT_TIMESTAMP - INTERVAL '1 second'
            WHERE request_id = %s
              AND status = 'PENDING'
            """,
            (request_id,),
        )

    def archive_batch(self, batch_id):
        """Invoke the real retryable archive service."""

        service = ArchiveService(
            database_url=self.database_url,
            inbox_root=self.inbox,
            archive_root=self.archive,
        )

        return service.archive_batch(batch_id)