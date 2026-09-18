"""Small real financial scenarios shared by recovery integration tests."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from threadline.archival import ArchiveService
from threadline.canonicalize import RecordEnvelope, canonicalize
from threadline.completeness import REPORT_SOURCES
from threadline.contracts import EntityType, ReportType, parse_source_record
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


def order(order_id="ORD-001", amount="100.00", day=DAY):
    return {
        "order_id": order_id,
        "created_at_utc": f"{day}T08:00:00Z",
        "status": "PAID",
        "currency": "EUR",
        "order_total": amount,
        "source_version": 1,
    }


def payment(
    *,
    payment_id="PAY-001",
    order_id="ORD-001",
    amount="100.00",
    version=1,
    day=DAY,
):
    return {
        "payment_id": payment_id,
        "order_id": order_id,
        "attempt_number": 1,
        "payment_method": "CARD",
        "status": "CAPTURED",
        "amount": amount,
        "currency": "EUR",
        "effective_at_utc": f"{day}T08:01:00Z",
        "available_on": day.isoformat(),
        "source_version": version,
    }


def fee(
    fee_id="FEE-001",
    payment_id="PAY-001",
    amount="2.00",
    day=DAY,
):
    return {
        "fee_id": fee_id,
        "payment_id": payment_id,
        "fee_type": "PROCESSING",
        "amount": amount,
        "currency": "EUR",
        "effective_at_utc": f"{day}T08:02:00Z",
        "available_on": day.isoformat(),
        "source_version": 1,
    }


def refund(amount="25.00", version=1):
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


def line(line_id, payout_id, movement_type, movement_id, amount):
    return {
        "settlement_line_id": line_id,
        "payout_id": payout_id,
        "movement_type": movement_type,
        "movement_id": movement_id,
        "signed_amount": amount,
        "currency": "EUR",
        "source_version": 1,
    }


def payout(payout_id="OUT-001", amount="98.00", day=DAY):
    return {
        "payout_id": payout_id,
        "payout_date": day.isoformat(),
        "currency": "EUR",
        "reported_net_amount": amount,
        "source_version": 1,
    }


def baseline_reports():
    return {
        ReportType.ORDERS: [order()],
        ReportType.PAYMENTS: [payment()],
        ReportType.REFUNDS: [],
        ReportType.FEES: [fee()],
        ReportType.SETTLEMENT_LINES: [
            line("LINE-CAP-001", "OUT-001", "CAPTURE", "PAY-001", "100.00"),
            line("LINE-FEE-001", "OUT-001", "FEE", "FEE-001", "-2.00"),
        ],
        ReportType.PAYOUTS: [payout()],
    }


def late_reports():
    # Another capture makes the later payout positive:
    # EUR 50 - EUR 1.10 fee - EUR 25 refund = EUR 23.90.
    return {
        ReportType.ORDERS: [
            order("ORD-002", "50.00", LATE_DAY),
        ],
        ReportType.PAYMENTS: [
            payment(
                payment_id="PAY-002",
                order_id="ORD-002",
                amount="50.00",
                day=LATE_DAY,
            ),
        ],
        ReportType.REFUNDS: [refund()],
        ReportType.FEES: [
            fee("FEE-002", "PAY-002", "1.10", LATE_DAY),
        ],
        ReportType.SETTLEMENT_LINES: [
            line("LINE-CAP-002", "OUT-002", "CAPTURE", "PAY-002", "50.00"),
            line("LINE-FEE-002", "OUT-002", "FEE", "FEE-002", "-1.10"),
            line("LINE-REF-001", "OUT-002", "REFUND", "REF-001", "-25.00"),
        ],
        ReportType.PAYOUTS: [
            payout("OUT-002", "23.90", LATE_DAY),
        ],
    }


class Scenario:
    def __init__(self, database_url, directory):
        self.database_url = database_url
        self.inbox = Path(directory) / "inbox"
        self.archive = Path(directory) / "archive"
        self.inbox.mkdir(parents=True)

        self.as_of = datetime(
            2026, 9, 15, 15, tzinfo=timezone.utc
        )

        self.sequence = 0
        self.oracle_inputs = []

    def rows(self, statement, parameters=()):
        with psycopg.connect(
            self.database_url,
            autocommit=True,
            row_factory=dict_row,
        ) as connection:
            return connection.execute(
                statement,
                parameters,
            ).fetchall()

    def scalar(self, statement, parameters=()):
        rows = self.rows(statement, parameters)
        return next(iter(rows[0].values()))

    def execute(self, statement, parameters=()):
        with psycopg.connect(
            self.database_url,
            autocommit=True,
        ) as connection:
            connection.execute(statement, parameters)

    def make_file(self, report_type, records, *, day=DAY):
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

    def ingest(self, path, *, failure_hook=None):
        return ingest_source_file(
            path,
            database_url=self.database_url,
            inbox_root=self.inbox,
            as_of_utc=self.as_of,
            failure_hook=failure_hook,
        )

    def deliver(self, report_type, records, *, day=DAY):
        path = self.make_file(report_type, records, day=day)
        outcome = self.ingest(path)
        assert outcome is not None

        self.oracle_inputs.extend(
            (ENTITY_BY_REPORT[report_type], payload)
            for payload in records
        )

        return path, outcome

    def worker(self, *, failure_hook=None):
        return FullRebuildRecovery(
            database_url=self.database_url,
            build_candidate=build_candidate,
            validate_domain=validate_for_publication,
            failure_hook=failure_hook,
        )

    def recover(self):
        outcomes = []

        while True:
            outcome = self.worker().run_next()

            if outcome is None:
                break

            outcomes.append(outcome)

        return outcomes

    def published(self):
        with psycopg.connect(
            self.database_url,
            autocommit=True,
        ) as connection:
            result = read_published_result(connection)

        assert result is not None
        return result

    def transaction(self, order_id="ORD-001"):
        return next(
            row
            for row in self.published()["result_payload"]["transactions"]
            if row["order_id"] == order_id
        )

    def assert_oracle(self):
        # Financial inputs come from the fixture's delivered records,
        # independently of PostgreSQL receipt reconstruction.
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
        assert published["logical_fingerprint"] == logical_fingerprint(
            oracle
        )

    def clean_baseline(self):
        for report_type, records in baseline_reports().items():
            self.deliver(report_type, records)

        self.recover()

        document = self.published()["result_payload"]

        assert document["exceptions"] == []
        assert self.transaction()["state"] == "RECONCILED"
        assert Decimal(self.transaction()["captured_total"]) == Decimal("100")
        assert Decimal(
            self.transaction()["successful_refund_total"]
        ) == Decimal("0")

        assert document["payouts"][0]["state"] == "RECONCILED"
        assert Decimal(
            document["payouts"][0]["expected_payout"]
        ) == Decimal("98")

        assert all(
            status["state"] == "COMPLETE"
            for status in document["source_completeness"]
        )

        return self.published()

    def allow_retry_now(self, request_id):
        self.execute(
            """
            UPDATE recovery_request
            SET next_attempt_at_utc = CURRENT_TIMESTAMP - INTERVAL '1 second'
            WHERE request_id = %s AND status = 'PENDING'
            """,
            (request_id,),
        )

    def archive_batch(self, batch_id):
        service = ArchiveService(
            database_url=self.database_url,
            inbox_root=self.inbox,
            archive_root=self.archive,
        )
        return service.archive_batch(batch_id)