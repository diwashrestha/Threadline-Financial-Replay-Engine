"""PostgreSQL integration tests for the ingestion ledger."""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

import psycopg
import pytest
from alembic import command
from alembic.config import Config

from threadline.contracts import EntityType, ReportType
from threadline.ingestion import (
    IngestionLedgerService,
    SourceDelivery,
)


pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture()
def migrated_database() -> str:
    database_url = os.environ.get(
        "THREADLINE_TEST_DATABASE_URL"
    )

    if not database_url:
        pytest.skip(
            "THREADLINE_TEST_DATABASE_URL is not configured"
        )

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            database_name = cursor.fetchone()[0]

    if not database_name.endswith("_test"):
        raise RuntimeError(
            "Ingestion integration tests require "
            "a database ending with '_test'"
        )

    with psycopg.connect(
        database_url,
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "DROP SCHEMA public CASCADE"
            )
            cursor.execute(
                "CREATE SCHEMA public"
            )

    configuration = Config(
        str(PROJECT_ROOT / "alembic.ini")
    )
    configuration.set_main_option(
        "script_location",
        str(PROJECT_ROOT / "migrations"),
    )
    configuration.attributes[
        "database_url"
    ] = database_url

    command.upgrade(configuration, "head")

    return database_url


def _payment(
    *,
    payment_id: str,
    order_id: str,
    amount: str = "100.00",
) -> dict[str, object]:
    return {
        "payment_id": payment_id,
        "order_id": order_id,
        "attempt_number": 1,
        "payment_method": "CARD",
        "status": "CAPTURED",
        "amount": amount,
        "currency": "EUR",
        "effective_at_utc": "2026-09-14T08:01:00Z",
        "available_on": "2026-09-14",
        "source_version": 1,
    }


def _delivery(
    *,
    filename: str = "payments-2026-09-14.json",
    records: tuple[dict[str, object], ...] | None = None,
    declared_row_count: int | None = None,
    expected_file_checksum: str | None = None,
) -> SourceDelivery:
    if records is None:
        records = (
            _payment(
                payment_id="PAY-001",
                order_id="ORD-001",
            ),
            _payment(
                payment_id="PAY-002",
                order_id="ORD-002",
                amount="50.00",
            ),
        )

    file_bytes = json.dumps(
        records,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    declared = (
        len(records)
        if declared_row_count is None
        else declared_row_count
    )

    manifest = {
        "report_type": "PAYMENTS",
        "report_date": "2026-09-14",
        "schema_version": "1",
        "row_count": declared,
    }

    manifest_bytes = json.dumps(
        manifest,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    return SourceDelivery(
        source_system="TEST_PROVIDER",
        report_type=ReportType.PAYMENTS,
        report_date=date(2026, 9, 14),
        entity_type=EntityType.PAYMENT,
        original_filename=filename,
        schema_version="1",
        declared_row_count=declared,
        records=records,
        file_bytes=file_bytes,
        manifest_bytes=manifest_bytes,
        expected_file_checksum=(
            expected_file_checksum
        ),
    )


def test_valid_delivery_records_batch_and_every_receipt(
    migrated_database: str,
):
    service = IngestionLedgerService(
        migrated_database
    )

    result = service.record_delivery(
        _delivery()
    )

    assert result.was_replay is False
    assert result.ingestion_status == "PROCESSING"
    assert result.observed_row_count == 2
    assert result.processable_receipt_count == 2
    assert result.quarantined_receipt_count == 0

    with psycopg.connect(
        migrated_database
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    original_filename,
                    declared_row_count,
                    observed_row_count,
                    ingestion_status,
                    archive_status
                FROM ingestion_batch
                WHERE batch_id = %s
                """,
                (result.batch_id,),
            )

            assert cursor.fetchone() == (
                "payments-2026-09-14.json",
                2,
                2,
                "PROCESSING",
                "PENDING",
            )

            cursor.execute(
                """
                SELECT
                    row_number,
                    source_id,
                    source_version,
                    disposition,
                    reason_code
                FROM source_receipt
                WHERE batch_id = %s
                ORDER BY row_number
                """,
                (result.batch_id,),
            )

            assert cursor.fetchall() == [
                (
                    1,
                    "PAY-001",
                    1,
                    "PENDING",
                    None,
                ),
                (
                    2,
                    "PAY-002",
                    1,
                    "PENDING",
                    None,
                ),
            ]


def test_same_delivery_is_idempotent(
    migrated_database: str,
):
    service = IngestionLedgerService(
        migrated_database
    )
    delivery = _delivery()

    first = service.record_delivery(delivery)
    second = service.record_delivery(delivery)

    assert second.was_replay is True
    assert second.batch_id == first.batch_id
    assert second.delivery_key == first.delivery_key
    assert second.observed_row_count == 2

    with psycopg.connect(
        migrated_database
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM ingestion_batch"
            )
            assert cursor.fetchone()[0] == 1

            cursor.execute(
                "SELECT COUNT(*) FROM source_receipt"
            )
            assert cursor.fetchone()[0] == 2


def test_same_content_under_different_filename_is_auditable(
    migrated_database: str,
):
    service = IngestionLedgerService(
        migrated_database
    )

    first = service.record_delivery(
        _delivery(
            filename="payments-attempt-1.json"
        )
    )
    second = service.record_delivery(
        _delivery(
            filename="payments-redelivery.json"
        )
    )

    assert first.batch_id != second.batch_id
    assert first.delivery_key != second.delivery_key
    assert first.file_checksum == second.file_checksum

    with psycopg.connect(
        migrated_database
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM ingestion_batch"
            )
            assert cursor.fetchone()[0] == 2

            cursor.execute(
                "SELECT COUNT(*) FROM source_receipt"
            )
            assert cursor.fetchone()[0] == 4

            cursor.execute(
                """
                SELECT
                    source_id,
                    COUNT(DISTINCT payload_hash)
                FROM source_receipt
                GROUP BY source_id
                ORDER BY source_id
                """
            )

            assert cursor.fetchall() == [
                ("PAY-001", 1),
                ("PAY-002", 1),
            ]


def test_malformed_record_is_quarantined(
    migrated_database: str,
):
    malformed = _payment(
        payment_id="PAY-BAD",
        order_id="ORD-BAD",
    )
    malformed["amount"] = "one hundred"

    records = (
        _payment(
            payment_id="PAY-GOOD",
            order_id="ORD-GOOD",
        ),
        malformed,
    )

    service = IngestionLedgerService(
        migrated_database
    )
    result = service.record_delivery(
        _delivery(records=records)
    )

    assert result.ingestion_status == "PROCESSING"
    assert result.processable_receipt_count == 1
    assert result.quarantined_receipt_count == 1

    with psycopg.connect(
        migrated_database
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    source_id,
                    disposition,
                    reason_code
                FROM source_receipt
                WHERE batch_id = %s
                ORDER BY row_number
                """,
                (result.batch_id,),
            )

            assert cursor.fetchall() == [
                (
                    "PAY-GOOD",
                    "PENDING",
                    None,
                ),
                (
                    "PAY-BAD",
                    "QUARANTINED",
                    "INVALID_AMOUNT",
                ),
            ]


def test_row_count_mismatch_preserves_evidence_and_fails_batch(
    migrated_database: str,
):
    service = IngestionLedgerService(
        migrated_database
    )

    result = service.record_delivery(
        _delivery(
            declared_row_count=3,
        )
    )

    assert result.ingestion_status == "FAILED"
    assert result.observed_row_count == 2
    assert result.processable_receipt_count == 0
    assert result.quarantined_receipt_count == 2

    with psycopg.connect(
        migrated_database
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT error_message
                FROM ingestion_batch
                WHERE batch_id = %s
                """,
                (result.batch_id,),
            )

            error_message = cursor.fetchone()[0]

            assert "ROW_COUNT_MISMATCH" in error_message

            cursor.execute(
                """
                SELECT DISTINCT
                    disposition,
                    reason_code
                FROM source_receipt
                WHERE batch_id = %s
                """,
                (result.batch_id,),
            )

            assert cursor.fetchall() == [
                (
                    "QUARANTINED",
                    "BATCH_VALIDATION_FAILED",
                )
            ]


def test_file_checksum_mismatch_fails_batch(
    migrated_database: str,
):
    service = IngestionLedgerService(
        migrated_database
    )

    result = service.record_delivery(
        _delivery(
            expected_file_checksum="0" * 64,
        )
    )

    assert result.ingestion_status == "FAILED"
    assert result.processable_receipt_count == 0
    assert result.quarantined_receipt_count == 2

    with psycopg.connect(
        migrated_database
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT error_message
                FROM ingestion_batch
                WHERE batch_id = %s
                """,
                (result.batch_id,),
            )

            assert (
                "FILE_CHECKSUM_MISMATCH"
                in cursor.fetchone()[0]
            )