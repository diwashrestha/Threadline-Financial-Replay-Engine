import json

from datetime import date, datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from threadline.completeness import CompletenessIssueCode
from threadline.contracts import (
    EntityType,
    ReportType,
    SourceSystem,
    parse_source_record,
)
from threadline.recovery_adapters import (
    enum_member,
    load_completeness,
    make_envelope,
    make_quarantine,
)


AS_OF = datetime(
    2026, 9, 15, 12, 0,
    tzinfo=timezone.utc,
)


def connection_with_batches(batches):
    connection = MagicMock()

    cursor = (
        connection.cursor.return_value
        .__enter__.return_value
    )

    cursor.fetchall.return_value = batches

    return connection


def payment_record():
    return parse_source_record(
        EntityType.PAYMENT,
        {
            "payment_id": "PAY-ADAPTER-001",
            "order_id": "ORD-ADAPTER-001",
            "attempt_number": 1,
            "payment_method": "CARD",
            "status": "CAPTURED",
            "amount": "100.00",
            "currency": "EUR",
            "effective_at_utc": "2026-09-14T08:01:00Z",
            "available_on": "2026-09-14",
            "source_version": 1,
        },
    )


@pytest.mark.parametrize(
    "label",
    [
        ReportType.PAYMENTS,
        ReportType.PAYMENTS.name,
        ReportType.PAYMENTS.value,
    ],
)
def test_report_enum_accepts_names_and_values(label):
    assert enum_member(ReportType, label) is ReportType.PAYMENTS


def test_envelope_preserves_receipt_identity():
    receipt_id = uuid4()
    record = payment_record()

    envelope = make_envelope(
        {
            "receipt_id": receipt_id,
            "source_id": record.record_id,
            "source_version": record.source_version,
        },
        record,
    )

    assert envelope.receipt_id == str(receipt_id)
    assert envelope.record == record
    assert len(envelope.payload_hash) == 64


def test_envelope_rejects_mismatched_stored_identity():
    record = payment_record()

    with pytest.raises(ValueError, match="source_id"):
        make_envelope(
            {
                "receipt_id": uuid4(),
                "source_id": "DIFFERENT-PAYMENT",
                "source_version": record.source_version,
            },
            record,
        )


def test_quarantine_preserves_code_and_payload():
    raw_payload = {
        "payment_id": "PAY-BAD-001",
        "amount": "one hundred",
    }

    rejected = make_quarantine(
        {
            "entity_type": EntityType.PAYMENT.name,
            "source_id": "PAY-BAD-001",
            "reason_code": "INVALID_AMOUNT",
            "raw_payload": raw_payload,
        }
    )

    assert rejected.reason_code == "INVALID_AMOUNT"
    assert rejected.source_id == "PAY-BAD-001"
    assert json.loads(rejected.raw_payload_json) == raw_payload


def test_empty_database_does_not_manufacture_complete_reports():
    results = load_completeness(
        connection_with_batches([]),
        AS_OF,
    )

    assert len(results) == 6
    assert all(result.is_incomplete for result in results)


def test_batch_without_manifest_body_cannot_be_complete():
    batch = {
        "batch_id": uuid4(),
        "source_system": SourceSystem.MOCKPAY.value,
        "report_type": ReportType.PAYMENTS.value,
        "report_date": date(2026, 9, 14),
        "observed_row_count": 1,
        "file_checksum": "a" * 64,
        "received_at_utc": datetime(
            2026, 9, 15, 0, 0,
            tzinfo=timezone.utc,
        ),
    }

    results = load_completeness(
        connection_with_batches([batch]),
        AS_OF,
    )

    payment_result = next(
        result
        for result in results
        if result.status.report_type is ReportType.PAYMENTS
    )

    assert payment_result.is_incomplete

    assert CompletenessIssueCode.MANIFEST_MISSING in {
        issue.code
        for issue in payment_result.issues
    }