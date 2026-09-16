from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from threadline.recovery_fingerprint import (
    FINANCIAL_FINGERPRINT_VERSION,
    FinancialResultMismatch,
    assert_financial_equal,
    canonical_json,
    json_value,
    logical_document,
    logical_fingerprint,
    verify_stored_financial_result,
)


def result():
    """Serialization fixture; domain reconciliation has separate tests."""

    return {
        "run_id": "RUN-A",
        "contract_version": "test-v1",
        "detected_at_utc": datetime(
            2026, 9, 15, 12, 0, tzinfo=timezone.utc
        ),
        "transactions": [
            {
                "run_id": "RUN-A",
                "order_id": "ORD-A",
                "currency": "EUR",
                "captured_total": Decimal("100.00"),
                "successful_refund_total": Decimal("0.00"),
                "expected_fee_total": Decimal("2.00"),
                "reported_fee_total": Decimal("2.00"),
                "lifetime_net_collection": Decimal("98.00"),
                "state": "RECONCILED",
                "exception_codes": [],
            }
        ],
        "expected_movements": [
            {
                "movement_type": "CAPTURE",
                "movement_id": "PAY-A",
                "available_on": "2026-09-14",
                "signed_amount": Decimal("100.00"),
                "supporting_source_record_ids": ["PAY-A"],
            },
            {
                "movement_type": "FEE",
                "movement_id": "FEE-A",
                "available_on": "2026-09-14",
                "signed_amount": Decimal("-2.00"),
                "supporting_source_record_ids": ["PAY-A", "FEE-A"],
            },
        ],
        "payouts": [
            {
                "run_id": "RUN-A",
                "payout_id": "PAYOUT-A",
                "payout_date": "2026-09-14",
                "currency": "EUR",
                "expected_payout": Decimal("98.00"),
                "reported_line_total": Decimal("98.00"),
                "reported_net_amount": Decimal("98.00"),
                "provider_report_variance": Decimal("0.00"),
                "end_to_end_payout_variance": Decimal("0.00"),
                "state": "RECONCILED",
            }
        ],
        "exceptions": [],
        "source_completeness": [
            {
                "report_type": "PAYMENTS",
                "report_date": "2026-09-14",
                "state": "COMPLETE",
                "observed_at_utc": "2026-09-15T12:00:00+00:00",
                "issue_codes": [],
            }
        ],
        "quarantine_records": [],
    }


def test_deterministic_fingerprint():
    first = result()
    second = deepcopy(first)

    assert logical_fingerprint(first) == logical_fingerprint(second)
    assert len(logical_fingerprint(first)) == 64


def test_execution_and_delivery_metadata_do_not_change_fingerprint():
    first = result()
    second = deepcopy(first)

    second["run_id"] = "RUN-B"
    second["detected_at_utc"] = datetime(
        2026, 9, 16, 12, 0, tzinfo=timezone.utc
    )
    second["transactions"][0]["run_id"] = "RUN-B"
    second["payouts"][0]["run_id"] = "RUN-B"
    second["transactions"][0]["original_filename"] = "redelivery.json"
    second["source_completeness"][0]["observed_at_utc"] = (
        "2026-09-16T12:00:00+00:00"
    )
    second["quarantine_records"] = [
        {
            "receipt_id": "RECEIPT-B",
            "reason_code": "INVALID_AMOUNT",
        }
    ]

    assert_financial_equal(first, second)


def test_collection_order_does_not_change_fingerprint():
    first = result()
    second = deepcopy(first)

    second["expected_movements"].reverse()
    second["expected_movements"][0][
        "supporting_source_record_ids"
    ].reverse()

    assert logical_fingerprint(first) == logical_fingerprint(second)


def test_json_round_trip_preserves_financial_fingerprint():
    original = result()

    stored_json = canonical_json(json_value(original))
    restored = json.loads(stored_json)

    assert logical_document(original) == logical_document(restored)
    assert logical_fingerprint(original) == logical_fingerprint(restored)


def test_decimal_string_and_integer_amounts_are_equivalent():
    first = result()
    second = deepcopy(first)
    third = deepcopy(first)

    first["transactions"][0]["captured_total"] = Decimal("100.0")
    second["transactions"][0]["captured_total"] = "100.00"
    third["transactions"][0]["captured_total"] = 100

    assert_financial_equal(first, second)
    assert_financial_equal(second, third)


@pytest.mark.parametrize(
    "field_name",
    [
        "expected_payout",
        "reported_line_total",
        "reported_net_amount",
    ],
)
def test_each_payout_total_remains_independently_visible(field_name):
    first = result()
    second = deepcopy(first)

    second["payouts"][0][field_name] += Decimal("0.01")

    assert logical_fingerprint(first) != logical_fingerprint(second)


def test_fee_change_changes_fingerprint():
    first = result()
    second = deepcopy(first)

    second["transactions"][0]["reported_fee_total"] = Decimal("2.01")

    assert logical_fingerprint(first) != logical_fingerprint(second)


def test_state_change_changes_fingerprint():
    first = result()
    second = deepcopy(first)

    second["transactions"][0]["state"] = "INCOMPLETE"

    assert logical_fingerprint(first) != logical_fingerprint(second)


def test_completeness_change_changes_fingerprint():
    first = result()
    second = deepcopy(first)

    second["source_completeness"][0]["state"] = "INCOMPLETE"

    assert logical_fingerprint(first) != logical_fingerprint(second)


def test_contract_version_change_changes_fingerprint():
    first = result()
    second = deepcopy(first)
    second["contract_version"] = "test-v2"

    assert logical_fingerprint(first) != logical_fingerprint(second)


def test_duplicate_financial_output_is_not_silently_removed():
    first = result()
    second = deepcopy(first)

    second["transactions"].append(
        deepcopy(second["transactions"][0])
    )

    assert logical_fingerprint(first) != logical_fingerprint(second)


def test_negative_zero_and_positive_zero_are_equivalent():
    first = result()
    second = deepcopy(first)

    second["payouts"][0]["provider_report_variance"] = Decimal("-0.00")

    assert_financial_equal(first, second)


def test_float_money_is_rejected():
    candidate = result()
    candidate["transactions"][0]["captured_total"] = 100.0

    with pytest.raises(TypeError):
        logical_fingerprint(candidate)


def test_fractional_cent_is_rejected():
    candidate = result()
    candidate["transactions"][0]["captured_total"] = Decimal("100.001")

    with pytest.raises(ValueError, match="fractional cent"):
        logical_fingerprint(candidate)


def test_mismatch_identifies_differing_section():
    first = result()
    second = deepcopy(first)

    second["transactions"][0]["captured_total"] = Decimal("125.00")

    with pytest.raises(
        FinancialResultMismatch,
        match="transactions",
    ):
        assert_financial_equal(first, second)


def test_stored_artifact_can_be_verified():
    candidate = result()
    payload = json_value(candidate)
    payload["financial_fingerprint_version"] = (
        FINANCIAL_FINGERPRINT_VERSION
    )

    publication = {
        "result_payload": payload,
        "logical_fingerprint": logical_fingerprint(candidate),
    }

    assert verify_stored_financial_result(publication) == (
        publication["logical_fingerprint"]
    )


def test_changed_stored_artifact_fails_verification():
    candidate = result()
    payload = json_value(candidate)
    payload["financial_fingerprint_version"] = (
        FINANCIAL_FINGERPRINT_VERSION
    )

    publication = {
        "result_payload": payload,
        "logical_fingerprint": logical_fingerprint(candidate),
    }

    payload["transactions"][0]["captured_total"] = "125.00"

    with pytest.raises(FinancialResultMismatch, match="does not match"):
        verify_stored_financial_result(publication)