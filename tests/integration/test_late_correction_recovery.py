from decimal import Decimal

import pytest

from threadline.contracts import ReportType
from tests.integration.recovery_scenarios import payment


pytestmark = pytest.mark.integration


def test_l02_higher_version_replaces_lower_version(scenario):
    run_a = scenario.clean_baseline()

    _, correction = scenario.deliver(
        ReportType.PAYMENTS,
        [payment(amount="120.00", version=2)],
    )

    resolution = scenario.rows(
        """
        SELECT *
        FROM entity_resolution
        WHERE entity_type = 'PAYMENT' AND source_id = 'PAY-001'
        """
    )[0]

    assert resolution["winning_version"] == 2
    assert resolution["resolution_state"] == "ACCEPTED"
    assert resolution["selected_payload_hash"] is not None

    receipts = scenario.rows(
        """
        SELECT source_version, disposition
        FROM source_receipt
        WHERE entity_type = 'PAYMENT' AND source_id = 'PAY-001'
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

    assert scenario.scalar(
        """
        SELECT COUNT(*)
        FROM recovery_request
        WHERE request_id = %s AND status = 'PENDING'
        """,
        (correction.request_id,),
    ) == 1

    scenario.recover()

    assert (
        scenario.published()["current_run_id"]
        != run_a["current_run_id"]
    )

    assert Decimal(
        scenario.transaction()["captured_total"]
    ) == Decimal("120.00")

    # The order and provider fee still report the old values.
    # The corrected capture must expose those mismatches.
    assert scenario.transaction()["state"] != "RECONCILED"

    scenario.assert_oracle()


def test_l04_late_same_version_conflict_excludes_both_variants(scenario):
    scenario.clean_baseline()

    scenario.deliver(
        ReportType.PAYMENTS,
        [payment(amount="100.00", version=2)],
    )
    scenario.recover()

    run_a = scenario.published()
    assert scenario.transaction()["state"] == "RECONCILED"

    scenario.deliver(
        ReportType.PAYMENTS,
        [payment(amount="120.00", version=2)],
    )

    resolution = scenario.rows(
        """
        SELECT *
        FROM entity_resolution
        WHERE entity_type = 'PAYMENT' AND source_id = 'PAY-001'
        """
    )[0]

    assert resolution["winning_version"] == 2
    assert resolution["resolution_state"] == "CONFLICTED"
    assert resolution["selected_payload_hash"] is None

    variants = scenario.rows(
        """
        SELECT payload_hash, canonical_payload
        FROM source_record_version
        WHERE entity_type = 'PAYMENT'
          AND source_id = 'PAY-001'
          AND source_version = 2
        """
    )

    assert len(variants) == 2
    assert len({row["payload_hash"] for row in variants}) == 2

    assert {
        Decimal(row["canonical_payload"]["amount"])
        for row in variants
    } == {
        Decimal("100.00"),
        Decimal("120.00"),
    }

    receipts = scenario.rows(
        """
        SELECT disposition
        FROM source_receipt
        WHERE entity_type = 'PAYMENT'
          AND source_id = 'PAY-001'
          AND source_version = 2
        """
    )

    assert len(receipts) == 2
    assert all(
        row["disposition"] == "CONFLICTED"
        for row in receipts
    )

    scenario.recover()

    run_b = scenario.published()

    assert run_b["current_run_id"] != run_a["current_run_id"]
    assert scenario.transaction()["state"] != "RECONCILED"
    assert Decimal(
        scenario.transaction()["captured_total"]
    ) == Decimal("0.00")

    assert any(
        exception["exception_type"] == "CONFLICTING_SOURCE_VERSION"
        and exception["entity_id"] == "PAY-001"
        for exception in run_b["result_payload"]["exceptions"]
    )