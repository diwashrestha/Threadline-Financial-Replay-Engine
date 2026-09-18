from datetime import datetime, timezone
from decimal import Decimal

import pytest

from threadline.contracts import ReportType
from tests.integration.recovery_scenarios import (
    LATE_DAY,
    late_reports,
    refund,
)


pytestmark = pytest.mark.integration


def test_l01_late_refund_changes_lifetime_and_later_payout(scenario):
    run_a = scenario.clean_baseline()

    scenario.as_of = datetime(
        2026, 9, 18, 15, tzinfo=timezone.utc
    )

    refund_delivery = None

    for report_type, records in late_reports().items():
        _, outcome = scenario.deliver(
            report_type,
            records,
            day=LATE_DAY,
        )

        if report_type is ReportType.REFUNDS:
            refund_delivery = outcome

    assert refund_delivery is not None

    receipt = scenario.rows(
        """
        SELECT disposition
        FROM source_receipt
        WHERE batch_id = %s AND source_id = 'REF-001'
        """,
        (refund_delivery.batch_id,),
    )[0]

    assert receipt["disposition"] == "ACCEPTED"

    request = scenario.rows(
        """
        SELECT status
        FROM recovery_request
        WHERE request_id = %s
        """,
        (refund_delivery.request_id,),
    )[0]

    assert request["status"] == "PENDING"

    outcomes = scenario.recover()
    assert outcomes

    run_b = scenario.published()

    assert run_b["current_run_id"] != run_a["current_run_id"]

    transaction = scenario.transaction()

    assert Decimal(
        transaction["successful_refund_total"]
    ) == Decimal("25.00")

    assert Decimal(
        transaction["lifetime_net_collection"]
    ) == Decimal("73.00")

    assert transaction["state"] == "RECONCILED"

    document = run_b["result_payload"]

    refund_movement = next(
        movement
        for movement in document["expected_movements"]
        if movement["movement_type"] == "REFUND"
        and movement["movement_id"] == "REF-001"
    )

    assert refund_movement["available_on"] == LATE_DAY.isoformat()
    assert Decimal(
        refund_movement["signed_amount"]
    ) == Decimal("-25.00")

    payouts = {
        row["payout_id"]: row
        for row in document["payouts"]
    }

    assert Decimal(
        payouts["OUT-001"]["expected_payout"]
    ) == Decimal("98.00")

    assert payouts["OUT-002"]["payout_date"] == LATE_DAY.isoformat()
    assert Decimal(
        payouts["OUT-002"]["expected_payout"]
    ) == Decimal("23.90")
    assert payouts["OUT-002"]["state"] == "RECONCILED"

    fingerprint_before_replay = run_b["logical_fingerprint"]

    _, replay = scenario.deliver(
        ReportType.REFUNDS,
        [refund()],
        day=LATE_DAY,
    )

    assert scenario.rows(
        """
        SELECT disposition
        FROM source_receipt
        WHERE batch_id = %s
        """,
        (replay.batch_id,),
    )[0]["disposition"] == "DUPLICATE"

    scenario.recover()

    assert (
        scenario.published()["logical_fingerprint"]
        == fingerprint_before_replay
    )

    scenario.assert_oracle()