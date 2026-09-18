from decimal import Decimal

import pytest

from threadline.contracts import ReportType
from tests.integration.recovery_scenarios import payment


pytestmark = pytest.mark.integration


def published_transaction_rows(scenario):
    return scenario.rows(
        """
        SELECT t.*
        FROM publication_pointer p
        JOIN transaction_reconciliation t
            ON t.run_id = p.current_run_id
        WHERE p.publication_name = 'threadline'
        ORDER BY t.order_id
        """
    )


def test_f04_candidate_rows_roll_back_before_pointer_update(scenario):
    previous = scenario.clean_baseline()

    _, delivery = scenario.deliver(
        ReportType.PAYMENTS,
        [payment(amount="120.00", version=2)],
    )

    counts_before = {
        table: scenario.scalar(f"SELECT COUNT(*) FROM {table}")
        for table in (
            "reconciliation_run",
            "recovery_result",
            "transaction_reconciliation",
            "payout_reconciliation",
            "reconciliation_exception",
            "source_completeness_snapshot",
        )
    }

    observations = []

    def fail_after_result_rows(point):
        if point != "after_result_insert":
            return

        # A separate connection reads while candidate rows are uncommitted.
        observations.append(
            {
                "run_id": scenario.published()["current_run_id"],
                "transactions": published_transaction_rows(scenario),
            }
        )

        raise RuntimeError("Injected publication failure")

    with pytest.raises(RuntimeError, match="publication failure"):
        scenario.worker(
            failure_hook=fail_after_result_rows,
        ).run_next()

    assert len(observations) == 1
    assert observations[0]["run_id"] == previous["current_run_id"]

    visible_during_failure = observations[0]["transactions"]

    assert len(visible_during_failure) == 1
    assert (
        visible_during_failure[0]["run_id"]
        == previous["current_run_id"]
    )
    assert (
        visible_during_failure[0]["captured_total"]
        == Decimal("100.00")
    )

    for table, expected_count in counts_before.items():
        assert scenario.scalar(
            f"SELECT COUNT(*) FROM {table}"
        ) == expected_count

    assert (
        scenario.published()["current_run_id"]
        == previous["current_run_id"]
    )

    visible_after_failure = published_transaction_rows(scenario)

    assert len(visible_after_failure) == 1
    assert visible_after_failure[0]["captured_total"] == Decimal("100.00")

    request = scenario.rows(
        """
        SELECT status, attempt_count, result_run_id
        FROM recovery_request
        WHERE request_id = %s
        """,
        (delivery.request_id,),
    )[0]

    assert request["status"] == "PENDING"
    assert request["attempt_count"] == 1
    assert request["result_run_id"] is None

    scenario.allow_retry_now(delivery.request_id)

    outcome = scenario.worker().run_next()
    assert outcome is not None

    current = scenario.published()

    assert current["current_run_id"] != previous["current_run_id"]

    visible_after_retry = published_transaction_rows(scenario)

    assert len(visible_after_retry) == 1
    assert (
        visible_after_retry[0]["run_id"]
        == current["current_run_id"]
    )
    assert visible_after_retry[0]["captured_total"] == Decimal("120.00")

    # Historical rows remain retained, but the pointer exposes only one run.
    assert scenario.scalar(
        """
        SELECT COUNT(DISTINCT t.run_id)
        FROM publication_pointer p
        JOIN transaction_reconciliation t
            ON t.run_id = p.current_run_id
        WHERE p.publication_name = 'threadline'
        """
    ) == 1