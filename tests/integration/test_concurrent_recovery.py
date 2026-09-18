from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from threading import Barrier

import pytest

from threadline.contracts import ReportType
from tests.integration.recovery_scenarios import payment


pytestmark = pytest.mark.integration


@pytest.mark.parametrize("request_count", [1, 2])
def test_concurrent_workers_do_not_double_publish_or_lose_requests(
    scenario,
    request_count,
):
    scenario.clean_baseline()

    before_runs = scenario.scalar(
        "SELECT COUNT(*) FROM reconciliation_run"
    )

    deliveries = []

    for offset in range(request_count):
        _, delivery = scenario.deliver(
            ReportType.PAYMENTS,
            [
                payment(
                    amount="120.00" if offset == 0 else "130.00",
                    version=2 + offset,
                )
            ],
        )
        deliveries.append(delivery)

    barrier = Barrier(2)

    def run_worker():
        worker = scenario.worker()
        barrier.wait(timeout=10)
        return worker.run_next()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(run_worker)
            for _ in range(2)
        ]

        outcomes = [
            future.result(timeout=30)
            for future in futures
        ]

    successful = [
        outcome
        for outcome in outcomes
        if outcome is not None
    ]

    assert len(successful) == request_count
    assert len({
        outcome.request_id
        for outcome in successful
    }) == request_count

    assert {
        outcome.request_id
        for outcome in successful
    } == {
        delivery.request_id
        for delivery in deliveries
    }

    assert scenario.scalar(
        "SELECT COUNT(*) FROM reconciliation_run"
    ) == before_runs + request_count

    for delivery in deliveries:
        request = scenario.rows(
            """
            SELECT status, attempt_count, result_run_id
            FROM recovery_request
            WHERE request_id = %s
            """,
            (delivery.request_id,),
        )[0]

        assert request["status"] == "SUCCEEDED"
        assert request["attempt_count"] == 1
        assert request["result_run_id"] is not None

        assert scenario.scalar(
            """
            SELECT COUNT(*)
            FROM recovery_attempt
            WHERE request_id = %s
              AND outcome = 'SUCCEEDED'
            """,
            (delivery.request_id,),
        ) == 1

    expected_capture = (
        Decimal("120.00")
        if request_count == 1
        else Decimal("130.00")
    )

    assert Decimal(
        scenario.transaction()["captured_total"]
    ) == expected_capture

    assert scenario.scalar(
        """
        SELECT COUNT(*)
        FROM recovery_request
        WHERE status = 'PENDING'
        """
    ) == 0

    current = scenario.published()

    assert current["current_run_id"] in {
        outcome.run_id
        for outcome in successful
    }

    visible_rows = scenario.rows(
        """
        SELECT t.run_id, t.captured_total
        FROM publication_pointer p
        JOIN transaction_reconciliation t
            ON t.run_id = p.current_run_id
        WHERE p.publication_name = 'threadline'
          AND t.order_id = 'ORD-001'
        """
    )

    assert len(visible_rows) == 1
    assert visible_rows[0]["run_id"] == current["current_run_id"]
    assert visible_rows[0]["captured_total"] == expected_capture

    scenario.assert_oracle()