from decimal import Decimal

import pytest

from threadline.contracts import ReportType
from tests.integration.recovery_scenarios import (
    Scenario,
    baseline_reports,
    payment,
)


pytestmark = pytest.mark.integration


def test_l03_arrival_sequences_have_identical_fingerprints(
    scenario_database_url,
    tmp_path,
):
    sequences = (
        (1, 2),
        (2, 1),
        (1, 2, 2),
    )

    fingerprints = []

    # Separate publication names are unnecessary here: reset the dedicated
    # test schema between independent sequences.
    from tests.integration.conftest import reset_scenario_database

    for index, sequence in enumerate(sequences):
        reset_scenario_database(
            scenario_database_url,
            expected_name="threadline_test",
        )

        scenario = Scenario(
            scenario_database_url,
            tmp_path / str(index),
        )

        for report_type, records in baseline_reports().items():
            if report_type is not ReportType.PAYMENTS:
                scenario.deliver(report_type, records)

        for version in sequence:
            scenario.deliver(
                ReportType.PAYMENTS,
                [
                    payment(
                        amount="100.00" if version == 1 else "120.00",
                        version=version,
                    )
                ],
            )
            scenario.recover()

        assert Decimal(
            scenario.transaction()["captured_total"]
        ) == Decimal("120.00")

        resolution = scenario.rows(
            """
            SELECT winning_version, resolution_state
            FROM entity_resolution
            WHERE entity_type = 'PAYMENT' AND source_id = 'PAY-001'
            """
        )[0]

        assert resolution["winning_version"] == 2
        assert resolution["resolution_state"] == "ACCEPTED"

        assert scenario.scalar(
            """
            SELECT COUNT(*)
            FROM source_record_version
            WHERE entity_type = 'PAYMENT' AND source_id = 'PAY-001'
            """
        ) == 2

        scenario.assert_oracle()

        fingerprints.append(
            scenario.published()["logical_fingerprint"]
        )

    assert len(set(fingerprints)) == 1


def test_same_delivery_retry_reuses_batch_and_request(scenario):
    scenario.clean_baseline()

    path = scenario.make_file(
        ReportType.PAYMENTS,
        [payment(amount="120.00", version=2)],
    )

    first = scenario.ingest(path)
    second = scenario.ingest(path)

    assert first is not None
    assert second is not None

    assert second.reused_existing_batch
    assert second.batch_id == first.batch_id
    assert second.request_id == first.request_id

    assert scenario.scalar(
        """
        SELECT COUNT(*)
        FROM source_receipt
        WHERE batch_id = %s
        """,
        (first.batch_id,),
    ) == 1

    assert scenario.scalar(
        """
        SELECT COUNT(*)
        FROM recovery_request
        WHERE trigger_batch_id = %s
        """,
        (first.batch_id,),
    ) == 1


def test_redelivery_under_different_filename_preserves_financial_result(
    scenario,
):
    baseline = scenario.clean_baseline()

    first_path, first = scenario.deliver(
        ReportType.PAYMENTS,
        [payment()],
    )
    second_path, second = scenario.deliver(
        ReportType.PAYMENTS,
        [payment()],
    )

    assert first_path.name != second_path.name
    assert first.batch_id != second.batch_id

    scenario.recover()

    assert (
        scenario.published()["logical_fingerprint"]
        == baseline["logical_fingerprint"]
    )

    assert scenario.scalar(
        """
        SELECT COUNT(*)
        FROM source_record_version
        WHERE entity_type = 'PAYMENT' AND source_id = 'PAY-001'
        """
    ) == 1

    assert scenario.scalar(
        """
        SELECT COUNT(*)
        FROM source_receipt
        WHERE entity_type = 'PAYMENT' AND source_id = 'PAY-001'
        """
    ) == 3

    assert Decimal(
        scenario.transaction()["captured_total"]
    ) == Decimal("100.00")