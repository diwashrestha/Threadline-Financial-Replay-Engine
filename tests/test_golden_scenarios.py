"""End-to-end tests for Threadline golden scenarios."""

from decimal import Decimal
from pathlib import Path

import pytest

from threadline.cli import run_scenario
from threadline.contracts import ReconciliationState
from threadline.publication import AtomicFilePublicationStore


GOLDEN_DIRECTORY = (
    Path(__file__).parent
    / "fixtures"
    / "golden"
)

SCENARIOS = sorted(
    GOLDEN_DIRECTORY.glob("g*.json")
)


@pytest.mark.parametrize(
    "scenario_path",
    SCENARIOS,
    ids=lambda path: path.stem,
)
def test_golden_scenario_matches_expected_output(
    scenario_path: Path,
):
    outcome = run_scenario(scenario_path)

    assert outcome.scenario_id
    assert outcome.result.run_id
    assert outcome.result.contract_version


def test_g01_clean_order_end_to_end(tmp_path: Path):
    scenario_path = (
        GOLDEN_DIRECTORY
        / "g01_clean_order.json"
    )

    outcome = run_scenario(scenario_path)
    result = outcome.result

    assert len(result.transactions) == 1
    assert len(result.payouts) == 1
    assert len(result.expected_movements) == 2
    assert result.exceptions == ()
    assert result.quarantine_records == ()

    transaction = result.transactions[0]

    assert transaction.order_id == "order-1001"
    assert transaction.expected_collection == Decimal("100.00")
    assert transaction.captured_total == Decimal("100.00")
    assert transaction.collection_variance == Decimal("0.00")
    assert transaction.expected_fee_total == Decimal("2.00")
    assert transaction.reported_fee_total == Decimal("2.00")
    assert transaction.lifetime_net_collection == Decimal("98.00")
    assert transaction.state is ReconciliationState.RECONCILED

    payout = result.payouts[0]

    assert payout.payout_id == "payout-2026-09-14"
    assert payout.expected_payout == Decimal("98.00")
    assert payout.reported_line_total == Decimal("98.00")
    assert payout.reported_net_amount == Decimal("98.00")
    assert payout.provider_report_variance == Decimal("0.00")
    assert payout.end_to_end_payout_variance == Decimal("0.00")
    assert payout.state is ReconciliationState.RECONCILED

    published_path = tmp_path / "published-result.json"
    publisher = AtomicFilePublicationStore(published_path)

    receipt = publisher.publish(result)
    published = publisher.read_current()

    assert published_path.exists()
    assert receipt.run_id == result.run_id
    assert receipt.byte_count > 0
    assert len(receipt.payload_sha256) == 64

    assert published is not None
    assert published["run_id"] == result.run_id
    assert published["transactions"][0]["state"] == "RECONCILED"
    assert published["payouts"][0]["reported_net_amount"] == "98.00"