from datetime import datetime, timezone

import pytest


from threadline.full_rebuild_recovery import FullRebuildRecovery
from threadline.publication import validate_for_publication
from threadline.recovery_adapters import build_candidate
from threadline.simulator.exports import export_history
from tests.test_simulator_exports import _clean_specs


@pytest.mark.integration
def test_clean_export_reconciles(scenario, tmp_path):
    scenario.as_of = datetime(
        2026, 9, 18, 12,
        tzinfo=timezone.utc,
    )

    exported = export_history(
        _clean_specs(),
        output_directory=tmp_path / "inbox",
        dataset_id="integration-clean-v1",
    )

    for report in exported:
        outcome = scenario.ingest(report.data_path)
        assert outcome is not None

    worker = FullRebuildRecovery(
        database_url=scenario.database_url,
        build_candidate=build_candidate,
        validate_domain=validate_for_publication,
    )

    while worker.run_next() is not None:
        pass

    document = scenario.published()["result_payload"]

    assert document["exceptions"] == []
    assert all(
        row["state"] == "RECONCILED"
        for row in document["transactions"]
    )
    assert all(
        row["state"] == "RECONCILED"
        for row in document["payouts"]
    )