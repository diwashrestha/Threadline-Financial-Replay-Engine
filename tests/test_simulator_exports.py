import json

from datetime import date, datetime, timezone
from hashlib import sha256

from threadline.contracts import parse_manifest
from threadline.simulator.catalog import (
    build_catalog,
    generate_order_draft,
)
from threadline.simulator.exports import (
    build_report_specs,
    export_history,
)
from threadline.simulator.fees import generate_processing_fee
from threadline.simulator.orders_payments import (
    generate_checkout,
)
from threadline.simulator.settlement import (
    build_settlement_book,
)


def _clean_specs():
    draft = generate_order_draft(
        order_id="ORD-000001",
        seed=42,
        catalog=build_catalog(),
    )
    checkout = generate_checkout(
        draft=draft,
        seed=42,
        checkout_started_at_utc=datetime(
            2026, 9, 14, 10,
            tzinfo=timezone.utc,
        ),
        forced_outcome="FIRST_CAPTURE",
    )
    fee = generate_processing_fee(checkout)
    assert fee is not None

    settlement = build_settlement_book(
        checkouts=(checkout,),
        fees=(fee,),
        refunds=(),
    )

    return build_report_specs(
        checkouts=(checkout,),
        fees=(fee,),
        return_refunds=(),
        settlement=settlement,
        start_date=date(2026, 9, 14),
        end_date=settlement.payouts[-1].payout_date,
    )


def test_export_is_complete_and_reproducible(tmp_path):
    specs = _clean_specs()

    first = export_history(
        specs,
        output_directory=tmp_path,
        dataset_id="sim-v1-seed-42",
    )
    second = export_history(
        specs,
        output_directory=tmp_path,
        dataset_id="sim-v1-seed-42",
    )

    assert first == second
    assert len(first) == 6 * len(
        {
            spec.business_date
            for spec in specs
        }
    )
    assert not list(tmp_path.glob("*.part"))

    for exported in first:
        data_bytes = exported.data_path.read_bytes()
        manifest = json.loads(
            exported.manifest_path.read_text(
                encoding="utf-8"
            )
        )
        parse_manifest(manifest)

        assert manifest["row_count"] == len(
            json.loads(data_bytes)
        )
        assert manifest["sha256"] == (
            sha256(data_bytes).hexdigest()
        )