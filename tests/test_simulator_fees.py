from datetime import datetime, timezone
from decimal import Decimal

from threadline.contracts import (
    EntityType,
    parse_source_record,
)
from threadline.money import calculate_fee
from threadline.simulator.catalog import (
    build_catalog,
    generate_order_draft,
)
from threadline.simulator.fees import (
    fee_source_record,
    generate_processing_fee,
)
from threadline.simulator.orders_payments import (
    captured_payment_source_record,
    generate_checkout,
)


def _checkout(outcome):
    draft = generate_order_draft(
        order_id="ORD-000001",
        seed=42,
        catalog=build_catalog(),
    )
    return generate_checkout(
        draft=draft,
        seed=42,
        checkout_started_at_utc=datetime(
            2026, 9, 14, 10, 0,
            tzinfo=timezone.utc,
        ),
        forced_outcome=outcome,
    )


def test_fee_matches_reconciliation_schedule_exactly():
    checkout = _checkout("RETRY_CAPTURE")
    payment = captured_payment_source_record(checkout)
    fee = generate_processing_fee(checkout)

    assert payment is not None
    assert fee is not None

    expected = calculate_fee(
        payment["payment_method"],
        Decimal(payment["amount"]),
        currency=payment["currency"],
    )

    source = fee_source_record(fee)

    assert Decimal(source["amount"]) == expected
    assert source["payment_id"] == payment["payment_id"]
    assert source["available_on"] == payment["available_on"]
    assert source["fee_type"] == "PROCESSING"

    # Verify the actual Stage 2 source contract accepts it.
    parsed = parse_source_record(
        EntityType.FEE,
        source,
    )
    assert parsed is not None


def test_declined_abandoned_checkout_has_no_fee():
    checkout = _checkout("ABANDONED")

    assert generate_processing_fee(checkout) is None


def test_fee_is_reproducible():
    checkout = _checkout("FIRST_CAPTURE")

    assert generate_processing_fee(checkout) == (
        generate_processing_fee(checkout)
    )