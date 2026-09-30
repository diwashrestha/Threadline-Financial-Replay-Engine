from datetime import datetime, timedelta, timezone

import pytest

from threadline.contracts import (
    EntityType,
    parse_source_record,
)
from threadline.simulator.catalog import (
    CATALOG_VERSION,
    OrderDraft,
    OrderItem,
    build_catalog,
)
from threadline.simulator.orders_payments import (
    generate_checkout,
)
from threadline.simulator.returns_refunds import (
    create_return_refund,
    generate_return_history,
    refund_source_record,
    validate_return_history,
)


def _checkout():
    sku = build_catalog()[0]
    order_id = "ORD-000001"

    # Three purchased units make a genuine partial return possible.
    item = OrderItem(
        item_id=f"{order_id}-L01",
        order_id=order_id,
        sku_id=sku.sku_id,
        quantity=3,
        unit_price_gross_cents=sku.price_gross_cents,
        line_total_gross_cents=(
            3 * sku.price_gross_cents
        ),
    )
    draft = OrderDraft(
        order_id=order_id,
        catalog_version=CATALOG_VERSION,
        items=(item,),
        order_total_gross_cents=item.line_total_gross_cents,
    )

    return generate_checkout(
        draft=draft,
        seed=42,
        checkout_started_at_utc=datetime(
            2026, 9, 14, 10, 0,
            tzinfo=timezone.utc,
        ),
        forced_outcome="FIRST_CAPTURE",
    )


def test_partial_return_refunds_only_returned_units():
    checkout = _checkout()

    history = generate_return_history(
        checkout=checkout,
        seed=42,
        forced_outcome="PARTIAL",
    )

    refunded = sum(
        pair.refund.amount_cents
        for pair in history
    )

    assert 0 < refunded < (
        checkout.draft.order_total_gross_cents
    )

    validate_return_history(checkout, history)

    for pair in history:
        source = refund_source_record(pair)
        parsed = parse_source_record(
            EntityType.REFUND,
            source,
        )
        assert parsed is not None


def test_full_return_refunds_exactly_the_capture():
    checkout = _checkout()

    history = generate_return_history(
        checkout=checkout,
        seed=42,
        forced_outcome="FULL",
    )

    assert len(history) == 1
    assert history[0].refund.amount_cents == (
        checkout.draft.order_total_gross_cents
    )
    validate_return_history(checkout, history)


def test_second_return_cannot_refund_a_unit_twice():
    checkout = _checkout()
    full_history = generate_return_history(
        checkout=checkout,
        seed=42,
        forced_outcome="FULL",
    )

    later = (
        full_history[0].refund.effective_at_utc
        + timedelta(days=3)
    )

    with pytest.raises(
        ValueError,
        match="returned twice",
    ):
        create_return_refund(
            checkout=checkout,
            existing=full_history,
            quantities_by_item_id={
                checkout.draft.items[0].item_id: 1
            },
            returned_at_utc=later,
            refund_at_utc=later + timedelta(days=2),
        )


def test_same_inputs_reproduce_the_same_refunds():
    checkout = _checkout()

    first = generate_return_history(
        checkout=checkout,
        seed=20260914,
        forced_outcome="PARTIAL",
    )
    second = generate_return_history(
        checkout=checkout,
        seed=20260914,
        forced_outcome="PARTIAL",
    )

    assert first == second