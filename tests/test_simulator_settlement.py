from datetime import date, datetime, timezone

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
from threadline.simulator.fees import generate_processing_fee
from threadline.simulator.orders_payments import (
    add_business_days,
    generate_checkout,
)
from threadline.simulator.returns_refunds import RefundFact
from threadline.simulator.settlement import (
    build_settlement_book,
    payout_source_record,
    settlement_line_source_record,
)


def _checkout(order_id, when):
    sku = build_catalog()[0]
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
        checkout_started_at_utc=when,
        forced_outcome="FIRST_CAPTURE",
    )


def _inputs():
    first = _checkout(
        "ORD-000001",
        datetime(2026, 9, 14, 10, tzinfo=timezone.utc),
    )
    second = _checkout(
        "ORD-000002",
        datetime(2026, 9, 17, 10, tzinfo=timezone.utc),
    )

    first_fee = generate_processing_fee(first)
    second_fee = generate_processing_fee(second)
    assert first_fee is not None
    assert second_fee is not None

    first_capture = first.attempts[0]
    refund_at = datetime(
        2026, 9, 17, 9,
        tzinfo=timezone.utc,
    )

    # A one-unit refund from the first order. The second
    # capture gives the refund date a positive payout balance.
    refund = RefundFact(
        refund_id="REF-000001-01",
        payment_id=first_capture.payment_id,
        amount_cents=(
            first.draft.items[0].unit_price_gross_cents
        ),
        effective_at_utc=refund_at,
        available_on=add_business_days(
            refund_at.date(),
            2,
        ),
    )

    return first, second, first_fee, second_fee, refund


def test_movements_are_allocated_once_and_totals_balance():
    first, second, first_fee, second_fee, refund = (
        _inputs()
    )

    book = build_settlement_book(
        checkouts=(first, second),
        fees=(first_fee, second_fee),
        refunds=(refund,),
    )

    assert len(book.movements) == 5
    assert len(book.lines) == 5
    assert len(book.payouts) == 2

    assert [p.payout_date for p in book.payouts] == [
        date(2026, 9, 16),
        date(2026, 9, 21),
    ]

    assert {
        (line.movement_type, line.movement_id)
        for line in book.lines
    } == {
        (movement.movement_type, movement.movement_id)
        for movement in book.movements
    }

    for payout in book.payouts:
        payout_lines = [
            line
            for line in book.lines
            if line.payout_id == payout.payout_id
        ]
        assert sum(
            line.signed_amount_cents
            for line in payout_lines
        ) == payout.reported_net_cents

        parse_source_record(
            EntityType.PAYOUT,
            payout_source_record(payout),
        )

        for line in payout_lines:
            parse_source_record(
                EntityType.SETTLEMENT_LINE,
                settlement_line_source_record(line),
            )


def test_input_order_does_not_change_settlement():
    first, second, first_fee, second_fee, refund = (
        _inputs()
    )

    forward = build_settlement_book(
        checkouts=(first, second),
        fees=(first_fee, second_fee),
        refunds=(refund,),
    )
    reversed_inputs = build_settlement_book(
        checkouts=(second, first),
        fees=(second_fee, first_fee),
        refunds=(refund,),
    )

    assert forward == reversed_inputs


def test_duplicate_refund_is_rejected():
    first, second, first_fee, second_fee, refund = (
        _inputs()
    )

    with pytest.raises(ValueError, match="Duplicate refund"):
        build_settlement_book(
            checkouts=(first, second),
            fees=(first_fee, second_fee),
            refunds=(refund, refund),
        )


def test_negative_daily_net_is_not_hidden():
    first, _, first_fee, _, refund = _inputs()

    too_large_for_that_day = RefundFact(
        refund_id=refund.refund_id,
        payment_id=refund.payment_id,
        amount_cents=(
            first.draft.order_total_gross_cents
        ),
        effective_at_utc=refund.effective_at_utc,
        available_on=refund.available_on,
    )

    with pytest.raises(ValueError, match="Negative daily net"):
        build_settlement_book(
            checkouts=(first,),
            fees=(first_fee,),
            refunds=(too_large_for_that_day,),
        )