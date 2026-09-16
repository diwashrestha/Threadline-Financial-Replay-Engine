from datetime import date, timedelta
from types import SimpleNamespace

import pytest

from threadline.affected_windows import (
    DateWindow,
    RecoveryScope,
    business_day_utc_bounds,
    compress_dates,
    plan_affected_windows,
)
from threadline.contracts import EntityType
from threadline.late_data import ChangeKind


D14 = date(2026, 9, 14)
D16 = date(2026, 9, 16)
D20 = date(2026, 9, 20)


def fact(entity_type, source_id, **attributes):
    return SimpleNamespace(
        ENTITY_TYPE=entity_type,
        record_id=source_id,
        **attributes,
    )


def order(source_id="ORD-A"):
    return fact(EntityType.ORDER, source_id)


def payment(
    source_id="PAY-A",
    *,
    order_id="ORD-A",
    available_on=D14,
    status="CAPTURED",
):
    return fact(
        EntityType.PAYMENT,
        source_id,
        order_id=order_id,
        available_on=available_on,
        status=status,
    )


def refund(source_id="REF-A", *, payment_id="PAY-A", available_on=D20):
    return fact(
        EntityType.REFUND,
        source_id,
        payment_id=payment_id,
        available_on=available_on,
        status="SUCCEEDED",
    )


def plan(before, after, changed, **options):
    return plan_affected_windows(
        before_records=before,
        after_records=after,
        changed_records=changed,
        change_kind=options.pop("change_kind", ChangeKind.NEW_FACT),
        **options,
    )


@pytest.mark.parametrize(
    "change_kind",
    [
        ChangeKind.EXACT_DUPLICATE,
        ChangeKind.STALE_VERSION,
    ],
)
def test_duplicate_and_stale_records_require_no_window(change_kind):
    changed = payment()

    result = plan(
        [order(), changed],
        [order(), changed],
        [changed],
        change_kind=change_kind,
    )

    assert result.scope == RecoveryScope.NONE


def test_late_refund_includes_order_lifetime_and_payout_dependencies():
    original = [order(), payment()]
    late_refund = refund()

    result = plan(
        original,
        original + [late_refund],
        [late_refund],
    )

    assert result.scope == RecoveryScope.WINDOWS
    assert result.order_ids == ("ORD-A",)
    assert result.payout_dates == (D14, D20)


def test_correction_preserves_old_and_new_orders_and_dates():
    old = payment(order_id="ORD-A", available_on=D14)
    new = payment(order_id="ORD-B", available_on=D16)
    orders = [order("ORD-A"), order("ORD-B")]

    result = plan(
        orders + [old],
        orders + [new],
        [old, new],
        change_kind=ChangeKind.HIGHER_VERSION_CORRECTION,
    )

    assert result.scope == RecoveryScope.WINDOWS
    assert result.order_ids == ("ORD-A", "ORD-B")
    assert result.payout_dates == (D14, D16)


def test_fee_uses_its_parent_payment_date():
    fee = fact(
        EntityType.FEE,
        "FEE-A",
        payment_id="PAY-A",
    )
    original = [order(), payment()]

    result = plan(original, original + [fee], [fee])

    assert result.scope == RecoveryScope.WINDOWS
    assert result.order_ids == ("ORD-A",)
    assert result.payout_dates == (D14,)


def test_shared_payout_day_includes_other_orders():
    payout = fact(
        EntityType.PAYOUT,
        "PAYOUT-A",
        payout_date=D14,
    )
    records = [
        order("ORD-A"),
        order("ORD-B"),
        payment("PAY-A", order_id="ORD-A"),
        payment("PAY-B", order_id="ORD-B"),
        payout,
    ]

    result = plan(records, records, [payout])

    assert result.scope == RecoveryScope.WINDOWS
    assert result.order_ids == ("ORD-A", "ORD-B")
    assert result.payout_ids == ("PAYOUT-A",)
    assert result.payout_dates == (D14,)


def test_settlement_line_links_movement_and_payout_dates():
    payout = fact(
        EntityType.PAYOUT,
        "PAYOUT-A",
        payout_date=D20,
    )
    line = fact(
        EntityType.SETTLEMENT_LINE,
        "LINE-A",
        payout_id="PAYOUT-A",
        movement_type="CAPTURE",
        movement_id="PAY-A",
    )
    original = [order(), payment(), payout]

    result = plan(original, original + [line], [line])

    assert result.scope == RecoveryScope.WINDOWS
    assert result.order_ids == ("ORD-A",)
    assert result.payout_dates == (D14, D20)


def test_conflict_retains_previous_accepted_impact():
    old = payment(available_on=D14)
    conflicting = payment(available_on=D16)

    result = plan(
        [order(), old],
        [order()],
        [old, conflicting],
        change_kind=ChangeKind.SAME_VERSION_CONFLICT,
    )

    assert result.scope == RecoveryScope.WINDOWS
    assert result.order_ids == ("ORD-A",)
    assert result.payout_dates == (D14, D16)


def test_missing_parent_falls_back_to_full_rebuild():
    orphan = refund(payment_id="PAY-MISSING")

    result = plan([], [orphan], [orphan])

    assert result.scope == RecoveryScope.FULL
    assert (
        "RELATED_RECORD_MISSING:PAYMENT:PAY-MISSING"
        in result.reason_codes
    )


def test_completeness_change_requires_full_rebuild():
    result = plan(
        [],
        [],
        [],
        canonical_changed=False,
        completeness_changed=True,
    )

    assert result.scope == RecoveryScope.FULL
    assert result.reason_codes == ("REPORT_COMPLETENESS_CHANGED",)


def test_date_compression_preserves_gaps():
    result = compress_dates(
        [
            D14,
            D14 + timedelta(days=1),
            D20,
        ]
    )

    assert result == (
        DateWindow(D14, D16),
        DateWindow(D20, D20 + timedelta(days=1)),
    )


def test_business_day_bounds_handle_daylight_saving():
    start, end = business_day_utc_bounds(date(2026, 3, 29))

    assert end - start == timedelta(hours=23)


def test_input_order_does_not_change_plan():
    original = [order(), payment()]
    late_refund = refund()
    current = original + [late_refund]

    first = plan(original, current, [late_refund])
    second = plan(
        reversed(original),
        reversed(current),
        [late_refund],
    )

    assert first == second