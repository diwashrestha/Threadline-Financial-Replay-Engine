from datetime import date, datetime, timezone

from threadline.simulator.catalog import (
    build_catalog,
    generate_order_draft,
)
from threadline.simulator.orders_payments import (
    add_business_days,
    captured_payment_source_record,
    generate_checkout,
)


def _draft():
    return generate_order_draft(
        order_id="ORD-000001",
        seed=42,
        catalog=build_catalog(),
    )


def _start():
    return datetime(
        2026, 9, 18, 16, 0,
        tzinfo=timezone.utc,
    )


def test_successful_capture_never_exceeds_order_total():
    for outcome in ("FIRST_CAPTURE", "RETRY_CAPTURE"):
        checkout = generate_checkout(
            draft=_draft(),
            seed=42,
            checkout_started_at_utc=_start(),
            forced_outcome=outcome,
        )

        assert checkout.placed_order is not None

        captured = [
            attempt
            for attempt in checkout.attempts
            if attempt.status == "CAPTURED"
        ]
        assert len(captured) == 1
        assert sum(
            attempt.captured_amount_cents
            for attempt in checkout.attempts
        ) == checkout.draft.order_total_gross_cents

        payment_record = captured_payment_source_record(
            checkout
        )
        assert payment_record is not None
        assert payment_record["status"] == "CAPTURED"
        assert payment_record["available_on"] == "2026-09-22"


def test_decline_then_retry_has_one_capture():
    checkout = generate_checkout(
        draft=_draft(),
        seed=42,
        checkout_started_at_utc=_start(),
        forced_outcome="RETRY_CAPTURE",
    )

    assert [attempt.status for attempt in checkout.attempts] == [
        "DECLINED",
        "CAPTURED",
    ]
    assert [attempt.attempt_number for attempt in checkout.attempts] == [
        1,
        2,
    ]
    assert checkout.attempts[0].available_on is None


def test_abandoned_checkout_creates_no_financial_order():
    checkout = generate_checkout(
        draft=_draft(),
        seed=42,
        checkout_started_at_utc=_start(),
        forced_outcome="ABANDONED",
    )

    assert checkout.placed_order is None
    assert captured_payment_source_record(checkout) is None
    assert sum(
        attempt.captured_amount_cents
        for attempt in checkout.attempts
    ) == 0


def test_same_inputs_reproduce_identical_checkout():
    arguments = {
        "draft": _draft(),
        "seed": 42,
        "checkout_started_at_utc": _start(),
    }

    assert generate_checkout(**arguments) == generate_checkout(
        **arguments
    )


def test_settlement_lag_skips_weekends():
    # Friday September 18 + two weekdays = Tuesday September 22.
    assert add_business_days(
        date(2026, 9, 18),
        2,
    ) == date(2026, 9, 22)