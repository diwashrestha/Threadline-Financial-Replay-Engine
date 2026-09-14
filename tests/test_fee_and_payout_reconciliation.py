"""Fee and payout reconciliation scenarios.

G13: fee rounding
G14: incorrect provider fee
G15: payout header differs from settlement lines
G16: missing eligible settlement movement
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timezone
from decimal import Decimal

from threadline.canonicalize import (
    RecordEnvelope,
    canonicalize,
)
from threadline.completeness import (
    CompletenessResult,
    report_deadline_utc,
)
from threadline.contracts import (
    EntityType,
    ExceptionCode,
    Fee,
    FinancialRecord,
    Order,
    Payment,
    Payout,
    ReconciliationState,
    ReportType,
    SettlementLine,
    SourceCompleteness,
    SourceReportStatus,
    SourceSystem,
    parse_source_record,
)
from threadline.money import calculate_fee
from threadline.reconcile import (
    ReconciliationException,
    ReconciliationResult,
    reconcile,
)


BUSINESS_DATE = date(2026, 1, 2)

DETECTED_AT = datetime(
    2026,
    1,
    3,
    11,
    30,
    tzinfo=timezone.utc,
)


def _order(
    *,
    order_id: str,
    amount: str,
) -> Order:
    record = parse_source_record(
        EntityType.ORDER,
        {
            "order_id": order_id,
            "created_at_utc": "2026-01-02T08:00:00Z",
            "status": "PAID",
            "currency": "EUR",
            "order_total": amount,
            "source_version": 1,
        },
    )

    assert isinstance(record, Order)
    return record


def _payment(
    *,
    payment_id: str,
    order_id: str,
    amount: str,
) -> Payment:
    record = parse_source_record(
        EntityType.PAYMENT,
        {
            "payment_id": payment_id,
            "order_id": order_id,
            "attempt_number": 1,
            "payment_method": "CARD",
            "status": "CAPTURED",
            "amount": amount,
            "currency": "EUR",
            "effective_at_utc": "2026-01-02T08:01:00Z",
            "available_on": "2026-01-02",
            "source_version": 1,
        },
    )

    assert isinstance(record, Payment)
    return record


def _fee(
    *,
    fee_id: str,
    payment_id: str,
    amount: str,
) -> Fee:
    record = parse_source_record(
        EntityType.FEE,
        {
            "fee_id": fee_id,
            "payment_id": payment_id,
            "fee_type": "PROCESSING",
            "amount": amount,
            "currency": "EUR",
            "effective_at_utc": "2026-01-02T08:01:01Z",
            "available_on": "2026-01-02",
            "source_version": 1,
        },
    )

    assert isinstance(record, Fee)
    return record


def _settlement_line(
    *,
    line_id: str,
    payout_id: str,
    movement_type: str,
    movement_id: str,
    signed_amount: str,
) -> SettlementLine:
    record = parse_source_record(
        EntityType.SETTLEMENT_LINE,
        {
            "settlement_line_id": line_id,
            "payout_id": payout_id,
            "movement_type": movement_type,
            "movement_id": movement_id,
            "signed_amount": signed_amount,
            "currency": "EUR",
            "source_version": 1,
        },
    )

    assert isinstance(record, SettlementLine)
    return record


def _payout(
    *,
    payout_id: str,
    reported_amount: str,
) -> Payout:
    record = parse_source_record(
        EntityType.PAYOUT,
        {
            "payout_id": payout_id,
            "payout_date": "2026-01-02",
            "currency": "EUR",
            "reported_net_amount": reported_amount,
            "source_version": 1,
        },
    )

    assert isinstance(record, Payout)
    return record


def _complete_reports() -> tuple[
    CompletenessResult,
    ...,
]:
    results: list[CompletenessResult] = []

    for report_type in ReportType:
        source_system = (
            SourceSystem.THREADLINE_SHOP
            if report_type is ReportType.ORDERS
            else SourceSystem.MOCKPAY
        )

        status = SourceReportStatus(
            source_system=source_system,
            report_type=report_type,
            business_date=BUSINESS_DATE,
            state=SourceCompleteness.COMPLETE,
            batch_id=f"test-{report_type.value}",
            reason=None,
        )

        results.append(
            CompletenessResult(
                status=status,
                deadline_at_utc=report_deadline_utc(
                    BUSINESS_DATE,
                    report_type,
                ),
                issues=(),
            )
        )

    return tuple(
        sorted(
            results,
            key=lambda result: result.sort_key,
        )
    )


def _run(
    records: Iterable[FinancialRecord],
) -> ReconciliationResult:
    envelopes = [
        RecordEnvelope(
            receipt_id=(
                f"{record.ENTITY_TYPE.value.lower()}:"
                f"{index}"
            ),
            record=record,
        )
        for index, record in enumerate(
            records,
            start=1,
        )
    ]

    return reconcile(
        run_id="test-fee-payout-reconciliation",
        detected_at=DETECTED_AT,
        canonicalization=canonicalize(envelopes),
        completeness_results=_complete_reports(),
    )


def _exceptions_of_type(
    result: ReconciliationResult,
    code: ExceptionCode,
) -> list[ReconciliationException]:
    return [
        exception
        for exception in result.exceptions
        if exception.exception_type is code
    ]


# ------------------------------------------------------------------
# G13: fee rounding
# ------------------------------------------------------------------


def test_g13_card_fee_rounds_half_up_to_cents():
    unrounded_fee = (
        Decimal("0.20")
        + Decimal("19.99") * Decimal("0.018")
    )

    assert unrounded_fee == Decimal("0.55982")

    expected_fee = calculate_fee(
        "CARD",
        Decimal("19.99"),
    )

    assert expected_fee == Decimal("0.56")

    result = _run(
        [
            _order(
                order_id="ORD-013",
                amount="19.99",
            ),
            _payment(
                payment_id="PAY-013",
                order_id="ORD-013",
                amount="19.99",
            ),
            _fee(
                fee_id="FEE-013",
                payment_id="PAY-013",
                amount="0.56",
            ),
        ]
    )

    transaction = result.transactions[0]

    assert transaction.expected_collection == Decimal("19.99")
    assert transaction.captured_total == Decimal("19.99")
    assert transaction.expected_fee_total == Decimal("0.56")
    assert transaction.reported_fee_total == Decimal("0.56")
    assert transaction.lifetime_net_collection == Decimal("19.43")
    assert transaction.state is ReconciliationState.RECONCILED
    assert transaction.exception_codes == ()

    assert not _exceptions_of_type(
        result,
        ExceptionCode.FEE_MISMATCH,
    )


# ------------------------------------------------------------------
# G14: incorrect provider fee
# ------------------------------------------------------------------


def test_g14_incorrect_provider_fee_creates_fee_mismatch():
    result = _run(
        [
            _order(
                order_id="ORD-014",
                amount="100.00",
            ),
            _payment(
                payment_id="PAY-014",
                order_id="ORD-014",
                amount="100.00",
            ),
            _fee(
                fee_id="FEE-014",
                payment_id="PAY-014",
                amount="3.00",
            ),
            _settlement_line(
                line_id="SL-014-A",
                payout_id="PO-014",
                movement_type="CAPTURE",
                movement_id="PAY-014",
                signed_amount="100.00",
            ),
            _settlement_line(
                line_id="SL-014-B",
                payout_id="PO-014",
                movement_type="FEE",
                movement_id="FEE-014",
                signed_amount="-3.00",
            ),
            _payout(
                payout_id="PO-014",
                reported_amount="97.00",
            ),
        ]
    )

    transaction = result.transactions[0]
    payout = result.payouts[0]

    assert transaction.expected_fee_total == Decimal("2.00")
    assert transaction.reported_fee_total == Decimal("3.00")
    assert transaction.state is ReconciliationState.EXCEPTION
    assert transaction.exception_codes == (
        ExceptionCode.FEE_MISMATCH,
    )

    fee_mismatches = _exceptions_of_type(
        result,
        ExceptionCode.FEE_MISMATCH,
    )

    assert len(fee_mismatches) == 1

    fee_exception = fee_mismatches[0]

    assert fee_exception.entity_id == "PAY-014"
    assert fee_exception.expected_amount == Decimal("2.00")
    assert fee_exception.actual_amount == Decimal("3.00")
    assert fee_exception.variance == Decimal("1.00")

    # MockPay's payout header agrees with its own detail.
    assert payout.reported_line_total == Decimal("97.00")
    assert payout.reported_net_amount == Decimal("97.00")
    assert payout.provider_report_variance == Decimal("0.00")

    # Contractual expectation remains €100 - €2 = €98.
    assert payout.expected_payout == Decimal("98.00")
    assert (
        payout.end_to_end_payout_variance
        == Decimal("-1.00")
    )
    assert payout.state is ReconciliationState.EXCEPTION

    # The settlement detail correctly reflects MockPay's reported
    # €3 fee, so this is not a settlement-line error.
    assert not _exceptions_of_type(
        result,
        ExceptionCode.SETTLEMENT_LINE_AMOUNT_MISMATCH,
    )
    assert not _exceptions_of_type(
        result,
        ExceptionCode.PAYOUT_TOTAL_MISMATCH,
    )

    assert {
        exception.exception_type
        for exception in result.exceptions
    } == {
        ExceptionCode.FEE_MISMATCH,
    }


# ------------------------------------------------------------------
# G15: payout header differs from itemized lines
# ------------------------------------------------------------------


def test_g15_payout_header_does_not_equal_settlement_lines():
    result = _run(
        [
            _order(
                order_id="ORD-015",
                amount="100.00",
            ),
            _payment(
                payment_id="PAY-015",
                order_id="ORD-015",
                amount="100.00",
            ),
            _fee(
                fee_id="FEE-015",
                payment_id="PAY-015",
                amount="2.00",
            ),
            _settlement_line(
                line_id="SL-015-A",
                payout_id="PO-015",
                movement_type="CAPTURE",
                movement_id="PAY-015",
                signed_amount="100.00",
            ),
            _settlement_line(
                line_id="SL-015-B",
                payout_id="PO-015",
                movement_type="FEE",
                movement_id="FEE-015",
                signed_amount="-2.00",
            ),
            _payout(
                payout_id="PO-015",
                reported_amount="97.40",
            ),
        ]
    )

    transaction = result.transactions[0]
    payout = result.payouts[0]
    
    

    assert transaction.state is ReconciliationState.RECONCILED
    assert transaction.exception_codes == ()

    assert payout.expected_payout == Decimal("98.00")
    assert payout.reported_line_total == Decimal("98.00")
    assert payout.reported_net_amount == Decimal("97.40")

    assert (
        payout.provider_report_variance
        == Decimal("-0.60")
    )
    assert (
        payout.end_to_end_payout_variance
        == Decimal("-0.60")
    )
    assert payout.state is ReconciliationState.EXCEPTION
    assert payout.exception_codes == (
        ExceptionCode.PAYOUT_TOTAL_MISMATCH,
    )

    mismatches = _exceptions_of_type(
        result,
        ExceptionCode.PAYOUT_TOTAL_MISMATCH,
    )

    assert len(mismatches) == 1

    exception = mismatches[0]

    assert exception.entity_type == "PAYOUT"
    assert exception.entity_id == "PO-015"
    assert exception.expected_amount == Decimal("98.00")
    assert exception.actual_amount == Decimal("97.40")
    assert exception.variance == Decimal("-0.60")

    assert {
        item.exception_type
        for item in result.exceptions
    } == {
        ExceptionCode.PAYOUT_TOTAL_MISMATCH,
    }


# ------------------------------------------------------------------
# G16: missing eligible settlement movement
# ------------------------------------------------------------------


def test_g16_missing_fee_settlement_line():
    result = _run(
        [
            _order(
                order_id="ORD-016",
                amount="100.00",
            ),
            _payment(
                payment_id="PAY-016",
                order_id="ORD-016",
                amount="100.00",
            ),
            _fee(
                fee_id="FEE-016",
                payment_id="PAY-016",
                amount="2.00",
            ),
            _settlement_line(
                line_id="SL-016-A",
                payout_id="PO-016",
                movement_type="CAPTURE",
                movement_id="PAY-016",
                signed_amount="100.00",
            ),
            # No settlement line for FEE/FEE-016.
            _payout(
                payout_id="PO-016",
                reported_amount="100.00",
            ),
        ]
    )

    transaction = result.transactions[0]
    payout = result.payouts[0]

    assert transaction.state is ReconciliationState.RECONCILED
    assert transaction.expected_fee_total == Decimal("2.00")
    assert transaction.reported_fee_total == Decimal("2.00")

    assert payout.expected_payout == Decimal("98.00")
    assert payout.reported_line_total == Decimal("100.00")
    assert payout.reported_net_amount == Decimal("100.00")

    # MockPay's header agrees with the lines it supplied.
    assert payout.provider_report_variance == Decimal("0.00")

    # But the fee movement is absent, leaving €2 unexplained.
    assert (
        payout.end_to_end_payout_variance
        == Decimal("2.00")
    )
    assert payout.state is ReconciliationState.EXCEPTION
    assert payout.exception_codes == (
        ExceptionCode.MISSING_SETTLEMENT_LINE,
    )

    missing_lines = _exceptions_of_type(
        result,
        ExceptionCode.MISSING_SETTLEMENT_LINE,
    )

    assert len(missing_lines) == 1

    exception = missing_lines[0]

    assert exception.entity_type == "EXPECTED_MOVEMENT"
    assert exception.entity_id == "FEE:FEE-016"
    assert exception.expected_amount == Decimal("-2.00")
    assert exception.actual_amount == Decimal("0.00")
    assert exception.variance == Decimal("2.00")

    assert not _exceptions_of_type(
        result,
        ExceptionCode.PAYOUT_TOTAL_MISMATCH,
    )
    assert not _exceptions_of_type(
        result,
        ExceptionCode.FEE_MISMATCH,
    )

    assert {
        item.exception_type
        for item in result.exceptions
    } == {
        ExceptionCode.MISSING_SETTLEMENT_LINE,
    }