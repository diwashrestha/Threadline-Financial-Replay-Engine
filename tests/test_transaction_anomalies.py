"""Transaction anomaly scenarios for Threadline.

G02: missing payment
G03: payment report not due
G04: payment report overdue
G05: captured amount mismatch
G08: multiple captures
G09: orphan payment
G10: valid partial refund
G11: orphan refund
G12: excessive refunds
G19: malformed amount quarantine
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, datetime, timezone
from decimal import Decimal

from threadline.canonicalize import (
    RecordEnvelope,
    canonicalize,
)
from threadline.completeness import (
    CompletenessIssue,
    CompletenessIssueCode,
    CompletenessResult,
    report_deadline_utc,
)
from threadline.contracts import (
    ContractViolation,
    EntityType,
    ExceptionCode,
    Fee,
    FinancialRecord,
    Order,
    Payment,
    QuarantinedRecord,
    ReconciliationState,
    Refund,
    ReportType,
    SourceCompleteness,
    SourceReportStatus,
    SourceSystem,
    parse_source_record,
    quarantine_from_violation,
)
from threadline.reconcile import (
    ReconciliationException,
    ReconciliationResult,
    reconcile,
)


BUSINESS_DATE = date(2026, 9, 14)

AFTER_ALL_DEADLINES = datetime(
    2026,
    9,
    15,
    10,
    30,
    tzinfo=timezone.utc,
)

BEFORE_PAYMENT_DEADLINE = datetime(
    2026,
    9,
    14,
    23,
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
            "created_at_utc": "2026-09-14T08:00:00Z",
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
    attempt_number: int = 1,
) -> Payment:
    record = parse_source_record(
        EntityType.PAYMENT,
        {
            "payment_id": payment_id,
            "order_id": order_id,
            "attempt_number": attempt_number,
            "payment_method": "CARD",
            "status": "CAPTURED",
            "amount": amount,
            "currency": "EUR",
            "effective_at_utc": "2026-09-14T08:01:00Z",
            "available_on": "2026-09-14",
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
            "effective_at_utc": "2026-09-14T08:01:01Z",
            "available_on": "2026-09-14",
            "source_version": 1,
        },
    )

    assert isinstance(record, Fee)
    return record


def _refund(
    *,
    refund_id: str,
    payment_id: str,
    amount: str,
) -> Refund:
    record = parse_source_record(
        EntityType.REFUND,
        {
            "refund_id": refund_id,
            "payment_id": payment_id,
            "status": "SUCCEEDED",
            "amount": amount,
            "currency": "EUR",
            "effective_at_utc": "2026-09-15T09:00:00Z",
            "available_on": "2026-09-15",
            "source_version": 1,
        },
    )

    assert isinstance(record, Refund)
    return record


def _missing_report_issues() -> tuple[
    CompletenessIssue,
    ...,
]:
    return (
        CompletenessIssue(
            code=CompletenessIssueCode.DATA_FILE_MISSING,
            message=(
                "data file was not available by evaluation time"
            ),
        ),
        CompletenessIssue(
            code=CompletenessIssueCode.MANIFEST_MISSING,
            message=(
                "manifest was not available by evaluation time"
            ),
        ),
    )


def _completeness_results(
    *,
    overrides: Mapping[
        ReportType,
        SourceCompleteness,
    ] | None = None,
) -> tuple[CompletenessResult, ...]:
    overrides = overrides or {}
    results: list[CompletenessResult] = []

    for report_type in ReportType:
        state = overrides.get(
            report_type,
            SourceCompleteness.COMPLETE,
        )

        source_system = (
            SourceSystem.THREADLINE_SHOP
            if report_type is ReportType.ORDERS
            else SourceSystem.MOCKPAY
        )

        issues = (
            ()
            if state is SourceCompleteness.COMPLETE
            else _missing_report_issues()
        )

        reason = (
            None
            if not issues
            else "; ".join(
                issue.message for issue in issues
            )
        )

        status = SourceReportStatus(
            source_system=source_system,
            report_type=report_type,
            business_date=BUSINESS_DATE,
            state=state,
            batch_id=(
                f"test-{report_type.value}"
                if state is SourceCompleteness.COMPLETE
                else None
            ),
            reason=reason,
        )

        results.append(
            CompletenessResult(
                status=status,
                deadline_at_utc=report_deadline_utc(
                    BUSINESS_DATE,
                    report_type,
                ),
                issues=issues,
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
    *,
    completeness: tuple[
        CompletenessResult,
        ...,
    ] | None = None,
    quarantine: Iterable[QuarantinedRecord] = (),
    detected_at: datetime = AFTER_ALL_DEADLINES,
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
        run_id="test-transaction-anomalies",
        detected_at=detected_at,
        canonicalization=canonicalize(envelopes),
        completeness_results=(
            completeness
            if completeness is not None
            else _completeness_results()
        ),
        quarantine_records=quarantine,
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
# G02: missing payment after complete report
# ------------------------------------------------------------------


def test_g02_missing_payment_after_complete_report():
    result = _run(
        [
            _order(
                order_id="ORD-002",
                amount="89.00",
            )
        ]
    )

    assert len(result.transactions) == 1

    transaction = result.transactions[0]

    assert transaction.order_id == "ORD-002"
    assert transaction.expected_collection == Decimal("89.00")
    assert transaction.captured_total == Decimal("0.00")
    assert transaction.collection_variance == Decimal("-89.00")
    assert transaction.state is ReconciliationState.EXCEPTION
    assert transaction.exception_codes == (
        ExceptionCode.MISSING_PAYMENT,
    )

    missing = _exceptions_of_type(
        result,
        ExceptionCode.MISSING_PAYMENT,
    )

    assert len(missing) == 1
    assert missing[0].entity_id == "ORD-002"
    assert missing[0].expected_amount == Decimal("89.00")
    assert missing[0].actual_amount == Decimal("0.00")
    assert missing[0].variance == Decimal("-89.00")

    # Missing payment does not also emit a redundant amount mismatch.
    assert not _exceptions_of_type(
        result,
        ExceptionCode.PAYMENT_AMOUNT_MISMATCH,
    )


# ------------------------------------------------------------------
# G03: payment report not due yet
# ------------------------------------------------------------------


def test_g03_missing_payment_report_before_deadline_is_pending():
    completeness = _completeness_results(
        overrides={
            ReportType.PAYMENTS: SourceCompleteness.PENDING,
        }
    )

    result = _run(
        [
            _order(
                order_id="ORD-003",
                amount="89.00",
            )
        ],
        completeness=completeness,
        detected_at=BEFORE_PAYMENT_DEADLINE,
    )

    transaction = result.transactions[0]

    assert transaction.captured_total == Decimal("0.00")
    assert transaction.collection_variance == Decimal("-89.00")
    assert transaction.state is ReconciliationState.PENDING
    assert transaction.exception_codes == ()

    assert not _exceptions_of_type(
        result,
        ExceptionCode.MISSING_PAYMENT,
    )
    assert not _exceptions_of_type(
        result,
        ExceptionCode.SOURCE_REPORT_MISSING,
    )

    payment_status = next(
        status
        for status in result.source_completeness
        if status.report_type is ReportType.PAYMENTS
    )

    assert payment_status.state is SourceCompleteness.PENDING
    
    assert result.transactions

    assert all(
        transaction.state is not ReconciliationState.RECONCILED
    for transaction in result.transactions
    )

    assert any(
        status.state is not SourceCompleteness.COMPLETE
        for status in result.source_completeness
    ) 

# ------------------------------------------------------------------
# G04: payment report overdue
# ------------------------------------------------------------------


def test_g04_missing_payment_report_after_deadline_is_incomplete():
    completeness = _completeness_results(
        overrides={
            ReportType.PAYMENTS: (
                SourceCompleteness.INCOMPLETE
            ),
        }
    )

    result = _run(
        [
            _order(
                order_id="ORD-004",
                amount="89.00",
            )
        ],
        completeness=completeness,
        detected_at=AFTER_ALL_DEADLINES,
    )

    transaction = result.transactions[0]

    assert transaction.state is ReconciliationState.INCOMPLETE
    assert transaction.exception_codes == ()

    # Entity-level missing checks are suppressed because the entire
    # source report is incomplete.
    assert not _exceptions_of_type(
        result,
        ExceptionCode.MISSING_PAYMENT,
    )

    source_exceptions = _exceptions_of_type(
        result,
        ExceptionCode.SOURCE_REPORT_MISSING,
    )

    assert len(source_exceptions) == 1
    assert (
        source_exceptions[0].entity_id
        == "mockpay:payments:2026-09-14"
    )

    payment_status = next(
        status
        for status in result.source_completeness
        if status.report_type is ReportType.PAYMENTS
    )

    assert payment_status.state is SourceCompleteness.INCOMPLETE
    
    assert result.transactions

    assert all(
        transaction.state is not ReconciliationState.RECONCILED
    for transaction in result.transactions
    )

    assert any(
        status.state is not SourceCompleteness.COMPLETE
        for status in result.source_completeness
    ) 



# ------------------------------------------------------------------
# G05: captured amount differs from order
# ------------------------------------------------------------------


def test_g05_payment_amount_mismatch():
    result = _run(
        [
            _order(
                order_id="ORD-005",
                amount="120.00",
            ),
            _payment(
                payment_id="PAY-005",
                order_id="ORD-005",
                amount="100.00",
            ),
            # Correct fee prevents G05 from accidentally testing
            # MISSING_FEE as well.
            _fee(
                fee_id="FEE-005",
                payment_id="PAY-005",
                amount="2.00",
            ),
        ]
    )

    transaction = result.transactions[0]

    assert transaction.expected_collection == Decimal("120.00")
    assert transaction.captured_total == Decimal("100.00")
    assert transaction.collection_variance == Decimal("-20.00")
    assert transaction.state is ReconciliationState.EXCEPTION
    assert transaction.exception_codes == (
        ExceptionCode.PAYMENT_AMOUNT_MISMATCH,
    )

    mismatches = _exceptions_of_type(
        result,
        ExceptionCode.PAYMENT_AMOUNT_MISMATCH,
    )

    assert len(mismatches) == 1
    assert mismatches[0].expected_amount == Decimal("120.00")
    assert mismatches[0].actual_amount == Decimal("100.00")
    assert mismatches[0].variance == Decimal("-20.00")


# ------------------------------------------------------------------
# G08: two distinct captures
# ------------------------------------------------------------------


def test_g08_two_distinct_captures_are_not_duplicates():
    result = _run(
        [
            _order(
                order_id="ORD-008",
                amount="100.00",
            ),
            _payment(
                payment_id="PAY-008-A",
                order_id="ORD-008",
                amount="100.00",
                attempt_number=1,
            ),
            _payment(
                payment_id="PAY-008-B",
                order_id="ORD-008",
                amount="100.00",
                attempt_number=2,
            ),
            _fee(
                fee_id="FEE-008-A",
                payment_id="PAY-008-A",
                amount="2.00",
            ),
            _fee(
                fee_id="FEE-008-B",
                payment_id="PAY-008-B",
                amount="2.00",
            ),
        ]
    )

    transaction = result.transactions[0]

    assert transaction.captured_payment_count == 2
    assert transaction.captured_total == Decimal("200.00")
    assert transaction.collection_variance == Decimal("100.00")
    assert transaction.state is ReconciliationState.EXCEPTION

    assert set(transaction.exception_codes) == {
        ExceptionCode.MULTIPLE_CAPTURE,
        ExceptionCode.PAYMENT_AMOUNT_MISMATCH,
    }

    multiple_capture = _exceptions_of_type(
        result,
        ExceptionCode.MULTIPLE_CAPTURE,
    )
    amount_mismatch = _exceptions_of_type(
        result,
        ExceptionCode.PAYMENT_AMOUNT_MISMATCH,
    )

    assert len(multiple_capture) == 1
    assert len(amount_mismatch) == 1

    assert amount_mismatch[0].expected_amount == Decimal("100.00")
    assert amount_mismatch[0].actual_amount == Decimal("200.00")
    assert amount_mismatch[0].variance == Decimal("100.00")


# ------------------------------------------------------------------
# G09: orphan payment
# ------------------------------------------------------------------


def test_g09_payment_referencing_unknown_order_is_orphaned():
    result = _run(
        [
            _payment(
                payment_id="PAY-009",
                order_id="ORD-UNKNOWN",
                amount="75.00",
            ),
            # Card fee for €75:
            # €0.20 + (€75 × 1.8%) = €1.55.
            _fee(
                fee_id="FEE-009",
                payment_id="PAY-009",
                amount="1.55",
            ),
        ]
    )

    assert result.transactions == ()

    orphan_payments = _exceptions_of_type(
        result,
        ExceptionCode.ORPHAN_PAYMENT,
    )

    assert len(orphan_payments) == 1

    exception = orphan_payments[0]

    assert exception.entity_type == "PAYMENT"
    assert exception.entity_id == "PAY-009"
    assert exception.actual_amount == Decimal("75.00")

    # The correct fee keeps this scenario focused on ORPHAN_PAYMENT.
    assert not _exceptions_of_type(
        result,
        ExceptionCode.MISSING_FEE,
    )
    assert not _exceptions_of_type(
        result,
        ExceptionCode.UNEXPECTED_FEE,
    )


# ------------------------------------------------------------------
# G10: valid partial refund
# ------------------------------------------------------------------


def test_g10_valid_partial_refund_updates_lifetime_net():
    result = _run(
        [
            _order(
                order_id="ORD-010",
                amount="100.00",
            ),
            _payment(
                payment_id="PAY-010",
                order_id="ORD-010",
                amount="100.00",
            ),
            _fee(
                fee_id="FEE-010",
                payment_id="PAY-010",
                amount="2.00",
            ),
            _refund(
                refund_id="REF-010",
                payment_id="PAY-010",
                amount="30.00",
            ),
        ]
    )

    transaction = result.transactions[0]

    assert transaction.captured_total == Decimal("100.00")
    assert transaction.successful_refund_total == Decimal("30.00")
    assert transaction.expected_fee_total == Decimal("2.00")
    assert transaction.reported_fee_total == Decimal("2.00")
    assert transaction.lifetime_net_collection == Decimal("68.00")
    assert transaction.successful_refund_count == 1
    assert transaction.state is ReconciliationState.RECONCILED
    assert transaction.exception_codes == ()

    assert not _exceptions_of_type(
        result,
        ExceptionCode.ORPHAN_REFUND,
    )
    assert not _exceptions_of_type(
        result,
        ExceptionCode.EXCESS_REFUND,
    )


# ------------------------------------------------------------------
# G11: orphan refund
# ------------------------------------------------------------------


def test_g11_refund_referencing_unknown_payment_is_orphaned():
    result = _run(
        [
            _refund(
                refund_id="REF-011",
                payment_id="PAY-UNKNOWN",
                amount="55.00",
            )
        ]
    )

    orphan_refunds = _exceptions_of_type(
        result,
        ExceptionCode.ORPHAN_REFUND,
    )

    assert len(orphan_refunds) == 1

    exception = orphan_refunds[0]

    assert exception.entity_type == "REFUND"
    assert exception.entity_id == "REF-011"
    assert exception.actual_amount == Decimal("55.00")
    assert exception.expected_amount is None
    assert exception.variance is None


# ------------------------------------------------------------------
# G12: cumulative refunds exceed capture
# ------------------------------------------------------------------


def test_g12_refunds_exceed_captured_amount():
    result = _run(
        [
            _order(
                order_id="ORD-012",
                amount="100.00",
            ),
            _payment(
                payment_id="PAY-012",
                order_id="ORD-012",
                amount="100.00",
            ),
            _fee(
                fee_id="FEE-012",
                payment_id="PAY-012",
                amount="2.00",
            ),
            _refund(
                refund_id="REF-012-A",
                payment_id="PAY-012",
                amount="60.00",
            ),
            _refund(
                refund_id="REF-012-B",
                payment_id="PAY-012",
                amount="50.00",
            ),
        ]
    )

    transaction = result.transactions[0]

    assert transaction.successful_refund_count == 2
    assert transaction.successful_refund_total == Decimal("110.00")
    assert transaction.lifetime_net_collection == Decimal("-12.00")
    assert transaction.state is ReconciliationState.EXCEPTION
    assert transaction.exception_codes == (
        ExceptionCode.EXCESS_REFUND,
    )

    excess_refunds = _exceptions_of_type(
        result,
        ExceptionCode.EXCESS_REFUND,
    )

    assert len(excess_refunds) == 1

    exception = excess_refunds[0]

    assert exception.entity_type == "PAYMENT"
    assert exception.entity_id == "PAY-012"
    assert exception.expected_amount == Decimal("100.00")
    assert exception.actual_amount == Decimal("110.00")
    assert exception.variance == Decimal("10.00")


# ------------------------------------------------------------------
# G19: malformed amount is quarantined
# ------------------------------------------------------------------


def test_g19_malformed_amount_is_quarantined():
    malformed_payment = {
        "payment_id": "PAY-019",
        "order_id": "ORD-019",
        "attempt_number": 1,
        "payment_method": "CARD",
        "status": "CAPTURED",
        "amount": "one hundred",
        "currency": "EUR",
        "effective_at_utc": "2026-09-14T08:01:00Z",
        "available_on": "2026-09-14",
        "source_version": 1,
    }

    try:
        parse_source_record(
            EntityType.PAYMENT,
            malformed_payment,
        )
    except ContractViolation as violation:
        quarantined = quarantine_from_violation(
            EntityType.PAYMENT,
            malformed_payment,
            violation,
        )
    else:
        raise AssertionError(
            "malformed payment was unexpectedly accepted"
        )

    result = _run(
        [],
        quarantine=[quarantined],
    )

    assert result.transactions == ()
    assert result.payouts == ()
    assert result.exceptions == ()
    assert len(result.quarantine_records) == 1

    rejected = result.quarantine_records[0]

    assert rejected.entity_type == "PAYMENT"
    assert rejected.source_id == "PAY-019"
    assert rejected.reason_code == "INVALID_AMOUNT"
    assert "one hundred" in rejected.raw_payload_json