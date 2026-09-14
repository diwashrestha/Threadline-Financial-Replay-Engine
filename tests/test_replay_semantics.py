"""Replay-safety scenarios for Threadline.

G06: identical logical duplicate
G07: conflicting payloads at the same version
G17: higher-version correction
G18: late refund, exact replay, and incremental/full-rebuild equivalence
"""

from __future__ import annotations

import random
from datetime import date, datetime, timezone
from decimal import Decimal

from threadline.canonicalize import (
    CanonicalizationResult,
    RecordEnvelope,
    ReplayLedger,
    canonicalize,
    conflict_exceptions,
)
from threadline.completeness import (
    CompletenessResult,
    report_deadline_utc,
)
from threadline.contracts import (
    EntityType,
    EvidenceDisposition,
    ExceptionCode,
    Fee,
    FinancialRecord,
    Order,
    Payment,
    Payout,
    ReconciliationState,
    Refund,
    ReportType,
    SettlementLine,
    SourceCompleteness,
    SourceReportStatus,
    SourceSystem,
    parse_source_record,
)
from threadline.reconcile import (
    ReconciliationResult,
    reconcile,
)


BUSINESS_DATE = date(2026, 9, 14)
DETECTED_AT = datetime(
    2026,
    9,
    15,
    10,
    30,
    tzinfo=timezone.utc,
)


def _parse(
    entity_type: EntityType,
    payload: dict,
) -> FinancialRecord:
    return parse_source_record(
        entity_type,
        payload,
    )


def _order(
    *,
    order_id: str = "order-1001",
    amount: str = "100.00",
    source_version: int = 1,
) -> Order:
    record = _parse(
        EntityType.ORDER,
        {
            "order_id": order_id,
            "created_at_utc": "2026-09-14T08:00:00Z",
            "status": "PAID",
            "currency": "EUR",
            "order_total": amount,
            "source_version": source_version,
        },
    )

    assert isinstance(record, Order)
    return record


def _payment(
    *,
    payment_id: str = "pay-1001",
    order_id: str = "order-1001",
    amount: str = "100.00",
    source_version: int = 1,
) -> Payment:
    record = _parse(
        EntityType.PAYMENT,
        {
            "payment_id": payment_id,
            "order_id": order_id,
            "attempt_number": 1,
            "payment_method": "CARD",
            "status": "CAPTURED",
            "amount": amount,
            "currency": "EUR",
            "effective_at_utc": "2026-09-14T08:01:00Z",
            "available_on": "2026-09-14",
            "source_version": source_version,
        },
    )

    assert isinstance(record, Payment)
    return record


def _fee(
    *,
    fee_id: str = "fee-1001",
    payment_id: str = "pay-1001",
    amount: str = "2.00",
    source_version: int = 1,
) -> Fee:
    record = _parse(
        EntityType.FEE,
        {
            "fee_id": fee_id,
            "payment_id": payment_id,
            "fee_type": "PROCESSING",
            "amount": amount,
            "currency": "EUR",
            "effective_at_utc": "2026-09-14T08:01:01Z",
            "available_on": "2026-09-14",
            "source_version": source_version,
        },
    )

    assert isinstance(record, Fee)
    return record


def _refund(
    *,
    refund_id: str = "refund-1001",
    payment_id: str = "pay-1001",
    amount: str = "30.00",
    source_version: int = 1,
) -> Refund:
    record = _parse(
        EntityType.REFUND,
        {
            "refund_id": refund_id,
            "payment_id": payment_id,
            "status": "SUCCEEDED",
            "amount": amount,
            "currency": "EUR",
            "effective_at_utc": "2026-09-15T09:00:00Z",
            "available_on": "2026-09-15",
            "source_version": source_version,
        },
    )

    assert isinstance(record, Refund)
    return record


def _settlement_line(
    *,
    line_id: str,
    movement_type: str,
    movement_id: str,
    signed_amount: str,
    source_version: int = 1,
) -> SettlementLine:
    record = _parse(
        EntityType.SETTLEMENT_LINE,
        {
            "settlement_line_id": line_id,
            "payout_id": "payout-2026-09-14",
            "movement_type": movement_type,
            "movement_id": movement_id,
            "signed_amount": signed_amount,
            "currency": "EUR",
            "source_version": source_version,
        },
    )

    assert isinstance(record, SettlementLine)
    return record


def _payout(
    *,
    payout_id: str = "payout-2026-09-14",
    amount: str = "98.00",
    source_version: int = 1,
) -> Payout:
    record = _parse(
        EntityType.PAYOUT,
        {
            "payout_id": payout_id,
            "payout_date": "2026-09-14",
            "currency": "EUR",
            "reported_net_amount": amount,
            "source_version": source_version,
        },
    )

    assert isinstance(record, Payout)
    return record


def _envelope(
    receipt_id: str,
    record: FinancialRecord,
) -> RecordEnvelope:
    return RecordEnvelope(
        receipt_id=receipt_id,
        record=record,
    )


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
            batch_id=(
                f"test-{report_type.value}-"
                f"{BUSINESS_DATE.isoformat()}"
            ),
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


def _run_reconciliation(
    canonicalization: CanonicalizationResult,
) -> ReconciliationResult:
    return reconcile(
        run_id="test-replay-semantics",
        detected_at=DETECTED_AT,
        canonicalization=canonicalization,
        completeness_results=_complete_reports(),
    )


# ------------------------------------------------------------------
# G06: identical duplicate
# ------------------------------------------------------------------


def test_g06_identical_duplicate_does_not_change_canonical_value():
    payment = _payment()

    first_receipt = _envelope(
        "payments-original.json:1",
        payment,
    )
    duplicate_receipt = _envelope(
        "payments-redelivery.json:1",
        payment,
    )

    result = canonicalize(
        [
            first_receipt,
            duplicate_receipt,
        ]
    )

    assert result.canonical_records == (payment,)
    assert result.accepted_count == 1
    assert result.duplicate_count == 1
    assert result.stale_count == 0
    assert result.conflicted_count == 0
    assert result.conflicts == ()

    evidence_by_receipt = {
        item.receipt_id: item.disposition
        for item in result.evidence
    }

    # The lexicographically smallest receipt becomes the deterministic
    # representative. This does not depend on input order.
    assert evidence_by_receipt[
        "payments-original.json:1"
    ] is EvidenceDisposition.ACCEPTED

    assert evidence_by_receipt[
        "payments-redelivery.json:1"
    ] is EvidenceDisposition.DUPLICATE

    reverse_result = canonicalize(
        [
            duplicate_receipt,
            first_receipt,
        ]
    )

    assert reverse_result == result


# ------------------------------------------------------------------
# G07: conflicting payloads at the same version
# ------------------------------------------------------------------


def test_g07_conflicting_same_version_is_excluded():
    payment_100 = _payment(
        amount="100.00",
        source_version=1,
    )
    payment_95 = _payment(
        amount="95.00",
        source_version=1,
    )

    first = _envelope(
        "payments-a.json:1",
        payment_100,
    )
    conflicting = _envelope(
        "payments-b.json:1",
        payment_95,
    )

    result = canonicalize(
        [
            first,
            conflicting,
        ]
    )

    # Neither payload is safe to use financially.
    assert result.canonical_records == ()
    assert result.accepted_count == 0
    assert result.duplicate_count == 0
    assert result.stale_count == 0
    assert result.conflicted_count == 2

    assert len(result.conflicts) == 1

    conflict = result.conflicts[0]

    assert conflict.identity == (
        "mockpay",
        "PAYMENT",
        "pay-1001",
    )
    assert conflict.source_version == 1
    assert len(conflict.payload_hashes) == 2
    assert conflict.receipt_ids == (
        "payments-a.json:1",
        "payments-b.json:1",
    )

    exceptions = conflict_exceptions(result)

    assert len(exceptions) == 1
    assert (
        exceptions[0].code
        is ExceptionCode.CONFLICTING_SOURCE_VERSION
    )
    assert exceptions[0].entity_type == "PAYMENT"
    assert exceptions[0].entity_id == "pay-1001"

    # Arrival order must not choose a different winner.
    reverse_result = canonicalize(
        [
            conflicting,
            first,
        ]
    )

    assert reverse_result == result


# ------------------------------------------------------------------
# G17: higher-version correction
# ------------------------------------------------------------------


def test_g17_higher_version_correction_supersedes_old_value():
    original = _payment(
        amount="90.00",
        source_version=1,
    )
    correction = _payment(
        amount="100.00",
        source_version=2,
    )

    original_receipt = _envelope(
        "payments-original.json:1",
        original,
    )
    correction_receipt = _envelope(
        "payments-correction.json:1",
        correction,
    )

    full_rebuild = canonicalize(
        [
            original_receipt,
            correction_receipt,
        ]
    )

    assert full_rebuild.canonical_records == (
        correction,
    )
    assert full_rebuild.accepted_count == 1
    assert full_rebuild.stale_count == 1
    assert full_rebuild.duplicate_count == 0
    assert full_rebuild.conflicted_count == 0

    evidence_by_receipt = {
        item.receipt_id: item.disposition
        for item in full_rebuild.evidence
    }

    assert evidence_by_receipt[
        "payments-original.json:1"
    ] is EvidenceDisposition.STALE

    assert evidence_by_receipt[
        "payments-correction.json:1"
    ] is EvidenceDisposition.ACCEPTED

    # Simulate the correction arriving before the stale original.
    ledger = ReplayLedger()

    assert ledger.apply([correction_receipt]) == 1
    assert ledger.apply([original_receipt]) == 1

    incremental = ledger.result()

    assert incremental == full_rebuild
    assert incremental.canonical_records[0].amount == Decimal(
        "100.00"
    )

    # Replaying the old record cannot roll the value backwards.
    assert ledger.apply([original_receipt]) == 0
    assert ledger.result() == full_rebuild


# ------------------------------------------------------------------
# G18: late refund and duplicate replay
# ------------------------------------------------------------------


def _g18_receipts() -> tuple[RecordEnvelope, ...]:
    return (
        _envelope(
            "orders.json:1",
            _order(),
        ),
        _envelope(
            "payments.json:1",
            _payment(),
        ),
        _envelope(
            "fees.json:1",
            _fee(),
        ),
        _envelope(
            "settlements.json:1",
            _settlement_line(
                line_id="line-1001-capture",
                movement_type="CAPTURE",
                movement_id="pay-1001",
                signed_amount="100.00",
            ),
        ),
        _envelope(
            "settlements.json:2",
            _settlement_line(
                line_id="line-1001-fee",
                movement_type="FEE",
                movement_id="fee-1001",
                signed_amount="-2.00",
            ),
        ),
        _envelope(
            "payouts.json:1",
            _payout(),
        ),
        _envelope(
            "late-refunds.json:1",
            _refund(),
        ),
    )


def test_g18_late_refund_and_duplicate_replay_are_safe():
    all_receipts = _g18_receipts()

    refund_receipt = next(
        receipt
        for receipt in all_receipts
        if receipt.record.ENTITY_TYPE
        is EntityType.REFUND
    )

    initial_receipts = tuple(
        receipt
        for receipt in all_receipts
        if receipt is not refund_receipt
    )

    ledger = ReplayLedger()

    # Initial daily load has no refund.
    assert ledger.apply(initial_receipts) == 6

    before_late_refund = _run_reconciliation(
        ledger.result()
    )

    initial_transaction = (
        before_late_refund.transactions[0]
    )
    initial_payout = before_late_refund.payouts[0]

    assert (
        initial_transaction.lifetime_net_collection
        == Decimal("98.00")
    )
    assert initial_payout.expected_payout == Decimal(
        "98.00"
    )
    assert initial_payout.reported_net_amount == Decimal(
        "98.00"
    )
    assert (
        initial_payout.state
        is ReconciliationState.RECONCILED
    )

    # The €30 refund arrives later. Its available_on date is the
    # following day, so it changes lifetime transaction value without
    # rewriting payout A.
    assert ledger.apply([refund_receipt]) == 1

    # Exact replay of all previously seen physical receipts is a no-op.
    assert ledger.apply(all_receipts) == 0
    assert len(ledger) == 7

    incremental_canonicalization = ledger.result()
    full_rebuild_canonicalization = canonicalize(
        all_receipts
    )

    assert (
        incremental_canonicalization
        == full_rebuild_canonicalization
    )

    incremental_result = _run_reconciliation(
        incremental_canonicalization
    )
    full_rebuild_result = _run_reconciliation(
        full_rebuild_canonicalization
    )

    # Threadline's main replay invariant.
    assert incremental_result == full_rebuild_result

    transaction = incremental_result.transactions[0]
    payout_a = incremental_result.payouts[0]

    assert (
        transaction.successful_refund_total
        == Decimal("30.00")
    )
    assert (
        transaction.lifetime_net_collection
        == Decimal("68.00")
    )

    # Payout A remains 100 capture - 2 fee = 98.
    assert payout_a.expected_payout == Decimal("98.00")
    assert payout_a.reported_line_total == Decimal("98.00")
    assert payout_a.reported_net_amount == Decimal("98.00")
    assert (
        payout_a.end_to_end_payout_variance
        == Decimal("0.00")
    )
    assert payout_a.state is ReconciliationState.RECONCILED

    refund_movements = [
        movement
        for movement in incremental_result.expected_movements
        if movement.movement_type.value == "REFUND"
    ]

    assert len(refund_movements) == 1
    assert refund_movements[0].movement_id == "refund-1001"
    assert refund_movements[0].available_on == date(
        2026,
        9,
        15,
    )
    assert refund_movements[0].signed_amount == Decimal(
        "-30.00"
    )

    assert incremental_result.exceptions == ()

    # A deterministic shuffle must produce the same canonical state.
    shuffled = list(all_receipts)
    random.Random(42).shuffle(shuffled)

    shuffled_result = _run_reconciliation(
        canonicalize(shuffled)
    )

    assert shuffled_result == full_rebuild_result