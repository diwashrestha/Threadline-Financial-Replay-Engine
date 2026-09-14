"""Deterministic financial reconciliation for Threadline.

This module consumes:

- canonical financial records;
- source-report completeness results;
- quarantined source evidence.

It produces:

- transaction reconciliation rows;
- expected payout movements;
- payout reconciliation rows;
- auditable financial exceptions.

The module performs no database or Airflow operations.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Final

from threadline.canonicalize import CanonicalizationResult
from threadline.completeness import (
    CompletenessIssueCode,
    CompletenessResult,
)
from threadline.contracts import (
    CONTRACT_VERSION,
    EntityType,
    EvidenceDisposition,
    ExceptionCode,
    Fee,
    Order,
    OrderStatus,
    Payment,
    PaymentStatus,
    Payout,
    QuarantinedRecord,
    ReconciliationState,
    Refund,
    RefundStatus,
    ReportType,
    SettlementLine,
    MovementType,
    SourceCompleteness,
    SourceReportStatus,
)
from threadline.money import calculate_fee


ZERO: Final = Decimal("0.00")

TRANSACTION_REQUIRED_REPORTS: Final = frozenset(
    {
        ReportType.ORDERS,
        ReportType.PAYMENTS,
        ReportType.REFUNDS,
        ReportType.FEES,
    }
)

PAYOUT_REQUIRED_REPORTS: Final = frozenset(
    {
        ReportType.PAYMENTS,
        ReportType.REFUNDS,
        ReportType.FEES,
        ReportType.SETTLEMENT_LINES,
        ReportType.PAYOUTS,
    }
)

_MISSING_COMPLETENESS_ISSUES: Final = frozenset(
    {
        CompletenessIssueCode.DATA_FILE_MISSING,
        CompletenessIssueCode.MANIFEST_MISSING,
    }
)


class ReconciliationError(ValueError):
    """Raised when the reconciler receives inconsistent run inputs."""


@dataclass(frozen=True, slots=True)
class ExpectedMovement:
    movement_type: MovementType
    movement_id: str
    available_on: date
    signed_amount: Decimal
    supporting_source_record_ids: tuple[str, ...]

    @property
    def movement_key(self) -> tuple[MovementType, str]:
        return self.movement_type, self.movement_id

    @property
    def sort_key(self) -> tuple[str, str, str]:
        return (
            self.available_on.isoformat(),
            self.movement_type.value,
            self.movement_id,
        )


@dataclass(frozen=True, slots=True)
class ReconciliationException:
    run_id: str
    contract_version: str
    rule_id: str
    exception_type: ExceptionCode
    entity_type: str
    entity_id: str
    expected_amount: Decimal | None
    actual_amount: Decimal | None
    variance: Decimal | None
    detected_at_utc: datetime
    status: str
    supporting_source_record_ids: tuple[str, ...]

    @property
    def sort_key(self) -> tuple[str, str, str, str]:
        return (
            self.exception_type.value,
            self.entity_type,
            self.entity_id,
            self.rule_id,
        )


@dataclass(frozen=True, slots=True)
class TransactionReconciliation:
    run_id: str
    contract_version: str
    order_id: str
    order_status: OrderStatus
    currency: str

    expected_collection: Decimal
    captured_total: Decimal
    collection_variance: Decimal

    successful_refund_total: Decimal
    expected_fee_total: Decimal
    reported_fee_total: Decimal
    lifetime_net_collection: Decimal

    captured_payment_count: int
    successful_refund_count: int

    state: ReconciliationState
    exception_codes: tuple[ExceptionCode, ...]

    @property
    def sort_key(self) -> str:
        return self.order_id


@dataclass(frozen=True, slots=True)
class PayoutReconciliation:
    run_id: str
    contract_version: str
    payout_id: str
    payout_date: date
    currency: str

    expected_payout: Decimal
    reported_line_total: Decimal
    reported_net_amount: Decimal

    provider_report_variance: Decimal
    end_to_end_payout_variance: Decimal

    expected_movement_count: int
    settlement_line_count: int

    state: ReconciliationState
    exception_codes: tuple[ExceptionCode, ...]

    @property
    def sort_key(self) -> tuple[str, str]:
        return self.payout_date.isoformat(), self.payout_id


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    run_id: str
    contract_version: str
    detected_at_utc: datetime

    transactions: tuple[TransactionReconciliation, ...]
    expected_movements: tuple[ExpectedMovement, ...]
    payouts: tuple[PayoutReconciliation, ...]
    exceptions: tuple[ReconciliationException, ...]
    source_completeness: tuple[SourceReportStatus, ...]
    quarantine_records: tuple[QuarantinedRecord, ...]


def _require_run_id(run_id: str) -> str:
    if (
        not isinstance(run_id, str)
        or not run_id
        or run_id != run_id.strip()
    ):
        raise ReconciliationError(
            "run_id must be a non-empty canonical string"
        )

    return run_id


def _as_utc(value: datetime, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ReconciliationError(
            f"{field_name} must be a datetime"
        )

    if value.tzinfo is None or value.utcoffset() is None:
        raise ReconciliationError(
            f"{field_name} must contain an explicit UTC offset"
        )

    return value.astimezone(timezone.utc)


def _sum_money(values: Iterable[Decimal]) -> Decimal:
    return sum(values, ZERO)


def _source_reference(record: object) -> str:
    entity_type = getattr(record, "ENTITY_TYPE")
    record_id = getattr(record, "record_id")
    return f"{entity_type.value}:{record_id}"


def _build_report_state_map(
    completeness_results: Iterable[CompletenessResult],
) -> dict[ReportType, SourceCompleteness]:
    states: dict[ReportType, SourceCompleteness] = {}

    for result in completeness_results:
        report_type = result.status.report_type

        if report_type in states:
            raise ReconciliationError(
                f"duplicate completeness result for "
                f"{report_type.value}"
            )

        states[report_type] = result.status.state

    missing = set(ReportType) - set(states)

    if missing:
        names = ", ".join(
            report_type.value
            for report_type in sorted(
                missing,
                key=lambda item: item.value,
            )
        )
        raise ReconciliationError(
            f"missing completeness results for: {names}"
        )

    return states


def _reports_complete(
    states: Mapping[ReportType, SourceCompleteness],
    required_reports: Iterable[ReportType],
) -> bool:
    return all(
        states[report_type] is SourceCompleteness.COMPLETE
        for report_type in required_reports
    )


def _result_state(
    *,
    report_states: Mapping[ReportType, SourceCompleteness],
    required_reports: Iterable[ReportType],
    has_blocking_exception: bool,
) -> ReconciliationState:
    required_states = {
        report_states[report_type]
        for report_type in required_reports
    }

    if SourceCompleteness.INCOMPLETE in required_states:
        return ReconciliationState.INCOMPLETE

    if SourceCompleteness.PENDING in required_states:
        return ReconciliationState.PENDING

    if has_blocking_exception:
        return ReconciliationState.EXCEPTION

    return ReconciliationState.RECONCILED


def _new_exception(
    *,
    run_id: str,
    detected_at_utc: datetime,
    rule_id: str,
    exception_type: ExceptionCode,
    entity_type: str,
    entity_id: str,
    expected_amount: Decimal | None = None,
    actual_amount: Decimal | None = None,
    supporting_source_record_ids: Iterable[str] = (),
) -> ReconciliationException:
    variance: Decimal | None = None

    if (
        expected_amount is not None
        and actual_amount is not None
    ):
        variance = actual_amount - expected_amount

    return ReconciliationException(
        run_id=run_id,
        contract_version=CONTRACT_VERSION,
        rule_id=rule_id,
        exception_type=exception_type,
        entity_type=entity_type,
        entity_id=entity_id,
        expected_amount=expected_amount,
        actual_amount=actual_amount,
        variance=variance,
        detected_at_utc=detected_at_utc,
        status="OPEN",
        supporting_source_record_ids=tuple(
            sorted(set(supporting_source_record_ids))
        ),
    )


def _source_completeness_exceptions(
    *,
    run_id: str,
    detected_at_utc: datetime,
    completeness_results: Iterable[CompletenessResult],
) -> list[ReconciliationException]:
    exceptions: list[ReconciliationException] = []

    for result in completeness_results:
        if result.status.state is not SourceCompleteness.INCOMPLETE:
            continue

        issue_codes = {
            issue.code for issue in result.issues
        }

        only_missing = issue_codes.issubset(
            _MISSING_COMPLETENESS_ISSUES
        )

        exception_type = (
            ExceptionCode.SOURCE_REPORT_MISSING
            if only_missing
            else ExceptionCode.SOURCE_REPORT_INVALID
        )

        status = result.status
        entity_id = (
            f"{status.source_system.value}:"
            f"{status.report_type.value}:"
            f"{status.business_date.isoformat()}"
        )

        exceptions.append(
            _new_exception(
                run_id=run_id,
                detected_at_utc=detected_at_utc,
                rule_id="source.report_completeness",
                exception_type=exception_type,
                entity_type="SOURCE_REPORT",
                entity_id=entity_id,
                supporting_source_record_ids=(
                    (status.batch_id,)
                    if status.batch_id
                    else ()
                ),
            )
        )

    return exceptions


def _canonical_conflict_exceptions(
    *,
    run_id: str,
    detected_at_utc: datetime,
    canonicalization: CanonicalizationResult,
) -> list[ReconciliationException]:
    exceptions: list[ReconciliationException] = []

    for conflict in canonicalization.conflicts:
        exceptions.append(
            _new_exception(
                run_id=run_id,
                detected_at_utc=detected_at_utc,
                rule_id="canonicalization.same_version_conflict",
                exception_type=(
                    ExceptionCode.CONFLICTING_SOURCE_VERSION
                ),
                entity_type=conflict.identity[1],
                entity_id=conflict.identity[2],
                supporting_source_record_ids=(
                    conflict.receipt_ids
                ),
            )
        )

    return exceptions


def _split_canonical_records(
    canonicalization: CanonicalizationResult,
) -> tuple[
    list[Order],
    list[Payment],
    list[Refund],
    list[Fee],
    list[SettlementLine],
    list[Payout],
]:
    orders: list[Order] = []
    payments: list[Payment] = []
    refunds: list[Refund] = []
    fees: list[Fee] = []
    settlement_lines: list[SettlementLine] = []
    payouts: list[Payout] = []

    for record in canonicalization.canonical_records:
        if isinstance(record, Order):
            orders.append(record)
        elif isinstance(record, Payment):
            payments.append(record)
        elif isinstance(record, Refund):
            refunds.append(record)
        elif isinstance(record, Fee):
            fees.append(record)
        elif isinstance(record, SettlementLine):
            settlement_lines.append(record)
        elif isinstance(record, Payout):
            payouts.append(record)
        else:
            raise ReconciliationError(
                f"unsupported canonical record: "
                f"{type(record).__name__}"
            )

    orders.sort(key=lambda record: record.order_id)
    payments.sort(key=lambda record: record.payment_id)
    refunds.sort(key=lambda record: record.refund_id)
    fees.sort(key=lambda record: record.fee_id)
    settlement_lines.sort(
        key=lambda record: record.settlement_line_id
    )
    payouts.sort(
        key=lambda record: (
            record.payout_date,
            record.payout_id,
        )
    )

    return (
        orders,
        payments,
        refunds,
        fees,
        settlement_lines,
        payouts,
    )


def reconcile(
    *,
    run_id: str,
    detected_at: datetime,
    canonicalization: CanonicalizationResult,
    completeness_results: Iterable[CompletenessResult],
    quarantine_records: Iterable[QuarantinedRecord] = (),
) -> ReconciliationResult:
    """Run deterministic transaction and payout reconciliation."""

    run_id = _require_run_id(run_id)
    detected_at_utc = _as_utc(
        detected_at,
        field_name="detected_at",
    )

    completeness_results = tuple(
        sorted(
            completeness_results,
            key=lambda result: result.sort_key,
        )
    )
    report_states = _build_report_state_map(
        completeness_results
    )

    (
        orders,
        payments,
        refunds,
        fees,
        settlement_lines,
        payouts,
    ) = _split_canonical_records(canonicalization)

    exceptions: list[ReconciliationException] = []
    transaction_codes: dict[
        str,
        set[ExceptionCode],
    ] = defaultdict(set)
    payout_codes: dict[
        str,
        set[ExceptionCode],
    ] = defaultdict(set)

    exceptions.extend(
        _source_completeness_exceptions(
            run_id=run_id,
            detected_at_utc=detected_at_utc,
            completeness_results=completeness_results,
        )
    )
    exceptions.extend(
        _canonical_conflict_exceptions(
            run_id=run_id,
            detected_at_utc=detected_at_utc,
            canonicalization=canonicalization,
        )
    )

    orders_by_id = {
        order.order_id: order
        for order in orders
    }
    payments_by_id = {
        payment.payment_id: payment
        for payment in payments
    }

    captured_payments = [
        payment
        for payment in payments
        if payment.status is PaymentStatus.CAPTURED
    ]

    captured_by_order: dict[str, list[Payment]] = defaultdict(list)

    for payment in captured_payments:
        captured_by_order[payment.order_id].append(payment)

    successful_refunds = [
        refund
        for refund in refunds
        if refund.status is RefundStatus.SUCCEEDED
    ]

    successful_refunds_by_payment: dict[
        str,
        list[Refund],
    ] = defaultdict(list)

    for refund in successful_refunds:
        successful_refunds_by_payment[
            refund.payment_id
        ].append(refund)

    fees_by_payment: dict[str, list[Fee]] = defaultdict(list)

    for fee in fees:
        fees_by_payment[fee.payment_id].append(fee)
        
    fees_by_id = {
        fee.fee_id: fee
        for fee in fees
        }

    # ---------------------------------------------------------
    # Orphan and non-payable payment rules
    # ---------------------------------------------------------

    if _reports_complete(
        report_states,
        {ReportType.ORDERS, ReportType.PAYMENTS},
    ):
        for payment in captured_payments:
            order = orders_by_id.get(payment.order_id)

            if order is None:
                exceptions.append(
                    _new_exception(
                        run_id=run_id,
                        detected_at_utc=detected_at_utc,
                        rule_id="payment.order_exists",
                        exception_type=ExceptionCode.ORPHAN_PAYMENT,
                        entity_type=EntityType.PAYMENT.value,
                        entity_id=payment.payment_id,
                        actual_amount=payment.amount,
                        supporting_source_record_ids=(
                            _source_reference(payment),
                        ),
                    )
                )
                continue

            if order.status is OrderStatus.CANCELLED:
                code = (
                    ExceptionCode
                    .CAPTURE_FOR_NON_PAYABLE_ORDER
                )
                transaction_codes[order.order_id].add(code)

                exceptions.append(
                    _new_exception(
                        run_id=run_id,
                        detected_at_utc=detected_at_utc,
                        rule_id="payment.order_is_payable",
                        exception_type=code,
                        entity_type=EntityType.ORDER.value,
                        entity_id=order.order_id,
                        expected_amount=ZERO,
                        actual_amount=payment.amount,
                        supporting_source_record_ids=(
                            _source_reference(order),
                            _source_reference(payment),
                        ),
                    )
                )

    # ---------------------------------------------------------
    # Refund integrity
    # ---------------------------------------------------------

    if _reports_complete(
        report_states,
        {ReportType.PAYMENTS, ReportType.REFUNDS},
    ):
        for refund in successful_refunds:
            payment = payments_by_id.get(refund.payment_id)

            if (
                payment is None
                or payment.status is not PaymentStatus.CAPTURED
            ):
                exceptions.append(
                    _new_exception(
                        run_id=run_id,
                        detected_at_utc=detected_at_utc,
                        rule_id="refund.captured_payment_exists",
                        exception_type=ExceptionCode.ORPHAN_REFUND,
                        entity_type=EntityType.REFUND.value,
                        entity_id=refund.refund_id,
                        actual_amount=refund.amount,
                        supporting_source_record_ids=(
                            _source_reference(refund),
                        ),
                    )
                )
                continue

            if refund.currency != payment.currency:
                code = (
                    ExceptionCode.REFUND_CURRENCY_MISMATCH
                )
                transaction_codes[payment.order_id].add(code)

                exceptions.append(
                    _new_exception(
                        run_id=run_id,
                        detected_at_utc=detected_at_utc,
                        rule_id="refund.currency_matches_payment",
                        exception_type=code,
                        entity_type=EntityType.REFUND.value,
                        entity_id=refund.refund_id,
                        supporting_source_record_ids=(
                            _source_reference(payment),
                            _source_reference(refund),
                        ),
                    )
                )

        for payment in captured_payments:
            related_refunds = successful_refunds_by_payment.get(
                payment.payment_id,
                [],
            )
            refund_total = _sum_money(
                refund.amount
                for refund in related_refunds
            )

            if refund_total > payment.amount:
                code = ExceptionCode.EXCESS_REFUND
                transaction_codes[payment.order_id].add(code)

                exceptions.append(
                    _new_exception(
                        run_id=run_id,
                        detected_at_utc=detected_at_utc,
                        rule_id="refund.total_within_capture",
                        exception_type=code,
                        entity_type=EntityType.PAYMENT.value,
                        entity_id=payment.payment_id,
                        expected_amount=payment.amount,
                        actual_amount=refund_total,
                        supporting_source_record_ids=(
                            _source_reference(payment),
                            *(
                                _source_reference(refund)
                                for refund in related_refunds
                            ),
                        ),
                    )
                )

    # ---------------------------------------------------------
    # Fee integrity
    # ---------------------------------------------------------

    expected_fee_by_payment: dict[str, Decimal] = {}

    for payment in captured_payments:
        expected_fee_by_payment[payment.payment_id] = (
            calculate_fee(
                payment.payment_method.value,
                payment.amount,
                currency=payment.currency,
            )
        )

    if _reports_complete(
        report_states,
        {ReportType.PAYMENTS, ReportType.FEES},
    ):
        for payment in captured_payments:
            related_fees = fees_by_payment.get(
                payment.payment_id,
                [],
            )
            expected_fee = expected_fee_by_payment[
                payment.payment_id
            ]
            reported_fee = _sum_money(
                fee.amount for fee in related_fees
            )

            if not related_fees:
                code = ExceptionCode.MISSING_FEE
                transaction_codes[payment.order_id].add(code)

                exceptions.append(
                    _new_exception(
                        run_id=run_id,
                        detected_at_utc=detected_at_utc,
                        rule_id="fee.processing_fee_exists",
                        exception_type=code,
                        entity_type=EntityType.PAYMENT.value,
                        entity_id=payment.payment_id,
                        expected_amount=expected_fee,
                        actual_amount=ZERO,
                        supporting_source_record_ids=(
                            _source_reference(payment),
                        ),
                    )
                )
            elif reported_fee != expected_fee:
                code = ExceptionCode.FEE_MISMATCH
                transaction_codes[payment.order_id].add(code)

                exceptions.append(
                    _new_exception(
                        run_id=run_id,
                        detected_at_utc=detected_at_utc,
                        rule_id="fee.amount_matches_schedule",
                        exception_type=code,
                        entity_type=EntityType.PAYMENT.value,
                        entity_id=payment.payment_id,
                        expected_amount=expected_fee,
                        actual_amount=reported_fee,
                        supporting_source_record_ids=(
                            _source_reference(payment),
                            *(
                                _source_reference(fee)
                                for fee in related_fees
                            ),
                        ),
                    )
                )

        for fee in fees:
            payment = payments_by_id.get(fee.payment_id)

            if (
                payment is None
                or payment.status is not PaymentStatus.CAPTURED
            ):
                exceptions.append(
                    _new_exception(
                        run_id=run_id,
                        detected_at_utc=detected_at_utc,
                        rule_id="fee.belongs_to_capture",
                        exception_type=ExceptionCode.UNEXPECTED_FEE,
                        entity_type=EntityType.FEE.value,
                        entity_id=fee.fee_id,
                        expected_amount=ZERO,
                        actual_amount=fee.amount,
                        supporting_source_record_ids=(
                            _source_reference(fee),
                        ),
                    )
                )

    # ---------------------------------------------------------
    # Transaction reconciliation
    # ---------------------------------------------------------

    transactions: list[TransactionReconciliation] = []

    order_and_payment_complete = _reports_complete(
        report_states,
        {ReportType.ORDERS, ReportType.PAYMENTS},
    )

    for order in orders:
        order_payments = captured_by_order.get(
            order.order_id,
            [],
        )
        captured_total = _sum_money(
            payment.amount
            for payment in order_payments
        )

        expected_collection = (
            order.order_total
            if order.status is OrderStatus.PAID
            else ZERO
        )
        collection_variance = (
            captured_total - expected_collection
        )

        if (
            order.status is OrderStatus.PAID
            and order_and_payment_complete
        ):
            if not order_payments:
                code = ExceptionCode.MISSING_PAYMENT
                transaction_codes[order.order_id].add(code)

                exceptions.append(
                    _new_exception(
                        run_id=run_id,
                        detected_at_utc=detected_at_utc,
                        rule_id="order.payment_exists",
                        exception_type=code,
                        entity_type=EntityType.ORDER.value,
                        entity_id=order.order_id,
                        expected_amount=order.order_total,
                        actual_amount=ZERO,
                        supporting_source_record_ids=(
                            _source_reference(order),
                        ),
                    )
                )
            elif collection_variance != ZERO:
                code = (
                    ExceptionCode.PAYMENT_AMOUNT_MISMATCH
                )
                transaction_codes[order.order_id].add(code)

                exceptions.append(
                    _new_exception(
                        run_id=run_id,
                        detected_at_utc=detected_at_utc,
                        rule_id="order.collection_matches_total",
                        exception_type=code,
                        entity_type=EntityType.ORDER.value,
                        entity_id=order.order_id,
                        expected_amount=order.order_total,
                        actual_amount=captured_total,
                        supporting_source_record_ids=(
                            _source_reference(order),
                            *(
                                _source_reference(payment)
                                for payment in order_payments
                            ),
                        ),
                    )
                )

            if len(order_payments) > 1:
                code = ExceptionCode.MULTIPLE_CAPTURE
                transaction_codes[order.order_id].add(code)

                exceptions.append(
                    _new_exception(
                        run_id=run_id,
                        detected_at_utc=detected_at_utc,
                        rule_id="order.maximum_one_capture",
                        exception_type=code,
                        entity_type=EntityType.ORDER.value,
                        entity_id=order.order_id,
                        expected_amount=Decimal("1.00"),
                        actual_amount=Decimal(
                            len(order_payments)
                        ),
                        supporting_source_record_ids=(
                            _source_reference(order),
                            *(
                                _source_reference(payment)
                                for payment in order_payments
                            ),
                        ),
                    )
                )

        payment_ids = {
            payment.payment_id
            for payment in order_payments
        }

        order_refunds = [
            refund
            for refund in successful_refunds
            if refund.payment_id in payment_ids
        ]

        order_fees = [
            fee
            for fee in fees
            if fee.payment_id in payment_ids
        ]

        refund_total = _sum_money(
            refund.amount for refund in order_refunds
        )
        expected_fee_total = _sum_money(
            expected_fee_by_payment[payment_id]
            for payment_id in payment_ids
        )
        reported_fee_total = _sum_money(
            fee.amount for fee in order_fees
        )

        lifetime_net_collection = (
            captured_total
            - refund_total
            - expected_fee_total
        )

        codes = tuple(
            sorted(
                transaction_codes[order.order_id],
                key=lambda item: item.value,
            )
        )

        transactions.append(
            TransactionReconciliation(
                run_id=run_id,
                contract_version=CONTRACT_VERSION,
                order_id=order.order_id,
                order_status=order.status,
                currency=order.currency,
                expected_collection=expected_collection,
                captured_total=captured_total,
                collection_variance=collection_variance,
                successful_refund_total=refund_total,
                expected_fee_total=expected_fee_total,
                reported_fee_total=reported_fee_total,
                lifetime_net_collection=(
                    lifetime_net_collection
                ),
                captured_payment_count=len(order_payments),
                successful_refund_count=len(order_refunds),
                state=_result_state(
                    report_states=report_states,
                    required_reports=(
                        TRANSACTION_REQUIRED_REPORTS
                    ),
                    has_blocking_exception=bool(codes),
                ),
                exception_codes=codes,
            )
        )

    # ---------------------------------------------------------
    # Expected payout movements
    # ---------------------------------------------------------

    expected_movements: list[ExpectedMovement] = []

    for payment in captured_payments:
        if payment.available_on is None:
            raise ReconciliationError(
                f"captured payment {payment.payment_id} "
                "has no available_on date"
            )

        expected_movements.append(
            ExpectedMovement(
                movement_type=MovementType.CAPTURE,
                movement_id=payment.payment_id,
                available_on=payment.available_on,
                signed_amount=payment.amount,
                supporting_source_record_ids=(
                    _source_reference(payment),
                ),
            )
        )

        related_fees = fees_by_payment.get(
            payment.payment_id,
            [],
        )

        fee_movement_id = (
            related_fees[0].fee_id
            if len(related_fees) == 1
            else f"expected-fee:{payment.payment_id}"
        )

        expected_movements.append(
            ExpectedMovement(
                movement_type=MovementType.FEE,
                movement_id=fee_movement_id,
                available_on=payment.available_on,
                signed_amount=(
                    -expected_fee_by_payment[
                        payment.payment_id
                    ]
                ),
                supporting_source_record_ids=(
                    _source_reference(payment),
                    *(
                        _source_reference(fee)
                        for fee in related_fees
                    ),
                ),
            )
        )

    for refund in successful_refunds:
        if refund.available_on is None:
            raise ReconciliationError(
                f"successful refund {refund.refund_id} "
                "has no available_on date"
            )

        expected_movements.append(
            ExpectedMovement(
                movement_type=MovementType.REFUND,
                movement_id=refund.refund_id,
                available_on=refund.available_on,
                signed_amount=-refund.amount,
                supporting_source_record_ids=(
                    _source_reference(refund),
                ),
            )
        )

    expected_movements.sort(
        key=lambda movement: movement.sort_key
    )

    # ---------------------------------------------------------
    # Settlement line integrity
    # ---------------------------------------------------------

    lines_by_movement: dict[
        tuple[MovementType, str],
        list[SettlementLine],
    ] = defaultdict(list)

    lines_by_payout: dict[
        str,
        list[SettlementLine],
    ] = defaultdict(list)

    for line in settlement_lines:
        lines_by_movement[
            (line.movement_type, line.movement_id)
        ].append(line)
        lines_by_payout[line.payout_id].append(line)

    if report_states[
        ReportType.SETTLEMENT_LINES
    ] is SourceCompleteness.COMPLETE:
        for movement_key, matching_lines in sorted(
            lines_by_movement.items(),
            key=lambda item: (
                item[0][0].value,
                item[0][1],
            ),
        ):
            if len(matching_lines) <= 1:
                continue

            movement_type, movement_id = movement_key

            exceptions.append(
                _new_exception(
                    run_id=run_id,
                    detected_at_utc=detected_at_utc,
                    rule_id=(
                        "settlement.unique_movement_allocation"
                    ),
                    exception_type=(
                        ExceptionCode
                        .DUPLICATE_SETTLEMENT_MOVEMENT
                    ),
                    entity_type="SETTLEMENT_MOVEMENT",
                    entity_id=(
                        f"{movement_type.value}:{movement_id}"
                    ),
                    actual_amount=Decimal(
                        len(matching_lines)
                    ),
                    supporting_source_record_ids=(
                        _source_reference(line)
                        for line in matching_lines
                    ),
                )
            )

            for line in matching_lines:
                payout_codes[line.payout_id].add(
                    ExceptionCode
                    .DUPLICATE_SETTLEMENT_MOVEMENT
                )

    expected_by_date: dict[
        date,
        list[ExpectedMovement],
    ] = defaultdict(list)

    for movement in expected_movements:
        expected_by_date[
            movement.available_on
        ].append(movement)

    # ---------------------------------------------------------
    # Payout reconciliation
    # ---------------------------------------------------------

    payouts_by_date: dict[date, list[Payout]] = defaultdict(list)

    for payout in payouts:
        payouts_by_date[payout.payout_date].append(payout)

    duplicate_payout_dates = {
        payout_date: daily_payouts
        for payout_date, daily_payouts
        in payouts_by_date.items()
        if len(daily_payouts) > 1
    }

    if duplicate_payout_dates:
        dates = ", ".join(
            payout_date.isoformat()
            for payout_date in sorted(duplicate_payout_dates)
        )
        raise ReconciliationError(
            f"multiple canonical payouts exist for dates: {dates}"
        )

    payout_results: list[PayoutReconciliation] = []

    movement_comparison_complete = _reports_complete(
        report_states,
        {
            ReportType.PAYMENTS,
            ReportType.REFUNDS,
            ReportType.FEES,
            ReportType.SETTLEMENT_LINES,
        },
    )

    payout_total_comparison_complete = _reports_complete(
        report_states,
        {
            ReportType.SETTLEMENT_LINES,
            ReportType.PAYOUTS,
        },
    )

    for payout in payouts:
        expected_for_date = expected_by_date.get(
            payout.payout_date,
            [],
        )
        payout_lines = lines_by_payout.get(
            payout.payout_id,
            [],
        )

        expected_payout = _sum_money(
            movement.signed_amount
            for movement in expected_for_date
        )
        reported_line_total = _sum_money(
            line.signed_amount
            for line in payout_lines
        )

        provider_variance = (
            payout.reported_net_amount
            - reported_line_total
        )
        end_to_end_variance = (
            payout.reported_net_amount
            - expected_payout
        )

        payout_lines_by_movement: dict[
            tuple[MovementType, str],
            list[SettlementLine],
        ] = defaultdict(list)

        for line in payout_lines:
            payout_lines_by_movement[
                (line.movement_type, line.movement_id)
            ].append(line)

        if movement_comparison_complete:
            for movement in expected_for_date:
                matching_lines = payout_lines_by_movement.get(
                    movement.movement_key,
                    [],
                )

                if not matching_lines:
                    code = (
                        ExceptionCode.MISSING_SETTLEMENT_LINE
                    )
                    payout_codes[payout.payout_id].add(code)

                    exceptions.append(
                        _new_exception(
                            run_id=run_id,
                            detected_at_utc=detected_at_utc,
                            rule_id=(
                                "settlement.expected_movement_exists"
                            ),
                            exception_type=code,
                            entity_type="EXPECTED_MOVEMENT",
                            entity_id=(
                                f"{movement.movement_type.value}:"
                                f"{movement.movement_id}"
                            ),
                            expected_amount=(
                                movement.signed_amount
                            ),
                            actual_amount=ZERO,
                            supporting_source_record_ids=(
                                movement
                                .supporting_source_record_ids
                            ),
                        )
                    )
                    continue

                if len(matching_lines) == 1:
                    line = matching_lines[0]
                    
                    expected_line_amount = movement.signed_amount

                    # Expected payout uses the contractual fee amount.
                    # Settlement validation checks whether the settlement
                    # line agrees with MockPay's reported fee record.
                    if movement.movement_type is MovementType.FEE:
                        reported_fee = fees_by_id.get(
                            movement.movement_id
                        )

                        if reported_fee is not None:
                            expected_line_amount = -reported_fee.amount

                    if line.signed_amount != expected_line_amount:
                        code = (
                            ExceptionCode
                            .SETTLEMENT_LINE_AMOUNT_MISMATCH
                        )
                        payout_codes[payout.payout_id].add(code)

                        exceptions.append(
                            _new_exception(
                                run_id=run_id,
                                detected_at_utc=detected_at_utc,
                                rule_id=(
                                    "settlement.amount_matches_"
                                    "reported_source_movement"
                                ),
                                exception_type=code,
                                entity_type=(
                                EntityType.SETTLEMENT_LINE.value
                                ),
                                entity_id=line.settlement_line_id,
                                expected_amount=expected_line_amount,
                                actual_amount=line.signed_amount,
                                supporting_source_record_ids=(
                                    *movement.supporting_source_record_ids,
                                    _source_reference(line),
                                ),
                        )
                    )

        if (
            payout_total_comparison_complete
            and provider_variance != ZERO
        ):
            code = ExceptionCode.PAYOUT_TOTAL_MISMATCH
            payout_codes[payout.payout_id].add(code)

            exceptions.append(
                _new_exception(
                    run_id=run_id,
                    detected_at_utc=detected_at_utc,
                    rule_id=(
                        "payout.header_matches_settlement_lines"
                    ),
                    exception_type=code,
                    entity_type=EntityType.PAYOUT.value,
                    entity_id=payout.payout_id,
                    expected_amount=reported_line_total,
                    actual_amount=payout.reported_net_amount,
                    supporting_source_record_ids=(
                        _source_reference(payout),
                        *(
                            _source_reference(line)
                            for line in payout_lines
                        ),
                    ),
                )
            )

        codes = tuple(
            sorted(
                payout_codes[payout.payout_id],
                key=lambda item: item.value,
            )
        )

        # A nonzero end-to-end variance remains blocking even when
        # provider header and provider detail agree. Its amount is
        # visible directly on the payout result.
        has_blocking_exception = (
            bool(codes)
            or end_to_end_variance != ZERO
        )

        payout_results.append(
            PayoutReconciliation(
                run_id=run_id,
                contract_version=CONTRACT_VERSION,
                payout_id=payout.payout_id,
                payout_date=payout.payout_date,
                currency=payout.currency,
                expected_payout=expected_payout,
                reported_line_total=reported_line_total,
                reported_net_amount=(
                    payout.reported_net_amount
                ),
                provider_report_variance=(
                    provider_variance
                ),
                end_to_end_payout_variance=(
                    end_to_end_variance
                ),
                expected_movement_count=len(
                    expected_for_date
                ),
                settlement_line_count=len(payout_lines),
                state=_result_state(
                    report_states=report_states,
                    required_reports=PAYOUT_REQUIRED_REPORTS,
                    has_blocking_exception=(
                        has_blocking_exception
                    ),
                ),
                exception_codes=codes,
            )
        )

    # One failed rule per entity per run.
    unique_exceptions = {
        (
            exception.rule_id,
            exception.exception_type,
            exception.entity_type,
            exception.entity_id,
        ): exception
        for exception in exceptions
    }

    sorted_exceptions = tuple(
        sorted(
            unique_exceptions.values(),
            key=lambda exception: exception.sort_key,
        )
    )

    transactions.sort(key=lambda row: row.sort_key)
    payout_results.sort(key=lambda row: row.sort_key)

    sorted_quarantine = tuple(
        sorted(
            quarantine_records,
            key=lambda record: record.sort_key,
        )
    )

    return ReconciliationResult(
        run_id=run_id,
        contract_version=CONTRACT_VERSION,
        detected_at_utc=detected_at_utc,
        transactions=tuple(transactions),
        expected_movements=tuple(expected_movements),
        payouts=tuple(payout_results),
        exceptions=sorted_exceptions,
        source_completeness=tuple(
            result.status
            for result in completeness_results
        ),
        quarantine_records=sorted_quarantine,
    )