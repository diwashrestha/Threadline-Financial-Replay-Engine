"""Tests for Threadline's precise late-data semantics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import ClassVar

import pytest

from threadline.contracts import EntityType
from threadline.late_data import (
    ChangeKind,
    ImpactContext,
    LateDataCandidate,
    LateDataClassification,
    ReconciliationBoundary,
    build_late_data_candidate,
    business_date_from_timestamp,
    classify_late_data,
    resolve_record_impact,
)


BOUNDARY = ReconciliationBoundary(
    run_id="RUN-A",
    input_watermark_utc=datetime(
        2026,
        9,
        14,
        12,
        0,
        tzinfo=timezone.utc,
    ),
    covered_through_date=date(
        2026,
        9,
        14,
    ),
)


def _candidate(
    *,
    change_kind: ChangeKind,
    received_at_utc: datetime | None = None,
    affected_dates: tuple[date, ...] = (
        date(2026, 9, 14),
    ),
    unresolved_reason: str | None = None,
) -> LateDataCandidate:
    return LateDataCandidate(
        receipt_id="RECEIPT-001",
        entity_type=EntityType.REFUND,
        source_id="REF-001",
        source_version=1,
        received_at_utc=(
            received_at_utc
            or datetime(
                2026,
                9,
                15,
                8,
                0,
                tzinfo=timezone.utc,
            )
        ),
        change_kind=change_kind,
        affected_dates=affected_dates,
        unresolved_reason=unresolved_reason,
    )


def test_new_fact_after_watermark_for_covered_date_is_late():
    decision = classify_late_data(
        candidate=_candidate(
            change_kind=ChangeKind.NEW_FACT
        ),
        boundary=BOUNDARY,
    )

    assert (
        decision.classification
        is LateDataClassification.LATE_NEW_FACT
    )
    assert decision.is_late is True
    assert decision.requires_reprocessing is True
    assert decision.historical_affected_dates == (
        date(2026, 9, 14),
    )


def test_higher_version_for_covered_date_is_late_correction():
    decision = classify_late_data(
        candidate=_candidate(
            change_kind=(
                ChangeKind
                .HIGHER_VERSION_CORRECTION
            )
        ),
        boundary=BOUNDARY,
    )

    assert (
        decision.classification
        is LateDataClassification.LATE_CORRECTION
    )
    assert decision.requires_reprocessing is True


def test_same_version_conflict_is_late_conflict():
    decision = classify_late_data(
        candidate=_candidate(
            change_kind=(
                ChangeKind.SAME_VERSION_CONFLICT
            )
        ),
        boundary=BOUNDARY,
    )

    assert (
        decision.classification
        is LateDataClassification.LATE_CONFLICT
    )
    assert decision.is_late is True


@pytest.mark.parametrize(
    "change_kind",
    [
        ChangeKind.EXACT_DUPLICATE,
        ChangeKind.STALE_VERSION,
    ],
)
def test_duplicate_and_stale_records_have_no_logical_change(
    change_kind: ChangeKind,
):
    decision = classify_late_data(
        candidate=_candidate(
            change_kind=change_kind
        ),
        boundary=BOUNDARY,
    )

    assert (
        decision.classification
        is LateDataClassification.NO_LOGICAL_CHANGE
    )
    assert decision.is_late is False
    assert decision.requires_reprocessing is False


def test_future_financial_date_is_not_late():
    decision = classify_late_data(
        candidate=_candidate(
            change_kind=ChangeKind.NEW_FACT,
            affected_dates=(
                date(2026, 9, 16),
            ),
        ),
        boundary=BOUNDARY,
    )

    assert (
        decision.classification
        is LateDataClassification.FUTURE_IMPACT
    )
    assert decision.requires_reprocessing is False


def test_receipt_at_watermark_is_within_watermark():
    decision = classify_late_data(
        candidate=_candidate(
            change_kind=ChangeKind.NEW_FACT,
            received_at_utc=(
                BOUNDARY.input_watermark_utc
            ),
        ),
        boundary=BOUNDARY,
    )

    assert (
        decision.classification
        is LateDataClassification.WITHIN_WATERMARK
    )
    assert decision.arrived_after_watermark is False


def test_receipt_before_watermark_is_within_watermark():
    decision = classify_late_data(
        candidate=_candidate(
            change_kind=ChangeKind.NEW_FACT,
            received_at_utc=datetime(
                2026,
                9,
                14,
                11,
                59,
                59,
                tzinfo=timezone.utc,
            ),
        ),
        boundary=BOUNDARY,
    )

    assert (
        decision.classification
        is LateDataClassification.WITHIN_WATERMARK
    )


def test_unresolved_impact_fails_closed():
    decision = classify_late_data(
        candidate=_candidate(
            change_kind=ChangeKind.NEW_FACT,
            affected_dates=(),
            unresolved_reason=(
                "parent payment was not found"
            ),
        ),
        boundary=BOUNDARY,
    )

    assert (
        decision.classification
        is LateDataClassification.UNRESOLVED_IMPACT
    )
    assert decision.is_late is False
    assert decision.requires_reprocessing is True


def test_arrival_delay_is_measured_from_watermark():
    decision = classify_late_data(
        candidate=_candidate(
            change_kind=ChangeKind.NEW_FACT,
            received_at_utc=datetime(
                2026,
                9,
                14,
                12,
                5,
                tzinfo=timezone.utc,
            ),
        ),
        boundary=BOUNDARY,
    )

    assert decision.arrival_delay_seconds == 300


def test_berlin_business_date_handles_utc_day_boundary():
    timestamp = datetime(
        2026,
        9,
        14,
        22,
        30,
        tzinfo=timezone.utc,
    )

    # September uses UTC+02:00 in Berlin.
    assert business_date_from_timestamp(
        timestamp
    ) == date(2026, 9, 15)


@dataclass(frozen=True)
class FakePayment:
    ENTITY_TYPE: ClassVar[EntityType] = (
        EntityType.PAYMENT
    )

    record_id: str
    source_version: int
    available_on: date


def test_correction_uses_old_and_new_impact_dates():
    previous = FakePayment(
        record_id="PAY-001",
        source_version=1,
        available_on=date(2026, 9, 14),
    )
    incoming = FakePayment(
        record_id="PAY-001",
        source_version=2,
        available_on=date(2026, 9, 16),
    )

    candidate = build_late_data_candidate(
        receipt_id="RECEIPT-CORRECTION",
        received_at_utc=datetime(
            2026,
            9,
            15,
            8,
            0,
            tzinfo=timezone.utc,
        ),
        change_kind=(
            ChangeKind.HIGHER_VERSION_CORRECTION
        ),
        previous_record=previous,
        incoming_record=incoming,
    )

    assert candidate.affected_dates == (
        date(2026, 9, 14),
        date(2026, 9, 16),
    )

    decision = classify_late_data(
        candidate=candidate,
        boundary=BOUNDARY,
    )

    assert (
        decision.classification
        is LateDataClassification.LATE_CORRECTION
    )
    assert decision.historical_affected_dates == (
        date(2026, 9, 14),
    )


@dataclass(frozen=True)
class FakeFee:
    ENTITY_TYPE: ClassVar[EntityType] = (
        EntityType.FEE
    )

    record_id: str
    source_version: int
    payment_id: str


def test_fee_uses_parent_payment_available_date():
    fee = FakeFee(
        record_id="FEE-001",
        source_version=1,
        payment_id="PAY-001",
    )

    resolution = resolve_record_impact(
        fee,
        context=ImpactContext(
            payment_available_on={
                "PAY-001": date(2026, 9, 14),
            }
        ),
    )

    assert resolution.affected_dates == (
        date(2026, 9, 14),
    )
    assert resolution.unresolved_reason is None


def test_fee_without_parent_payment_is_unresolved():
    fee = FakeFee(
        record_id="FEE-001",
        source_version=1,
        payment_id="PAY-MISSING",
    )

    resolution = resolve_record_impact(fee)

    assert resolution.affected_dates == ()
    assert resolution.unresolved_reason is not None


@dataclass(frozen=True)
class FakeSettlementLine:
    ENTITY_TYPE: ClassVar[EntityType] = (
        EntityType.SETTLEMENT_LINE
    )

    record_id: str
    source_version: int
    payout_id: str


def test_settlement_line_uses_parent_payout_date():
    line = FakeSettlementLine(
        record_id="LINE-001",
        source_version=1,
        payout_id="PAYOUT-001",
    )

    resolution = resolve_record_impact(
        line,
        context=ImpactContext(
            payout_dates={
                "PAYOUT-001": date(2026, 9, 14),
            }
        ),
    )

    assert resolution.affected_dates == (
        date(2026, 9, 14),
    )