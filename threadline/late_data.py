"""Precise late-data semantics for Threadline.

This module contains no PostgreSQL or Airflow code. It classifies
logical record changes relative to an already published financial
boundary.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo

from threadline.contracts import EntityType


BERLIN = ZoneInfo("Europe/Berlin")


class ChangeKind(str, Enum):
    """Logical outcome assigned by canonicalization."""

    NEW_FACT = "NEW_FACT"
    HIGHER_VERSION_CORRECTION = (
        "HIGHER_VERSION_CORRECTION"
    )
    SAME_VERSION_CONFLICT = "SAME_VERSION_CONFLICT"
    EXACT_DUPLICATE = "EXACT_DUPLICATE"
    STALE_VERSION = "STALE_VERSION"


class LateDataClassification(str, Enum):
    """Timing classification relative to a published run."""

    WITHIN_WATERMARK = "WITHIN_WATERMARK"
    NO_LOGICAL_CHANGE = "NO_LOGICAL_CHANGE"
    FUTURE_IMPACT = "FUTURE_IMPACT"

    LATE_NEW_FACT = "LATE_NEW_FACT"
    LATE_CORRECTION = "LATE_CORRECTION"
    LATE_CONFLICT = "LATE_CONFLICT"

    UNRESOLVED_IMPACT = "UNRESOLVED_IMPACT"


LATE_CLASSIFICATIONS = frozenset(
    {
        LateDataClassification.LATE_NEW_FACT,
        LateDataClassification.LATE_CORRECTION,
        LateDataClassification.LATE_CONFLICT,
    }
)


def _as_utc(
    value: datetime,
    *,
    field_name: str,
) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(
            f"{field_name} must be a datetime"
        )

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(
            f"{field_name} must be timezone-aware"
        )

    return value.astimezone(timezone.utc)


def _require_date(
    value: Any,
    *,
    field_name: str,
) -> date:
    # datetime is a subclass of date, but these fields must be
    # business dates without a clock component.
    if (
        not isinstance(value, date)
        or isinstance(value, datetime)
    ):
        raise TypeError(
            f"{field_name} must be a date"
        )

    return value


def business_date_from_timestamp(
    value: datetime,
    *,
    business_timezone: ZoneInfo = BERLIN,
) -> date:
    """Convert an event timestamp to its local business date."""

    utc_value = _as_utc(
        value,
        field_name="value",
    )

    return utc_value.astimezone(
        business_timezone
    ).date()


@dataclass(frozen=True, slots=True)
class ReconciliationBoundary:
    """Coverage of an already published reconciliation run."""

    run_id: str

    # Latest receipt time included in the run.
    input_watermark_utc: datetime

    # Latest business date considered financially complete.
    covered_through_date: date

    def __post_init__(self) -> None:
        if (
            not self.run_id
            or self.run_id != self.run_id.strip()
        ):
            raise ValueError(
                "run_id must be a non-empty "
                "canonical string"
            )

        object.__setattr__(
            self,
            "input_watermark_utc",
            _as_utc(
                self.input_watermark_utc,
                field_name="input_watermark_utc",
            ),
        )

        _require_date(
            self.covered_through_date,
            field_name="covered_through_date",
        )


@dataclass(frozen=True, slots=True)
class ImpactContext:
    """Relationship data required to resolve financial dates."""

    payment_available_on: Mapping[str, date] = field(
        default_factory=dict
    )
    payout_dates: Mapping[str, date] = field(
        default_factory=dict
    )


@dataclass(frozen=True, slots=True)
class ImpactResolution:
    affected_dates: tuple[date, ...]
    unresolved_reason: str | None = None

    def __post_init__(self) -> None:
        normalized = tuple(
            sorted(set(self.affected_dates))
        )

        for affected_date in normalized:
            _require_date(
                affected_date,
                field_name="affected_date",
            )

        object.__setattr__(
            self,
            "affected_dates",
            normalized,
        )


@dataclass(frozen=True, slots=True)
class LateDataCandidate:
    receipt_id: str
    entity_type: EntityType
    source_id: str
    source_version: int

    received_at_utc: datetime
    change_kind: ChangeKind

    affected_dates: tuple[date, ...]
    unresolved_reason: str | None = None

    def __post_init__(self) -> None:
        if not self.receipt_id:
            raise ValueError(
                "receipt_id must not be empty"
            )

        if not self.source_id:
            raise ValueError(
                "source_id must not be empty"
            )

        if self.source_version <= 0:
            raise ValueError(
                "source_version must be positive"
            )

        object.__setattr__(
            self,
            "received_at_utc",
            _as_utc(
                self.received_at_utc,
                field_name="received_at_utc",
            ),
        )

        normalized_dates = tuple(
            sorted(set(self.affected_dates))
        )

        for affected_date in normalized_dates:
            _require_date(
                affected_date,
                field_name="affected_date",
            )

        object.__setattr__(
            self,
            "affected_dates",
            normalized_dates,
        )


@dataclass(frozen=True, slots=True)
class LateDataDecision:
    receipt_id: str
    entity_type: EntityType
    source_id: str
    source_version: int

    classification: LateDataClassification
    change_kind: ChangeKind

    received_at_utc: datetime
    input_watermark_utc: datetime
    covered_through_date: date

    affected_dates: tuple[date, ...]
    historical_affected_dates: tuple[date, ...]

    unresolved_reason: str | None = None

    @property
    def is_late(self) -> bool:
        return (
            self.classification
            in LATE_CLASSIFICATIONS
        )

    @property
    def requires_reprocessing(self) -> bool:
        """Fail closed when financial impact cannot be resolved."""

        return (
            self.is_late
            or self.classification
            is LateDataClassification.UNRESOLVED_IMPACT
        )

    @property
    def arrived_after_watermark(self) -> bool:
        return (
            self.received_at_utc
            > self.input_watermark_utc
        )

    @property
    def arrival_delay_seconds(self) -> int:
        delay = (
            self.received_at_utc
            - self.input_watermark_utc
        )

        return max(0, int(delay.total_seconds()))


def _record_entity_type(
    record: object,
) -> EntityType:
    value = getattr(record, "ENTITY_TYPE", None)

    if value is None:
        value = getattr(record, "entity_type", None)

    if isinstance(value, EntityType):
        return value

    if isinstance(value, str):
        return EntityType(value)

    raise TypeError(
        f"record {type(record).__name__} "
        "does not expose an entity type"
    )


def resolve_record_impact(
    record: object,
    *,
    context: ImpactContext | None = None,
) -> ImpactResolution:
    """Resolve the business dates affected by one source record.

    Related lookup data is deliberately supplied by the caller. The
    pure domain function never performs database queries.
    """

    context = context or ImpactContext()
    entity_type = _record_entity_type(record)

    if entity_type in {
        EntityType.PAYMENT,
        EntityType.REFUND,
    }:
        available_on = getattr(
            record,
            "available_on",
            None,
        )

        if available_on is None:
            return ImpactResolution(
                (),
                (
                    f"{entity_type.value} has no "
                    "available_on date"
                ),
            )

        return ImpactResolution(
            (
                _require_date(
                    available_on,
                    field_name="available_on",
                ),
            )
        )

    if entity_type is EntityType.FEE:
        payment_id = getattr(
            record,
            "payment_id",
            None,
        )

        available_on = (
            context.payment_available_on.get(
                payment_id
            )
            if payment_id
            else None
        )

        if available_on is None:
            return ImpactResolution(
                (),
                (
                    "FEE requires the parent payment's "
                    "available_on date"
                ),
            )

        return ImpactResolution((available_on,))

    if entity_type is EntityType.SETTLEMENT_LINE:
        payout_id = getattr(
            record,
            "payout_id",
            None,
        )

        payout_date = (
            context.payout_dates.get(payout_id)
            if payout_id
            else None
        )

        if payout_date is None:
            return ImpactResolution(
                (),
                (
                    "SETTLEMENT_LINE requires its "
                    "payout date"
                ),
            )

        return ImpactResolution((payout_date,))

    if entity_type is EntityType.PAYOUT:
        payout_date = getattr(
            record,
            "payout_date",
            None,
        )

        if payout_date is None:
            return ImpactResolution(
                (),
                "PAYOUT has no payout_date",
            )

        return ImpactResolution(
            (
                _require_date(
                    payout_date,
                    field_name="payout_date",
                ),
            )
        )

    if entity_type is EntityType.ORDER:
        effective_at = getattr(
            record,
            "effective_at_utc",
            None,
        )

        if effective_at is None:
            return ImpactResolution(
                (),
                "ORDER has no effective_at_utc",
            )

        return ImpactResolution(
            (
                business_date_from_timestamp(
                    effective_at
                ),
            )
        )

    return ImpactResolution(
        (),
        (
            f"unsupported entity type "
            f"{entity_type.value}"
        ),
    )


def build_late_data_candidate(
    *,
    receipt_id: str,
    received_at_utc: datetime,
    change_kind: ChangeKind,
    incoming_record: object,
    previous_record: object | None = None,
    context: ImpactContext | None = None,
) -> LateDataCandidate:
    """Build a candidate from the old and new logical versions.

    Both versions matter because a correction can move financial
    impact from one business date to another.
    """

    context = context or ImpactContext()

    records = [
        record
        for record in (
            previous_record,
            incoming_record,
        )
        if record is not None
    ]

    affected_dates: set[date] = set()
    unresolved_reasons: list[str] = []

    for record in records:
        resolution = resolve_record_impact(
            record,
            context=context,
        )

        affected_dates.update(
            resolution.affected_dates
        )

        if resolution.unresolved_reason:
            unresolved_reasons.append(
                resolution.unresolved_reason
            )

    entity_type = _record_entity_type(
        incoming_record
    )
    source_id = getattr(
        incoming_record,
        "record_id",
        None,
    )
    source_version = getattr(
        incoming_record,
        "source_version",
        None,
    )

    if not isinstance(source_id, str):
        raise TypeError(
            "incoming_record must expose record_id"
        )

    if not isinstance(source_version, int):
        raise TypeError(
            "incoming_record must expose "
            "source_version"
        )

    return LateDataCandidate(
        receipt_id=receipt_id,
        entity_type=entity_type,
        source_id=source_id,
        source_version=source_version,
        received_at_utc=received_at_utc,
        change_kind=change_kind,
        affected_dates=tuple(affected_dates),
        unresolved_reason=(
            "; ".join(sorted(set(unresolved_reasons)))
            if unresolved_reasons
            else None
        ),
    )


def classify_late_data(
    *,
    candidate: LateDataCandidate,
    boundary: ReconciliationBoundary,
) -> LateDataDecision:
    """Classify a receipt relative to a published run."""

    historical_dates = tuple(
        affected_date
        for affected_date in candidate.affected_dates
        if (
            affected_date
            <= boundary.covered_through_date
        )
    )

    if candidate.change_kind in {
        ChangeKind.EXACT_DUPLICATE,
        ChangeKind.STALE_VERSION,
    }:
        classification = (
            LateDataClassification.NO_LOGICAL_CHANGE
        )

    elif (
        candidate.received_at_utc
        <= boundary.input_watermark_utc
    ):
        classification = (
            LateDataClassification.WITHIN_WATERMARK
        )

    elif candidate.unresolved_reason:
        classification = (
            LateDataClassification.UNRESOLVED_IMPACT
        )

    elif not historical_dates:
        classification = (
            LateDataClassification.FUTURE_IMPACT
        )

    elif (
        candidate.change_kind
        is ChangeKind.NEW_FACT
    ):
        classification = (
            LateDataClassification.LATE_NEW_FACT
        )

    elif (
        candidate.change_kind
        is ChangeKind.HIGHER_VERSION_CORRECTION
    ):
        classification = (
            LateDataClassification.LATE_CORRECTION
        )

    elif (
        candidate.change_kind
        is ChangeKind.SAME_VERSION_CONFLICT
    ):
        classification = (
            LateDataClassification.LATE_CONFLICT
        )

    else:
        raise ValueError(
            "unsupported logical change kind: "
            f"{candidate.change_kind.value}"
        )

    return LateDataDecision(
        receipt_id=candidate.receipt_id,
        entity_type=candidate.entity_type,
        source_id=candidate.source_id,
        source_version=candidate.source_version,
        classification=classification,
        change_kind=candidate.change_kind,
        received_at_utc=(
            candidate.received_at_utc
        ),
        input_watermark_utc=(
            boundary.input_watermark_utc
        ),
        covered_through_date=(
            boundary.covered_through_date
        ),
        affected_dates=candidate.affected_dates,
        historical_affected_dates=(
            historical_dates
        ),
        unresolved_reason=(
            candidate.unresolved_reason
        ),
    )