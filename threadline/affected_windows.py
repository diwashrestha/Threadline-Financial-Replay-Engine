"""Conservative affected-window planning for Threadline.

This module plans recovery scope. It does not execute recovery.

Inputs must include:
- canonical records before the change;
- canonical records after the change;
- previous and incoming variants of changed records.

Order results are lifetime results.
Payout results are grouped by availability/payout date.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo

from threadline.contracts import EntityType
from threadline.late_data import ChangeKind


BERLIN = ZoneInfo("Europe/Berlin")


class RecoveryScope(str, Enum):
    NONE = "NONE"
    WINDOWS = "WINDOWS"
    FULL = "FULL"


@dataclass(frozen=True, slots=True)
class RecordKey:
    entity_type: EntityType
    source_id: str


@dataclass(frozen=True, slots=True)
class DateWindow:
    """Half-open date range: start_date <= day < end_date_exclusive."""

    start_date: date
    end_date_exclusive: date


@dataclass(frozen=True, slots=True)
class AffectedWindowPlan:
    scope: RecoveryScope
    order_ids: tuple[str, ...] = ()
    payout_ids: tuple[str, ...] = ()
    payout_dates: tuple[date, ...] = ()
    payout_windows: tuple[DateWindow, ...] = ()
    source_keys: tuple[RecordKey, ...] = ()
    reason_codes: tuple[str, ...] = ()


def _enum_value(value: Any) -> str:
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


def _require_date(value: Any) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise TypeError("Expected a parsed date, not a timestamp or string")
    return value


def record_key(record: Any) -> RecordKey:
    entity_type = EntityType(record.ENTITY_TYPE)
    source_id = record.record_id

    if not isinstance(source_id, str) or not source_id:
        raise ValueError("Record must have a non-empty record_id")

    return RecordKey(entity_type, source_id)


def compress_dates(days: Iterable[date]) -> tuple[DateWindow, ...]:
    """Combine adjacent dates without including gaps."""

    ordered = sorted({_require_date(day) for day in days})

    if not ordered:
        return ()

    windows = []
    start = ordered[0]
    previous = start

    for current in ordered[1:]:
        if current != previous + timedelta(days=1):
            windows.append(
                DateWindow(
                    start_date=start,
                    end_date_exclusive=previous + timedelta(days=1),
                )
            )
            start = current

        previous = current

    windows.append(
        DateWindow(
            start_date=start,
            end_date_exclusive=previous + timedelta(days=1),
        )
    )

    return tuple(windows)


def business_day_utc_bounds(day: date) -> tuple[datetime, datetime]:
    """UTC boundaries for one Berlin business day.

    Construct both local midnights separately so DST days can have
    23 or 25 hours.
    """

    day = _require_date(day)

    local_start = datetime.combine(
        day,
        time.min,
        tzinfo=BERLIN,
    )
    local_end = datetime.combine(
        day + timedelta(days=1),
        time.min,
        tzinfo=BERLIN,
    )

    return (
        local_start.astimezone(timezone.utc),
        local_end.astimezone(timezone.utc),
    )


class _ImpactGraph:
    def __init__(self) -> None:
        self.known: set[RecordKey] = set()

        self.neighbors: dict[RecordKey, set[RecordKey]] = defaultdict(set)
        self.dates: dict[RecordKey, set[date]] = defaultdict(set)
        self.keys_by_date: dict[date, set[RecordKey]] = defaultdict(set)
        self.issues: dict[RecordKey, set[str]] = defaultdict(set)

    def _link(
        self,
        child: RecordKey,
        parent_type: EntityType,
        parent_id: Any,
    ) -> None:
        if not isinstance(parent_id, str) or not parent_id:
            self.issues[child].add("MISSING_RELATIONSHIP_ID")
            return

        parent = RecordKey(parent_type, parent_id)

        # Follow relationships in both directions.
        self.neighbors[child].add(parent)
        self.neighbors[parent].add(child)

    def _add_date(self, key: RecordKey, value: Any) -> None:
        day = _require_date(value)
        self.dates[key].add(day)
        self.keys_by_date[day].add(key)

    def add(self, record: Any) -> None:
        key = record_key(record)
        self.known.add(key)
        entity_type = key.entity_type

        if entity_type == EntityType.ORDER:
            return

        if entity_type == EntityType.PAYMENT:
            self._link(
                key,
                EntityType.ORDER,
                getattr(record, "order_id", None),
            )

            available_on = getattr(record, "available_on", None)

            if available_on is not None:
                self._add_date(key, available_on)
            elif _enum_value(getattr(record, "status", None)) == "CAPTURED":
                self.issues[key].add("CAPTURE_DATE_UNRESOLVED")

            return

        if entity_type == EntityType.REFUND:
            self._link(
                key,
                EntityType.PAYMENT,
                getattr(record, "payment_id", None),
            )

            available_on = getattr(record, "available_on", None)

            if available_on is not None:
                self._add_date(key, available_on)
            elif _enum_value(getattr(record, "status", None)) == "SUCCEEDED":
                self.issues[key].add("REFUND_DATE_UNRESOLVED")

            return

        if entity_type == EntityType.FEE:
            self._link(
                key,
                EntityType.PAYMENT,
                getattr(record, "payment_id", None),
            )
            return

        if entity_type == EntityType.SETTLEMENT_LINE:
            self._link(
                key,
                EntityType.PAYOUT,
                getattr(record, "payout_id", None),
            )

            movement_type = _enum_value(
                getattr(record, "movement_type", None)
            )

            movement_entity = {
                "CAPTURE": EntityType.PAYMENT,
                "REFUND": EntityType.REFUND,
                "FEE": EntityType.FEE,
            }.get(movement_type)

            if movement_entity is None:
                self.issues[key].add("MOVEMENT_TYPE_UNRESOLVED")
            else:
                self._link(
                    key,
                    movement_entity,
                    getattr(record, "movement_id", None),
                )

            return

        if entity_type == EntityType.PAYOUT:
            payout_date = getattr(record, "payout_date", None)

            if payout_date is None:
                self.issues[key].add("PAYOUT_DATE_UNRESOLVED")
            else:
                self._add_date(key, payout_date)

            return

        self.issues[key].add("ENTITY_IMPACT_UNSUPPORTED")

    def expand(
        self,
        seeds: set[RecordKey],
    ) -> tuple[set[RecordKey], set[date], set[str]]:
        visited: set[RecordKey] = set()
        visited_dates: set[date] = set()
        reasons: set[str] = set()
        pending = list(seeds)

        while pending:
            key = pending.pop()

            if key in visited:
                continue

            visited.add(key)
            reasons.update(self.issues[key])

            if key not in self.known:
                reasons.add(
                    "RELATED_RECORD_MISSING:"
                    f"{key.entity_type.value}:{key.source_id}"
                )

            pending.extend(self.neighbors[key])

            for day in self.dates[key]:
                if day in visited_dates:
                    continue

                visited_dates.add(day)

                # Payout totals depend on every movement sharing this day,
                # including movements without a settlement line.
                pending.extend(self.keys_by_date[day])

        return visited, visited_dates, reasons


def plan_affected_windows(
    *,
    before_records: Iterable[Any],
    after_records: Iterable[Any],
    changed_records: Iterable[Any],
    change_kind: ChangeKind,
    canonical_changed: bool = True,
    completeness_changed: bool = False,
) -> AffectedWindowPlan:
    """Plan conservative recovery from old and new financial dependencies."""

    if completeness_changed:
        return AffectedWindowPlan(
            scope=RecoveryScope.FULL,
            reason_codes=("REPORT_COMPLETENESS_CHANGED",),
        )

    if not canonical_changed or change_kind in {
        ChangeKind.EXACT_DUPLICATE,
        ChangeKind.STALE_VERSION,
    }:
        return AffectedWindowPlan(scope=RecoveryScope.NONE)

    changed = tuple(changed_records)

    if not changed:
        return AffectedWindowPlan(
            scope=RecoveryScope.FULL,
            reason_codes=("CHANGE_EVIDENCE_MISSING",),
        )

    graph = _ImpactGraph()

    # Preserve old edges and dates when corrections move relationships.
    for records in (
        before_records,
        after_records,
        changed,
    ):
        for record in records:
            graph.add(record)

    seeds = {record_key(record) for record in changed}
    keys, payout_dates, reasons = graph.expand(seeds)

    order_ids = tuple(
        sorted(
            key.source_id
            for key in keys
            if key.entity_type == EntityType.ORDER
        )
    )

    payout_ids = tuple(
        sorted(
            key.source_id
            for key in keys
            if key.entity_type == EntityType.PAYOUT
        )
    )

    ordered_dates = tuple(sorted(payout_dates))
    ordered_keys = tuple(
        sorted(
            keys,
            key=lambda key: (
                key.entity_type.value,
                key.source_id,
            ),
        )
    )

    return AffectedWindowPlan(
        scope=RecoveryScope.FULL if reasons else RecoveryScope.WINDOWS,
        order_ids=order_ids,
        payout_ids=payout_ids,
        payout_dates=ordered_dates,
        payout_windows=compress_dates(ordered_dates),
        source_keys=ordered_keys,
        reason_codes=tuple(sorted(reasons)),
    )