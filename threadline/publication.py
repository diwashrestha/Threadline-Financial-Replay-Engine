"""Atomic publication for Threadline reconciliation results."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

from threadline.contracts import CONTRACT_VERSION, ReconciliationState
from threadline.reconcile import ReconciliationResult


class PublicationError(RuntimeError):
    """Base class for publication failures."""


class PublicationValidationError(PublicationError):
    """Raised when a candidate result violates output invariants."""


BeforeCommitHook = Callable[[], None]
Clock = Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class PublicationReceipt:
    run_id: str
    destination: str
    payload_sha256: str
    byte_count: int
    published_at_utc: datetime


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value

    if isinstance(value, Decimal):
        if not value.is_finite():
            raise PublicationValidationError(
                "cannot serialize a non-finite Decimal"
            )
        return format(value, ".2f")

    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise PublicationValidationError(
                "cannot serialize a timestamp without an offset"
            )

        return (
            value.astimezone(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )

    if isinstance(value, date):
        return value.isoformat()

    if is_dataclass(value):
        return {
            item.name: _json_value(getattr(value, item.name))
            for item in fields(value)
        }

    if isinstance(value, Mapping):
        return {
            str(key): _json_value(item)
            for key, item in sorted(
                value.items(),
                key=lambda pair: str(pair[0]),
            )
        }

    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]

    if value is None or isinstance(value, (str, int, bool)):
        return value

    raise PublicationValidationError(
        f"cannot serialize value of type "
        f"{type(value).__name__}"
    )


def result_to_dict(
    result: ReconciliationResult,
) -> dict[str, Any]:
    value = _json_value(result)

    if not isinstance(value, dict):
        raise AssertionError(
            "ReconciliationResult must serialize to a dictionary"
        )

    return value


def result_to_json(
    result: ReconciliationResult,
    *,
    pretty: bool = True,
) -> str:
    """Serialize a result deterministically."""

    if pretty:
        return (
            json.dumps(
                result_to_dict(result),
                sort_keys=True,
                indent=2,
                ensure_ascii=False,
            )
            + "\n"
        )

    return json.dumps(
        result_to_dict(result),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _require_unique(
    values: list[Any],
    *,
    description: str,
) -> None:
    if len(values) != len(set(values)):
        raise PublicationValidationError(
            f"duplicate {description} detected"
        )


def validate_for_publication(
    result: ReconciliationResult,
) -> None:
    """Validate the candidate before it replaces published state."""

    if not isinstance(result, ReconciliationResult):
        raise PublicationValidationError(
            "candidate must be a ReconciliationResult"
        )

    if (
        not result.run_id
        or result.run_id != result.run_id.strip()
    ):
        raise PublicationValidationError(
            "run_id must be a non-empty canonical string"
        )

    if result.contract_version != CONTRACT_VERSION:
        raise PublicationValidationError(
            f"unsupported contract version "
            f"{result.contract_version!r}"
        )

    if (
        result.detected_at_utc.tzinfo is None
        or result.detected_at_utc.utcoffset() is None
    ):
        raise PublicationValidationError(
            "detected_at_utc must contain an offset"
        )

    _require_unique(
        [
            transaction.order_id
            for transaction in result.transactions
        ],
        description="transaction order IDs",
    )

    _require_unique(
        [
            payout.payout_id
            for payout in result.payouts
        ],
        description="payout IDs",
    )

    _require_unique(
        [
            (
                movement.movement_type,
                movement.movement_id,
            )
            for movement in result.expected_movements
        ],
        description="expected movement keys",
    )

    _require_unique(
        [
            (
                exception.rule_id,
                exception.exception_type,
                exception.entity_type,
                exception.entity_id,
            )
            for exception in result.exceptions
        ],
        description="exception rule/entity keys",
    )

    _require_unique(
        [
            (
                status.source_system,
                status.report_type,
                status.business_date,
            )
            for status in result.source_completeness
        ],
        description="source-completeness keys",
    )

    for transaction in result.transactions:
        if transaction.run_id != result.run_id:
            raise PublicationValidationError(
                f"transaction {transaction.order_id} has "
                "a different run_id"
            )

        if transaction.contract_version != CONTRACT_VERSION:
            raise PublicationValidationError(
                f"transaction {transaction.order_id} has "
                "a different contract version"
            )

        expected_variance = (
            transaction.captured_total
            - transaction.expected_collection
        )

        if transaction.collection_variance != expected_variance:
            raise PublicationValidationError(
                f"transaction {transaction.order_id} has "
                "an invalid collection variance"
            )

        expected_lifetime_net = (
            transaction.captured_total
            - transaction.successful_refund_total
            - transaction.expected_fee_total
        )

        if (
            transaction.lifetime_net_collection
            != expected_lifetime_net
        ):
            raise PublicationValidationError(
                f"transaction {transaction.order_id} has "
                "an invalid lifetime net collection"
            )

        if (
            transaction.state
            is ReconciliationState.RECONCILED
            and transaction.exception_codes
        ):
            raise PublicationValidationError(
                f"reconciled transaction "
                f"{transaction.order_id} contains exceptions"
            )

    for payout in result.payouts:
        if payout.run_id != result.run_id:
            raise PublicationValidationError(
                f"payout {payout.payout_id} has "
                "a different run_id"
            )

        expected_provider_variance = (
            payout.reported_net_amount
            - payout.reported_line_total
        )

        if (
            payout.provider_report_variance
            != expected_provider_variance
        ):
            raise PublicationValidationError(
                f"payout {payout.payout_id} has an invalid "
                "provider report variance"
            )

        expected_end_to_end_variance = (
            payout.reported_net_amount
            - payout.expected_payout
        )

        if (
            payout.end_to_end_payout_variance
            != expected_end_to_end_variance
        ):
            raise PublicationValidationError(
                f"payout {payout.payout_id} has an invalid "
                "end-to-end variance"
            )

        if (
            payout.state is ReconciliationState.RECONCILED
            and (
                payout.exception_codes
                or payout.provider_report_variance
                != Decimal("0.00")
                or payout.end_to_end_payout_variance
                != Decimal("0.00")
            )
        ):
            raise PublicationValidationError(
                f"reconciled payout {payout.payout_id} "
                "contains unresolved variance"
            )

    for exception in result.exceptions:
        if exception.run_id != result.run_id:
            raise PublicationValidationError(
                f"exception {exception.entity_id} has "
                "a different run_id"
            )

        if (
            exception.expected_amount is not None
            and exception.actual_amount is not None
        ):
            expected_variance = (
                exception.actual_amount
                - exception.expected_amount
            )

            if exception.variance != expected_variance:
                raise PublicationValidationError(
                    f"exception {exception.rule_id} has "
                    "an invalid variance"
                )
        elif exception.variance is not None:
            raise PublicationValidationError(
                f"exception {exception.rule_id} has variance "
                "without both expected and actual amounts"
            )


class InMemoryPublicationStore:
    """Atomic in-memory publication store for tests.

    This store is useful for golden scenario G20. If validation or the
    before-commit hook fails, the previously published result remains current.
    """

    def __init__(
        self,
        *,
        clock: Clock = _utc_now,
    ) -> None:
        self._clock = clock
        self._current: ReconciliationResult | None = None
        self._lock = threading.Lock()

    @property
    def current(self) -> ReconciliationResult | None:
        with self._lock:
            return self._current

    def publish(
        self,
        candidate: ReconciliationResult,
        *,
        before_commit: BeforeCommitHook | None = None,
    ) -> PublicationReceipt:
        validate_for_publication(candidate)

        payload = result_to_json(
            candidate,
            pretty=False,
        ).encode("utf-8")
        payload_hash = hashlib.sha256(payload).hexdigest()

        with self._lock:
            if before_commit is not None:
                before_commit()

            self._current = candidate

        return PublicationReceipt(
            run_id=candidate.run_id,
            destination="memory",
            payload_sha256=payload_hash,
            byte_count=len(payload),
            published_at_utc=_ensure_clock_utc(self._clock()),
        )


class AtomicFilePublicationStore:
    """Publish JSON through an atomic same-directory file replacement."""

    def __init__(
        self,
        target_path: str | Path,
        *,
        clock: Clock = _utc_now,
    ) -> None:
        self.target_path = Path(target_path)
        self._clock = clock
        self._lock = threading.Lock()

    def publish(
        self,
        candidate: ReconciliationResult,
        *,
        before_commit: BeforeCommitHook | None = None,
    ) -> PublicationReceipt:
        validate_for_publication(candidate)

        payload = result_to_json(
            candidate,
            pretty=True,
        ).encode("utf-8")
        payload_hash = hashlib.sha256(payload).hexdigest()

        parent = self.target_path.parent
        parent.mkdir(parents=True, exist_ok=True)

        temporary_path: Path | None = None

        with self._lock:
            try:
                with tempfile.NamedTemporaryFile(
                    mode="wb",
                    prefix=f".{self.target_path.name}.",
                    suffix=".tmp",
                    dir=parent,
                    delete=False,
                ) as temporary_file:
                    temporary_file.write(payload)
                    temporary_file.flush()
                    os.fsync(temporary_file.fileno())
                    temporary_path = Path(temporary_file.name)

                if before_commit is not None:
                    before_commit()

                os.replace(
                    temporary_path,
                    self.target_path,
                )
                temporary_path = None

                _fsync_directory(parent)

            except Exception:
                if (
                    temporary_path is not None
                    and temporary_path.exists()
                ):
                    temporary_path.unlink()

                raise

        return PublicationReceipt(
            run_id=candidate.run_id,
            destination=str(self.target_path),
            payload_sha256=payload_hash,
            byte_count=len(payload),
            published_at_utc=_ensure_clock_utc(self._clock()),
        )

    def read_current(self) -> dict[str, Any] | None:
        if not self.target_path.exists():
            return None

        with self.target_path.open(
            "r",
            encoding="utf-8",
        ) as published_file:
            value = json.load(published_file)

        if not isinstance(value, dict):
            raise PublicationError(
                "published result is not a JSON object"
            )

        return value


def _ensure_clock_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise PublicationError(
            "publication clock returned a timestamp "
            "without an offset"
        )

    return value.astimezone(timezone.utc)


def _fsync_directory(path: Path) -> None:
    """Persist the directory entry on systems supporting O_DIRECTORY."""

    if not hasattr(os, "O_DIRECTORY"):
        return

    directory_fd = os.open(path, os.O_DIRECTORY)

    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)