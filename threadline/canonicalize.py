"""Deterministic canonicalization for Threadline financial records.

Canonicalization decides which version of each logical source entity is
financially active. It does not calculate reconciliation totals.

Rules:

1. Logical identity is:
   (source_system, entity_type, source_id)
2. The highest source version wins.
3. Repeated copies of the winning payload are duplicates.
4. Different payloads with the same winning version create a conflict.
5. A conflicted winning version produces no canonical record.
6. Lower versions are stale.
7. Results do not depend on arrival order.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Final, TypeAlias

from threadline.contracts import (
    EvidenceDisposition,
    ExceptionCode,
    ExceptionRecord,
    FinancialRecord,
)


Identity: TypeAlias = tuple[str, str, str]


class CanonicalizationError(ValueError):
    """Base class for canonicalization failures."""


class InvalidReceiptError(CanonicalizationError):
    """Raised when a receipt identifier is invalid."""


class ReceiptConflictError(CanonicalizationError):
    """Raised when one receipt ID refers to different source records."""


def _json_value(value: Any) -> Any:
    """Convert domain values into deterministic JSON-compatible values."""

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, Decimal):
        if not value.is_finite():
            raise CanonicalizationError(
                "cannot canonicalize a non-finite Decimal"
            )
        return format(value, "f")

    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise CanonicalizationError(
                "cannot canonicalize a timestamp without an offset"
            )

        normalized = value.astimezone(timezone.utc)
        return normalized.isoformat().replace("+00:00", "Z")

    if isinstance(value, date):
        return value.isoformat()

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

    raise CanonicalizationError(
        f"unsupported canonical value type: {type(value).__name__}"
    )


def canonical_record_dict(
    record: FinancialRecord,
) -> dict[str, Any]:
    """Return the normalized business payload used for hashing."""

    payload = {
        record_field.name: _json_value(
            getattr(record, record_field.name)
        )
        for record_field in fields(record)
    }

    payload["_source_system"] = record.SOURCE_SYSTEM.value
    payload["_entity_type"] = record.ENTITY_TYPE.value

    return payload


def canonical_record_json(record: FinancialRecord) -> str:
    """Serialize a record into stable JSON."""

    return json.dumps(
        canonical_record_dict(record),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def calculate_payload_hash(record: FinancialRecord) -> str:
    """Calculate a stable SHA-256 hash for a normalized record."""

    canonical_json = canonical_record_json(record)

    return hashlib.sha256(
        canonical_json.encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class RecordEnvelope:
    """One observed source-record receipt.

    ``receipt_id`` should identify a stable physical observation, such as:

        payments_2026-09-14.json:42

    Replaying the same receipt ID with the same payload is idempotent.
    Reusing that receipt ID for a different payload is an ingestion error.
    """

    receipt_id: str
    record: FinancialRecord
    payload_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.receipt_id, str):
            raise InvalidReceiptError(
                "receipt_id must be a string"
            )

        if (
            not self.receipt_id
            or self.receipt_id != self.receipt_id.strip()
        ):
            raise InvalidReceiptError(
                "receipt_id must be non-empty and contain no "
                "surrounding whitespace"
            )

        object.__setattr__(
            self,
            "payload_hash",
            calculate_payload_hash(self.record),
        )

    @property
    def identity(self) -> Identity:
        return self.record.identity

    @property
    def source_version(self) -> int:
        return self.record.source_version

    @property
    def fingerprint(self) -> tuple[
        Identity,
        int,
        str,
    ]:
        return (
            self.identity,
            self.source_version,
            self.payload_hash,
        )


@dataclass(frozen=True, slots=True)
class EvidenceRecord:
    """Classification assigned to one unique receipt."""

    receipt_id: str
    identity: Identity
    source_version: int
    payload_hash: str
    disposition: EvidenceDisposition
    reason: str

    @property
    def sort_key(self) -> tuple[
        Identity,
        int,
        str,
        str,
    ]:
        return (
            self.identity,
            self.source_version,
            self.payload_hash,
            self.receipt_id,
        )


@dataclass(frozen=True, slots=True)
class CanonicalConflict:
    """A winning source version contains different payloads."""

    identity: Identity
    source_version: int
    payload_hashes: tuple[str, ...]
    receipt_ids: tuple[str, ...]

    @property
    def sort_key(self) -> tuple[Identity, int]:
        return self.identity, self.source_version


@dataclass(frozen=True, slots=True)
class CanonicalizationResult:
    """Complete deterministic canonicalization result."""

    canonical_records: tuple[FinancialRecord, ...]
    evidence: tuple[EvidenceRecord, ...]
    conflicts: tuple[CanonicalConflict, ...]

    @property
    def accepted_count(self) -> int:
        return sum(
            item.disposition is EvidenceDisposition.ACCEPTED
            for item in self.evidence
        )

    @property
    def duplicate_count(self) -> int:
        return sum(
            item.disposition is EvidenceDisposition.DUPLICATE
            for item in self.evidence
        )

    @property
    def stale_count(self) -> int:
        return sum(
            item.disposition is EvidenceDisposition.STALE
            for item in self.evidence
        )

    @property
    def conflicted_count(self) -> int:
        return sum(
            item.disposition is EvidenceDisposition.CONFLICTED
            for item in self.evidence
        )

    def canonical_by_identity(
        self,
    ) -> dict[Identity, FinancialRecord]:
        return {
            record.identity: record
            for record in self.canonical_records
        }


def _deduplicate_receipts(
    envelopes: Iterable[RecordEnvelope],
) -> dict[str, RecordEnvelope]:
    """Collapse exact receipt replays and reject receipt-ID conflicts."""

    unique: dict[str, RecordEnvelope] = {}

    for envelope in envelopes:
        existing = unique.get(envelope.receipt_id)

        if existing is None:
            unique[envelope.receipt_id] = envelope
            continue

        if existing.fingerprint != envelope.fingerprint:
            raise ReceiptConflictError(
                f"receipt {envelope.receipt_id!r} refers to "
                "different source records"
            )

        # Same receipt ID and same payload means the physical receipt was
        # replayed. It is ignored rather than counted as another observation.

    return unique


def _evidence(
    envelope: RecordEnvelope,
    disposition: EvidenceDisposition,
    reason: str,
) -> EvidenceRecord:
    return EvidenceRecord(
        receipt_id=envelope.receipt_id,
        identity=envelope.identity,
        source_version=envelope.source_version,
        payload_hash=envelope.payload_hash,
        disposition=disposition,
        reason=reason,
    )


def canonicalize(
    envelopes: Iterable[RecordEnvelope],
) -> CanonicalizationResult:
    """Select canonical source records independently of arrival order."""

    unique_receipts = _deduplicate_receipts(envelopes)

    grouped: dict[
        Identity,
        dict[int, dict[str, list[RecordEnvelope]]],
    ] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )

    for envelope in unique_receipts.values():
        grouped[envelope.identity][
            envelope.source_version
        ][envelope.payload_hash].append(envelope)

    canonical_records: list[FinancialRecord] = []
    evidence_records: list[EvidenceRecord] = []
    conflicts: list[CanonicalConflict] = []

    for identity in sorted(grouped):
        versions = grouped[identity]
        winning_version = max(versions)

        # Every receipt below the highest observed version is stale.
        for source_version in sorted(versions):
            if source_version == winning_version:
                continue

            hash_buckets = versions[source_version]

            for payload_hash in sorted(hash_buckets):
                for envelope in sorted(
                    hash_buckets[payload_hash],
                    key=lambda item: item.receipt_id,
                ):
                    evidence_records.append(
                        _evidence(
                            envelope,
                            EvidenceDisposition.STALE,
                            (
                                f"version {source_version} was superseded "
                                f"by version {winning_version}"
                            ),
                        )
                    )

        winning_buckets = versions[winning_version]

        # More than one hash at the winning version means that the source
        # contradicted itself. No record is safe to use financially.
        if len(winning_buckets) > 1:
            conflicting_envelopes = sorted(
                (
                    envelope
                    for bucket in winning_buckets.values()
                    for envelope in bucket
                ),
                key=lambda item: (
                    item.payload_hash,
                    item.receipt_id,
                ),
            )

            conflicts.append(
                CanonicalConflict(
                    identity=identity,
                    source_version=winning_version,
                    payload_hashes=tuple(
                        sorted(winning_buckets)
                    ),
                    receipt_ids=tuple(
                        envelope.receipt_id
                        for envelope in conflicting_envelopes
                    ),
                )
            )

            for envelope in conflicting_envelopes:
                evidence_records.append(
                    _evidence(
                        envelope,
                        EvidenceDisposition.CONFLICTED,
                        (
                            f"version {winning_version} contains "
                            f"{len(winning_buckets)} different payloads"
                        ),
                    )
                )

            continue

        # Exactly one payload is present at the winning version.
        winning_hash = next(iter(winning_buckets))
        winning_envelopes = sorted(
            winning_buckets[winning_hash],
            key=lambda item: item.receipt_id,
        )

        # Selecting by receipt ID keeps the representative deterministic.
        accepted = winning_envelopes[0]
        canonical_records.append(accepted.record)

        evidence_records.append(
            _evidence(
                accepted,
                EvidenceDisposition.ACCEPTED,
                f"selected canonical version {winning_version}",
            )
        )

        for duplicate in winning_envelopes[1:]:
            evidence_records.append(
                _evidence(
                    duplicate,
                    EvidenceDisposition.DUPLICATE,
                    (
                        f"duplicates accepted receipt "
                        f"{accepted.receipt_id!r}"
                    ),
                )
            )

    canonical_records.sort(
        key=lambda record: record.identity
    )
    evidence_records.sort(
        key=lambda item: item.sort_key
    )
    conflicts.sort(
        key=lambda item: item.sort_key
    )

    return CanonicalizationResult(
        canonical_records=tuple(canonical_records),
        evidence=tuple(evidence_records),
        conflicts=tuple(conflicts),
    )


def conflict_exceptions(
    result: CanonicalizationResult,
) -> tuple[ExceptionRecord, ...]:
    """Convert canonical conflicts into reconciliation exceptions."""

    exceptions = [
        ExceptionRecord(
            code=ExceptionCode.CONFLICTING_SOURCE_VERSION,
            entity_type=conflict.identity[1],
            entity_id=conflict.identity[2],
            message=(
                f"source version {conflict.source_version} has "
                f"{len(conflict.payload_hashes)} different payloads"
            ),
        )
        for conflict in result.conflicts
    ]

    return tuple(
        sorted(
            exceptions,
            key=lambda item: item.sort_key,
        )
    )


class ReplayLedger:
    """Incrementally accumulated source evidence.

    The ledger stores unique physical receipts. Calling ``result()`` performs
    canonicalization over all evidence received so far.

    Applying the same batch repeatedly is idempotent. A failed batch application
    does not partially change the ledger.
    """

    def __init__(
        self,
        envelopes: Iterable[RecordEnvelope] = (),
    ) -> None:
        self._receipts: dict[str, RecordEnvelope] = {}
        self.apply(envelopes)

    def apply(
        self,
        envelopes: Iterable[RecordEnvelope],
    ) -> int:
        """Atomically add a batch of receipts.

        Returns the number of new unique receipts. Exact replays return zero.
        """

        candidate = dict(self._receipts)
        added = 0

        for envelope in envelopes:
            existing = candidate.get(envelope.receipt_id)

            if existing is None:
                candidate[envelope.receipt_id] = envelope
                added += 1
                continue

            if existing.fingerprint != envelope.fingerprint:
                raise ReceiptConflictError(
                    f"receipt {envelope.receipt_id!r} was already "
                    "stored with a different source record"
                )

            # Exact replay of an existing physical receipt: no change.

        self._receipts = candidate
        return added

    def result(self) -> CanonicalizationResult:
        return canonicalize(self._receipts.values())

    def receipts(
        self,
    ) -> tuple[RecordEnvelope, ...]:
        return tuple(
            self._receipts[receipt_id]
            for receipt_id in sorted(self._receipts)
        )

    def __len__(self) -> int:
        return len(self._receipts)