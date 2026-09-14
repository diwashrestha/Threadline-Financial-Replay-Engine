"""System-level invariants for the Threadline replay engine.

These tests verify properties that must remain true across every
financial scenario, rather than checking one specific scenario.
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import MISSING, fields, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping

import pytest

from threadline.canonicalize import (
    RecordEnvelope,
    ReplayLedger,
    canonicalize,
)
from threadline.contracts import (
    ContractViolation,
    EntityType,
    EvidenceDisposition,
    parse_source_record,
    quarantine_from_violation,
)


FIXED_RECEIVED_AT = datetime(
    2026,
    9,
    14,
    10,
    0,
    tzinfo=timezone.utc,
)


# ------------------------------------------------------------------
# Test data builders
# ------------------------------------------------------------------


def _payment(
    *,
    payment_id: str = "PAY-INV-001",
    order_id: str = "ORD-INV-001",
    amount: str = "100.00",
    source_version: int = 1,
) -> dict[str, Any]:
    return {
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
    }


def _payload_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")

    return hashlib.sha256(encoded).hexdigest()


def _envelope(
    receipt_id: str,
    entity_type: EntityType,
    payload: Mapping[str, Any],
) -> RecordEnvelope:
    """Construct a RecordEnvelope using the Stage 2 contract.

    The mapping supports the common field names used by the
    canonicalization implementation supplied earlier.
    """

    record = parse_source_record(entity_type, payload)

    available_values: dict[str, Any] = {
        "receipt_id": receipt_id,
        "entity_type": entity_type,
        "record": record,
        "source_record": record,
        "parsed_record": record,
        "source_id": record.record_id,
        "record_id": record.record_id,
        "source_version": record.source_version,
        "version": record.source_version,
        "payload_hash": _payload_hash(payload),
        "record_hash": _payload_hash(payload),
        "raw_payload": dict(payload),
        "payload": dict(payload),
        "received_at_utc": FIXED_RECEIVED_AT,
        "ingested_at_utc": FIXED_RECEIVED_AT,
    }

    constructor_values: dict[str, Any] = {}

    for field in fields(RecordEnvelope):
        # Dataclass fields declared with init=False are calculated by
        # RecordEnvelope itself and must not be constructor arguments.
        if not field.init:
            continue

        if field.name in available_values:
            constructor_values[field.name] = available_values[field.name]
            continue

        has_default = (
            field.default is not MISSING
            or field.default_factory is not MISSING
        )

        if not has_default:
            raise AssertionError(
                "Update tests/test_invariants.py::_envelope for "
                f"required constructor field {field.name!r}"
            )

    return RecordEnvelope(**constructor_values)


def _record(value: Any) -> Any:
    """Unwrap an accepted envelope if the result stores envelopes."""

    return getattr(
        value,
        "record",
        getattr(value, "source_record", value),
    )


def _accepted(result: Any) -> tuple[Any, ...]:
    """Return accepted canonical records across minor API variations."""

    for attribute in (
        "canonical_records",
        "accepted_records",
        "records",
    ):
        if hasattr(result, attribute):
            return tuple(getattr(result, attribute))

    raise AssertionError(
        "CanonicalizationResult must expose canonical_records "
        "or accepted_records"
    )


def _evidence(result: Any) -> tuple[Any, ...]:
    for attribute in (
        "evidence",
        "evidence_records",
        "decisions",
    ):
        if hasattr(result, attribute):
            return tuple(getattr(result, attribute))

    raise AssertionError(
        "CanonicalizationResult must expose evidence records"
    )


def _disposition(evidence: Any) -> str:
    value = evidence.disposition

    if isinstance(value, Enum):
        return str(value.value)

    return str(value)


def _apply_batch(
    ledger: ReplayLedger,
    batch: list[RecordEnvelope],
) -> Any:
    """Keep one adapter point for the ReplayLedger method name."""

    if hasattr(ledger, "apply"):
        return ledger.apply(batch)

    if hasattr(ledger, "apply_batch"):
        return ledger.apply_batch(batch)

    raise AssertionError(
        "ReplayLedger must expose apply() or apply_batch()"
    )

def _ledger_snapshot(ledger: ReplayLedger) -> Any:
    """Return the current canonical result stored by the ledger."""

    for attribute in (
        "snapshot",
        "current_result",
        "result",
        "canonicalization_result",
    ):
        if not hasattr(ledger, attribute):
            continue

        value = getattr(ledger, attribute)
        result = value() if callable(value) else value

        if any(
            hasattr(result, result_attribute)
            for result_attribute in (
                "canonical_records",
                "accepted_records",
                "records",
            )
        ):
            return result

    raise AssertionError(
        "ReplayLedger needs a public snapshot/result method "
        "that returns CanonicalizationResult"
    )


# ------------------------------------------------------------------
# Stable result serialization
# ------------------------------------------------------------------


def _json_value(value: Any) -> Any:
    if is_dataclass(value):
        return {
            field.name: _json_value(getattr(value, field.name))
            for field in fields(value)
        }

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, Decimal):
        return format(value, ".2f")

    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()

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

    if isinstance(value, (set, frozenset)):
        normalized = [_json_value(item) for item in value]

        return sorted(
            normalized,
            key=lambda item: json.dumps(
                item,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )

    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _json_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _canonical_snapshot(result: Any) -> dict[str, Any]:
    """Capture only externally meaningful canonicalization output."""

    accepted = sorted(
        (_json_value(item) for item in _accepted(result)),
        key=lambda item: json.dumps(
            item,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )

    evidence = sorted(
        (_json_value(item) for item in _evidence(result)),
        key=lambda item: json.dumps(
            item,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )

    return {
        "accepted": accepted,
        "evidence": evidence,
    }


def _accepted_total(result: Any) -> Decimal:
    return sum(
        (
            _record(item).amount
            for item in _accepted(result)
            if hasattr(_record(item), "amount")
        ),
        Decimal("0.00"),
    )


# ------------------------------------------------------------------
# I01: deterministic replay
# ------------------------------------------------------------------


def test_deterministic_replay_produces_identical_canonical_json():
    envelopes = [
        _envelope(
            "receipt-payment-v1",
            EntityType.PAYMENT,
            _payment(source_version=1, amount="100.00"),
        ),
        _envelope(
            "receipt-payment-v2",
            EntityType.PAYMENT,
            _payment(source_version=2, amount="120.00"),
        ),
    ]

    first = canonicalize(envelopes)
    second = canonicalize(envelopes)

    assert _canonical_json(
        _canonical_snapshot(first)
    ) == _canonical_json(
        _canonical_snapshot(second)
    )


# ------------------------------------------------------------------
# I02: duplicate safety
# ------------------------------------------------------------------


def test_identical_duplicate_does_not_change_accepted_total():
    payload = _payment(amount="100.00")

    original = canonicalize(
        [
            _envelope(
                "receipt-original",
                EntityType.PAYMENT,
                payload,
            )
        ]
    )

    with_duplicate = canonicalize(
        [
            _envelope(
                "receipt-original",
                EntityType.PAYMENT,
                payload,
            ),
            _envelope(
                "receipt-duplicate",
                EntityType.PAYMENT,
                payload,
            ),
        ]
    )

    assert _accepted_total(original) == Decimal("100.00")
    assert _accepted_total(with_duplicate) == Decimal("100.00")
    assert len(_accepted(with_duplicate)) == 1

    dispositions = {
        _disposition(item)
        for item in _evidence(with_duplicate)
    }

    assert EvidenceDisposition.DUPLICATE.value in dispositions


# ------------------------------------------------------------------
# I03: arrival-order independence
# ------------------------------------------------------------------


@pytest.mark.parametrize("seed", range(20))
def test_arrival_order_does_not_change_canonical_result(seed: int):
    envelopes = [
        _envelope(
            "receipt-v1",
            EntityType.PAYMENT,
            _payment(source_version=1, amount="100.00"),
        ),
        _envelope(
            "receipt-v2",
            EntityType.PAYMENT,
            _payment(source_version=2, amount="120.00"),
        ),
        _envelope(
            "receipt-v2-duplicate",
            EntityType.PAYMENT,
            _payment(source_version=2, amount="120.00"),
        ),
        _envelope(
            "receipt-other-payment",
            EntityType.PAYMENT,
            _payment(
                payment_id="PAY-INV-002",
                order_id="ORD-INV-002",
                source_version=1,
                amount="55.00",
            ),
        ),
    ]

    expected = _canonical_json(
        _canonical_snapshot(canonicalize(envelopes))
    )

    shuffled = list(envelopes)
    random.Random(seed).shuffle(shuffled)

    actual = _canonical_json(
        _canonical_snapshot(canonicalize(shuffled))
    )

    assert actual == expected


# ------------------------------------------------------------------
# I04: correction precedence
# ------------------------------------------------------------------


def test_higher_version_replaces_lower_version():
    result = canonicalize(
        [
            _envelope(
                "receipt-v1",
                EntityType.PAYMENT,
                _payment(
                    source_version=1,
                    amount="100.00",
                ),
            ),
            _envelope(
                "receipt-v2",
                EntityType.PAYMENT,
                _payment(
                    source_version=2,
                    amount="125.00",
                ),
            ),
        ]
    )

    accepted = [
        _record(item)
        for item in _accepted(result)
    ]

    assert len(accepted) == 1
    assert accepted[0].payment_id == "PAY-INV-001"
    assert accepted[0].source_version == 2
    assert accepted[0].amount == Decimal("125.00")

    dispositions = [
        _disposition(item)
        for item in _evidence(result)
    ]

    assert EvidenceDisposition.ACCEPTED.value in dispositions
    assert EvidenceDisposition.STALE.value in dispositions


# ------------------------------------------------------------------
# I05: conflict safety
# ------------------------------------------------------------------


def test_conflicting_same_version_is_excluded():
    result = canonicalize(
        [
            _envelope(
                "receipt-conflict-a",
                EntityType.PAYMENT,
                _payment(
                    source_version=3,
                    amount="100.00",
                ),
            ),
            _envelope(
                "receipt-conflict-b",
                EntityType.PAYMENT,
                _payment(
                    source_version=3,
                    amount="140.00",
                ),
            ),
        ]
    )

    accepted_payment_ids = {
        _record(item).payment_id
        for item in _accepted(result)
        if hasattr(_record(item), "payment_id")
    }

    assert "PAY-INV-001" not in accepted_payment_ids

    dispositions = [
        _disposition(item)
        for item in _evidence(result)
    ]

    assert dispositions.count(
        EvidenceDisposition.CONFLICTED.value
    ) == 2


# ------------------------------------------------------------------
# I06: incremental replay equals full rebuild
# ------------------------------------------------------------------


def test_incremental_replay_equals_full_rebuild():
    first_batch = [
        _envelope(
            "receipt-v1",
            EntityType.PAYMENT,
            _payment(
                source_version=1,
                amount="100.00",
            ),
        ),
        _envelope(
            "receipt-payment-2",
            EntityType.PAYMENT,
            _payment(
                payment_id="PAY-INV-002",
                order_id="ORD-INV-002",
                amount="50.00",
                source_version=1,
            ),
        ),
    ]

    second_batch = [
        _envelope(
            "receipt-v2",
            EntityType.PAYMENT,
            _payment(
                source_version=2,
                amount="125.00",
            ),
        ),
        _envelope(
            "receipt-payment-2-duplicate",
            EntityType.PAYMENT,
            _payment(
                payment_id="PAY-INV-002",
                order_id="ORD-INV-002",
                amount="50.00",
                source_version=1,
            ),
        ),
    ]

    ledger = ReplayLedger()

    first_applied_count = _apply_batch(
        ledger,
        first_batch,
    )

    second_applied_count = _apply_batch(
        ledger,
        second_batch,
    )

    assert first_applied_count == 2
    assert second_applied_count == 2

    incremental_result = _ledger_snapshot(ledger)

    rebuild_result = canonicalize(
        first_batch + second_batch
    )

    assert _canonical_json(
        _canonical_snapshot(incremental_result)
    ) == _canonical_json(
        _canonical_snapshot(rebuild_result)
    )

# ------------------------------------------------------------------
# I07: evidence conservation
# ------------------------------------------------------------------


def test_every_input_has_exactly_one_terminal_disposition():
    valid_envelopes = [
        # Stale after version 2 arrives.
        _envelope(
            "receipt-a-v1",
            EntityType.PAYMENT,
            _payment(
                payment_id="PAY-A",
                order_id="ORD-A",
                amount="100.00",
                source_version=1,
            ),
        ),
        # One of the conflicting winning-version records.
        _envelope(
            "receipt-a-v2-first",
            EntityType.PAYMENT,
            _payment(
                payment_id="PAY-A",
                order_id="ORD-A",
                amount="120.00",
                source_version=2,
            ),
        ),
        # Identical receipt payload at the winning version.
        _envelope(
            "receipt-a-v2-duplicate",
            EntityType.PAYMENT,
            _payment(
                payment_id="PAY-A",
                order_id="ORD-A",
                amount="120.00",
                source_version=2,
            ),
        ),
        # Conflicting payload at the same winning version.
        _envelope(
            "receipt-a-v2-conflict",
            EntityType.PAYMENT,
            _payment(
                payment_id="PAY-A",
                order_id="ORD-A",
                amount="140.00",
                source_version=2,
            ),
        ),
        # Independent accepted payment.
        _envelope(
            "receipt-b-v1",
            EntityType.PAYMENT,
            _payment(
                payment_id="PAY-B",
                order_id="ORD-B",
                amount="75.00",
                source_version=1,
            ),
        ),
    ]

    malformed_payload = _payment(
        payment_id="PAY-BAD",
    )
    malformed_payload["amount"] = "one hundred"

    try:
        parse_source_record(
            EntityType.PAYMENT,
            malformed_payload,
        )
    except ContractViolation as violation:
        quarantined = quarantine_from_violation(
            EntityType.PAYMENT,
            malformed_payload,
            violation,
        )
    else:
        raise AssertionError(
            "Malformed payment was unexpectedly accepted"
        )

    result = canonicalize(valid_envelopes)
    evidence = _evidence(result)

    # Every validly parsed input must receive exactly one canonical
    # disposition. The contract-invalid input becomes one quarantine
    # record before canonicalization.
    assert len(evidence) == len(valid_envelopes)
    assert len(evidence) + 1 == len(valid_envelopes) + 1
    assert quarantined.source_id == "PAY-BAD"

    allowed = {
        disposition.value
        for disposition in EvidenceDisposition
    }

    actual = [
        _disposition(item)
        for item in evidence
    ]

    assert all(
        disposition in allowed
        for disposition in actual
    )

    # Receipt IDs prove that no parsed input disappeared and none
    # received two terminal decisions.
    evidence_receipt_ids = [
        item.receipt_id
        for item in evidence
    ]

    assert len(evidence_receipt_ids) == len(
        set(evidence_receipt_ids)
    )

    assert set(evidence_receipt_ids) == {
        envelope.receipt_id
        for envelope in valid_envelopes
    }