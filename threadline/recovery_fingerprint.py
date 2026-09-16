"""Stable serialization and logical financial fingerprints."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
from enum import Enum
from typing import Any
from uuid import UUID


FINANCIAL_FINGERPRINT_VERSION = 2

CENT = Decimal("0.01")
MAX_MONEY = Decimal("9999999999999999.99")


# These fields describe execution or delivery evidence.
# Business dates, financial states, and exception reasons remain visible.
AUDIT_FIELDS = frozenset(
    {
        "run_id",
        "detected_at",
        "detected_at_utc",
        "observed_at",
        "observed_at_utc",
        "received_at_utc",
        "ingested_at_utc",
        "published_at_utc",
        "created_at_utc",
        "receipt_id",
        "batch_id",
        "original_filename",
        "archive_path",
    }
)


LOGICAL_SECTIONS = (
    "transactions",
    "expected_movements",
    "payouts",
    "exceptions",
    "source_completeness",
)


# Normalize known financial amounts whether they arrive as
# Decimal values, integer values, or strings restored from JSON.
MONEY_FIELDS = frozenset(
    {
        "expected_collection",
        "captured_total",
        "collection_variance",
        "successful_refund_total",
        "expected_fee_total",
        "reported_fee_total",
        "lifetime_net_collection",
        "expected_payout",
        "reported_line_total",
        "reported_net_amount",
        "provider_report_variance",
        "end_to_end_payout_variance",
        "signed_amount",
        "expected_amount",
        "actual_amount",
        "variance",
    }
)


class FinancialResultMismatch(AssertionError):
    """Raised when logical financial results differ."""


def json_value(value: Any) -> Any:
    """Serialize the complete result, including its audit evidence."""

    if isinstance(value, Enum):
        return json_value(value.value)

    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: json_value(getattr(value, field.name))
            for field in fields(value)
        }

    if isinstance(value, Mapping):
        return {
            str(key): json_value(item)
            for key, item in value.items()
        }

    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Non-finite Decimal is not publishable")

        return format(value, "f")

    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Naive datetime is not publishable")

        return value.astimezone(timezone.utc).isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, (tuple, list)):
        return [json_value(item) for item in value]

    if isinstance(value, float):
        raise TypeError(
            "Use Decimal instead of float in financial results"
        )

    if value is None or isinstance(value, (str, bool, int)):
        return value

    raise TypeError(
        f"Unsupported JSON value: {type(value).__name__}"
    )


def canonical_json(value: Any) -> str:
    """Encode a JSON-ready document deterministically."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def document_fingerprint(document: Any) -> str:
    """Hash a JSON-ready document without changing its meaning."""

    encoded = canonical_json(document).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _money_string(value: Any, *, field_name: str) -> str | None:
    """Represent exact monetary values consistently as two-decimal strings."""

    # Exception amounts may legitimately be unknown.
    if value is None:
        return None

    if isinstance(value, bool) or isinstance(value, float):
        raise TypeError(
            f"{field_name} must use Decimal, integer, or decimal string"
        )

    if not isinstance(value, (Decimal, int, str)):
        raise TypeError(
            f"Unsupported monetary value for {field_name}"
        )

    try:
        amount = Decimal(value)
    except InvalidOperation as error:
        raise ValueError(
            f"Invalid monetary value for {field_name}"
        ) from error

    if not amount.is_finite():
        raise ValueError(
            f"Non-finite monetary value for {field_name}"
        )

    if amount.copy_abs() > MAX_MONEY:
        raise ValueError(
            f"{field_name} exceeds NUMERIC(18, 2)"
        )

    # Do not depend on the caller's Decimal precision setting.
    with localcontext() as context:
        context.prec = 40
        cents = amount.quantize(CENT)

    # Fingerprinting must not silently round financial output.
    if amount != cents:
        raise ValueError(
            f"{field_name} contains a fractional cent"
        )

    # Negative zero and positive zero have the same financial meaning.
    if cents == 0:
        cents = Decimal("0.00")

    return format(cents, ".2f")


def _logical_value(value: Any) -> Any:
    """Remove audit metadata and normalize financial collections."""

    if isinstance(value, Enum):
        return _logical_value(value.value)

    if is_dataclass(value) and not isinstance(value, type):
        return _logical_value(
            {
                field.name: getattr(value, field.name)
                for field in fields(value)
            }
        )

    if isinstance(value, Mapping):
        normalized = {}

        for key, item in value.items():
            name = str(key)

            if name in AUDIT_FIELDS:
                continue

            if name in MONEY_FIELDS:
                normalized[name] = _money_string(
                    item,
                    field_name=name,
                )
            else:
                normalized[name] = _logical_value(item)

        return normalized

    if isinstance(value, (tuple, list)):
        items = [_logical_value(item) for item in value]

        # Result rows, exception codes, and supporting IDs are
        # unordered collections for this comparison contract.
        #
        # Preserve duplicates: repeated financial output rows
        # must change the fingerprint.
        return sorted(items, key=canonical_json)

    return json_value(value)


def logical_document(result: Any) -> dict[str, Any]:
    """Extract the explicit financial comparison contract."""

    if isinstance(result, Mapping):
        read = result.__getitem__
    else:
        read = lambda name: getattr(result, name)

    return {
        "fingerprint_version": FINANCIAL_FINGERPRINT_VERSION,
        "contract_version": read("contract_version"),
        **{
            section: _logical_value(read(section))
            for section in LOGICAL_SECTIONS
        },
    }


def logical_fingerprint(result: Any) -> str:
    """Hash logical financial results independently of execution metadata."""

    return document_fingerprint(logical_document(result))


def assert_financial_equal(expected: Any, actual: Any) -> None:
    """Compare documents directly and identify differing sections."""

    expected_document = logical_document(expected)
    actual_document = logical_document(actual)

    if expected_document == actual_document:
        return

    differing_sections = [
        section
        for section in expected_document
        if expected_document[section] != actual_document[section]
    ]

    raise FinancialResultMismatch(
        "Financial results differ in: "
        + ", ".join(differing_sections)
    )


def verify_stored_financial_result(
    publication: Mapping[str, Any],
) -> str:
    """Verify a version-2 artifact returned by read_published_result()."""

    payload = publication["result_payload"]
    version = payload.get("financial_fingerprint_version", 1)

    if version != FINANCIAL_FINGERPRINT_VERSION:
        raise ValueError(
            f"Stored fingerprint version {version} is not supported "
            "by this verification function"
        )

    calculated = logical_fingerprint(payload)

    if calculated != publication["logical_fingerprint"]:
        raise FinancialResultMismatch(
            "Stored financial fingerprint does not match result_payload"
        )

    return calculated