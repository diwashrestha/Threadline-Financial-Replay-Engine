"""Threadline financial data contracts.

This module converts untrusted JSON dictionaries into validated, immutable
domain records. Contract violations are raised before records enter the
canonical replay and reconciliation logic.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, ClassVar, Final, TypeAlias, TypeVar

from threadline.money import EUR, MoneyError, parse_money


CONTRACT_VERSION: Final = "threadline_financial_contract_v1"
SUPPORTED_SCHEMA_VERSIONS: Final = frozenset({1})

_SHA256_PATTERN: Final = re.compile(r"[0-9a-f]{64}\Z")


class StringEnum(str, Enum):
    """Enum whose members serialize naturally as strings."""

    def __str__(self) -> str:
        return self.value


class SourceSystem(StringEnum):
    THREADLINE_SHOP = "threadline_shop"
    MOCKPAY = "mockpay"


class EntityType(StringEnum):
    ORDER = "ORDER"
    PAYMENT = "PAYMENT"
    REFUND = "REFUND"
    FEE = "FEE"
    SETTLEMENT_LINE = "SETTLEMENT_LINE"
    PAYOUT = "PAYOUT"


class ReportType(StringEnum):
    ORDERS = "orders"
    PAYMENTS = "payments"
    REFUNDS = "refunds"
    FEES = "fees"
    SETTLEMENT_LINES = "settlement_lines"
    PAYOUTS = "payouts"


class OrderStatus(StringEnum):
    PAYMENT_PENDING = "PAYMENT_PENDING"
    PAID = "PAID"
    CANCELLED = "CANCELLED"


class PaymentStatus(StringEnum):
    FAILED = "FAILED"
    CAPTURED = "CAPTURED"


class RefundStatus(StringEnum):
    FAILED = "FAILED"
    SUCCEEDED = "SUCCEEDED"


class PaymentMethod(StringEnum):
    CARD = "CARD"
    WALLET = "WALLET"


class FeeType(StringEnum):
    PROCESSING = "PROCESSING"


class MovementType(StringEnum):
    CAPTURE = "CAPTURE"
    REFUND = "REFUND"
    FEE = "FEE"


class SourceCompleteness(StringEnum):
    PENDING = "PENDING"
    INCOMPLETE = "INCOMPLETE"
    COMPLETE = "COMPLETE"


class ReconciliationState(StringEnum):
    PENDING = "PENDING"
    INCOMPLETE = "INCOMPLETE"
    RECONCILED = "RECONCILED"
    EXCEPTION = "EXCEPTION"


class EvidenceDisposition(StringEnum):
    ACCEPTED = "ACCEPTED"
    STALE = "STALE"
    DUPLICATE = "DUPLICATE"
    CONFLICTED = "CONFLICTED"
    QUARANTINED = "QUARANTINED"


class ExceptionCode(StringEnum):
    BATCH_CONFLICT = "BATCH_CONFLICT"
    CAPTURE_FOR_NON_PAYABLE_ORDER = "CAPTURE_FOR_NON_PAYABLE_ORDER"
    CONFLICTING_SOURCE_VERSION = "CONFLICTING_SOURCE_VERSION"
    DUPLICATE_SETTLEMENT_MOVEMENT = "DUPLICATE_SETTLEMENT_MOVEMENT"
    EXCESS_REFUND = "EXCESS_REFUND"
    FEE_MISMATCH = "FEE_MISMATCH"
    MISSING_FEE = "MISSING_FEE"
    MISSING_PAYMENT = "MISSING_PAYMENT"
    MISSING_SETTLEMENT_LINE = "MISSING_SETTLEMENT_LINE"
    MULTIPLE_CAPTURE = "MULTIPLE_CAPTURE"
    ORPHAN_PAYMENT = "ORPHAN_PAYMENT"
    ORPHAN_REFUND = "ORPHAN_REFUND"
    PAYMENT_AMOUNT_MISMATCH = "PAYMENT_AMOUNT_MISMATCH"
    PAYOUT_TOTAL_MISMATCH = "PAYOUT_TOTAL_MISMATCH"
    REFUND_CURRENCY_MISMATCH = "REFUND_CURRENCY_MISMATCH"
    SETTLEMENT_LINE_AMOUNT_MISMATCH = (
        "SETTLEMENT_LINE_AMOUNT_MISMATCH"
    )
    SOURCE_REPORT_INVALID = "SOURCE_REPORT_INVALID"
    SOURCE_REPORT_MISSING = "SOURCE_REPORT_MISSING"
    UNEXPECTED_FEE = "UNEXPECTED_FEE"


class ContractViolation(ValueError):
    """A source value failed the Threadline financial contract."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "INVALID_VALUE",
        field: str | None = None,
    ) -> None:
        self.code = code
        self.field = field

        if field is not None:
            message = f"{field}: {message}"

        super().__init__(message)


@dataclass(frozen=True, slots=True, kw_only=True)
class VersionedSourceRecord:
    """Fields shared by versioned source entities."""

    source_version: int

    SOURCE_SYSTEM: ClassVar[SourceSystem]
    ENTITY_TYPE: ClassVar[EntityType]

    @property
    def record_id(self) -> str:
        raise NotImplementedError

    @property
    def identity(self) -> tuple[str, str, str]:
        """Logical identity used by canonical replay."""

        return (
            self.SOURCE_SYSTEM.value,
            self.ENTITY_TYPE.value,
            self.record_id,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class Order(VersionedSourceRecord):
    SOURCE_SYSTEM: ClassVar = SourceSystem.THREADLINE_SHOP
    ENTITY_TYPE: ClassVar = EntityType.ORDER

    order_id: str
    created_at_utc: datetime
    status: OrderStatus
    currency: str
    order_total: Decimal

    @property
    def record_id(self) -> str:
        return self.order_id


@dataclass(frozen=True, slots=True, kw_only=True)
class Payment(VersionedSourceRecord):
    SOURCE_SYSTEM: ClassVar = SourceSystem.MOCKPAY
    ENTITY_TYPE: ClassVar = EntityType.PAYMENT

    payment_id: str
    order_id: str
    attempt_number: int
    payment_method: PaymentMethod
    status: PaymentStatus
    amount: Decimal
    currency: str
    effective_at_utc: datetime
    available_on: date | None

    @property
    def record_id(self) -> str:
        return self.payment_id


@dataclass(frozen=True, slots=True, kw_only=True)
class Refund(VersionedSourceRecord):
    SOURCE_SYSTEM: ClassVar = SourceSystem.MOCKPAY
    ENTITY_TYPE: ClassVar = EntityType.REFUND

    refund_id: str
    payment_id: str
    status: RefundStatus
    amount: Decimal
    currency: str
    effective_at_utc: datetime
    available_on: date | None

    @property
    def record_id(self) -> str:
        return self.refund_id


@dataclass(frozen=True, slots=True, kw_only=True)
class Fee(VersionedSourceRecord):
    SOURCE_SYSTEM: ClassVar = SourceSystem.MOCKPAY
    ENTITY_TYPE: ClassVar = EntityType.FEE

    fee_id: str
    payment_id: str
    fee_type: FeeType
    amount: Decimal
    currency: str
    effective_at_utc: datetime
    available_on: date

    @property
    def record_id(self) -> str:
        return self.fee_id


@dataclass(frozen=True, slots=True, kw_only=True)
class SettlementLine(VersionedSourceRecord):
    SOURCE_SYSTEM: ClassVar = SourceSystem.MOCKPAY
    ENTITY_TYPE: ClassVar = EntityType.SETTLEMENT_LINE

    settlement_line_id: str
    payout_id: str
    movement_type: MovementType
    movement_id: str
    signed_amount: Decimal
    currency: str

    @property
    def record_id(self) -> str:
        return self.settlement_line_id


@dataclass(frozen=True, slots=True, kw_only=True)
class Payout(VersionedSourceRecord):
    SOURCE_SYSTEM: ClassVar = SourceSystem.MOCKPAY
    ENTITY_TYPE: ClassVar = EntityType.PAYOUT

    payout_id: str
    payout_date: date
    currency: str
    reported_net_amount: Decimal

    @property
    def record_id(self) -> str:
        return self.payout_id


@dataclass(frozen=True, slots=True, kw_only=True)
class Manifest:
    """Manifest accompanying one source report."""

    batch_id: str
    source_system: SourceSystem
    report_type: ReportType
    business_date: date
    schema_version: int
    generated_at_utc: datetime
    row_count: int
    sha256: str

    @property
    def identity(self) -> tuple[str, str, str]:
        return (
            self.source_system.value,
            self.report_type.value,
            self.batch_id,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class SourceReportStatus:
    """Completeness result for one expected source report."""

    source_system: SourceSystem
    report_type: ReportType
    business_date: date
    state: SourceCompleteness
    batch_id: str | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class ExceptionRecord:
    """One deterministic reconciliation exception."""

    code: ExceptionCode
    entity_type: str
    entity_id: str
    message: str
    amount: Decimal | None = None
    related_entity_type: str | None = None
    related_entity_id: str | None = None

    @property
    def sort_key(self) -> tuple[str, str, str]:
        return (
            self.code.value,
            self.entity_type,
            self.entity_id,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class QuarantinedRecord:
    """Evidence describing a source record rejected by validation."""

    entity_type: str
    source_id: str | None
    reason_code: str
    reason_detail: str
    raw_payload_json: str

    @property
    def sort_key(self) -> tuple[str, str, str]:
        return (
            self.entity_type,
            self.source_id or "",
            self.reason_code,
        )


FinancialRecord: TypeAlias = (
    Order
    | Payment
    | Refund
    | Fee
    | SettlementLine
    | Payout
)

EnumType = TypeVar("EnumType", bound=StringEnum)


def _require_keys(
    payload: Mapping[str, Any],
    *,
    required: set[str],
    optional: set[str] | None = None,
    record_name: str,
) -> None:
    if not isinstance(payload, Mapping):
        raise ContractViolation(
            f"{record_name} must be a JSON object",
            code="INVALID_TYPE",
        )

    optional = optional or set()
    actual = set(payload)
    missing = required - actual
    unexpected = actual - required - optional

    if missing:
        raise ContractViolation(
            f"missing required fields: {', '.join(sorted(missing))}",
            code="MISSING_FIELD",
        )

    if unexpected:
        raise ContractViolation(
            f"unexpected fields: {', '.join(sorted(unexpected))}",
            code="UNEXPECTED_FIELD",
        )


def _parse_text(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise ContractViolation(
            "must be a string",
            code="INVALID_TYPE",
            field=field,
        )

    if not value or value != value.strip():
        raise ContractViolation(
            "must be non-empty and contain no surrounding whitespace",
            field=field,
        )

    return value


def _parse_positive_integer(value: Any, *, field: str) -> int:
    if type(value) is not int:
        raise ContractViolation(
            "must be an integer",
            code="INVALID_TYPE",
            field=field,
        )

    if value <= 0:
        raise ContractViolation(
            "must be greater than zero",
            field=field,
        )

    return value


def _parse_non_negative_integer(value: Any, *, field: str) -> int:
    if type(value) is not int:
        raise ContractViolation(
            "must be an integer",
            code="INVALID_TYPE",
            field=field,
        )

    if value < 0:
        raise ContractViolation(
            "must be zero or greater",
            field=field,
        )

    return value


def _parse_enum(
    value: Any,
    enum_type: type[EnumType],
    *,
    field: str,
) -> EnumType:
    raw_value = _parse_text(value, field=field)

    try:
        return enum_type(raw_value)
    except ValueError as exc:
        allowed = ", ".join(member.value for member in enum_type)
        raise ContractViolation(
            f"must be one of: {allowed}",
            field=field,
        ) from exc


def _parse_timestamp(value: Any, *, field: str) -> datetime:
    raw_value = _parse_text(value, field=field)

    normalized = (
        f"{raw_value[:-1]}+00:00"
        if raw_value.endswith("Z")
        else raw_value
    )

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ContractViolation(
            "must be a valid ISO-8601 timestamp",
            field=field,
        ) from exc

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ContractViolation(
            "must include an explicit UTC offset",
            field=field,
        )

    return parsed.astimezone(timezone.utc)


def _parse_date(value: Any, *, field: str) -> date:
    raw_value = _parse_text(value, field=field)

    try:
        parsed = date.fromisoformat(raw_value)
    except ValueError as exc:
        raise ContractViolation(
            "must be a valid ISO date in YYYY-MM-DD format",
            field=field,
        ) from exc

    if parsed.isoformat() != raw_value:
        raise ContractViolation(
            "must use canonical YYYY-MM-DD format",
            field=field,
        )

    return parsed


def _parse_currency(value: Any, *, field: str = "currency") -> str:
    currency = _parse_text(value, field=field)

    if currency != EUR:
        raise ContractViolation(
            f"must equal {EUR}",
            code="UNSUPPORTED_CURRENCY",
            field=field,
        )

    return currency


def _parse_money(
    value: Any,
    *,
    field: str,
    positive: bool = False,
    allow_negative: bool = False,
) -> Decimal:
    try:
        amount = parse_money(
            value,
            allow_negative=allow_negative,
        )
    except (MoneyError, TypeError) as exc:
        raise ContractViolation(
            str(exc),
            code="INVALID_MONEY",
            field=field,
        ) from exc

    if positive and amount <= Decimal("0.00"):
        raise ContractViolation(
            "must be greater than zero",
            code="INVALID_MONEY",
            field=field,
        )

    return amount


def parse_order(payload: Mapping[str, Any]) -> Order:
    _require_keys(
        payload,
        required={
            "order_id",
            "created_at_utc",
            "status",
            "currency",
            "order_total",
            "source_version",
        },
        record_name="order",
    )

    return Order(
        order_id=_parse_text(
            payload["order_id"],
            field="order_id",
        ),
        created_at_utc=_parse_timestamp(
            payload["created_at_utc"],
            field="created_at_utc",
        ),
        status=_parse_enum(
            payload["status"],
            OrderStatus,
            field="status",
        ),
        currency=_parse_currency(payload["currency"]),
        order_total=_parse_money(
            payload["order_total"],
            field="order_total",
            positive=True,
        ),
        source_version=_parse_positive_integer(
            payload["source_version"],
            field="source_version",
        ),
    )


def parse_payment(payload: Mapping[str, Any]) -> Payment:
    _require_keys(
        payload,
        required={
            "payment_id",
            "order_id",
            "attempt_number",
            "payment_method",
            "status",
            "amount",
            "currency",
            "effective_at_utc",
            "source_version",
        },
        optional={"available_on"},
        record_name="payment",
    )

    status = _parse_enum(
        payload["status"],
        PaymentStatus,
        field="status",
    )

    if status is PaymentStatus.CAPTURED:
        if "available_on" not in payload:
            raise ContractViolation(
                "is required for CAPTURED payments",
                code="MISSING_FIELD",
                field="available_on",
            )

        available_on = _parse_date(
            payload["available_on"],
            field="available_on",
        )
    else:
        if "available_on" in payload:
            raise ContractViolation(
                "must be absent for FAILED payments",
                field="available_on",
            )

        available_on = None

    return Payment(
        payment_id=_parse_text(
            payload["payment_id"],
            field="payment_id",
        ),
        order_id=_parse_text(
            payload["order_id"],
            field="order_id",
        ),
        attempt_number=_parse_positive_integer(
            payload["attempt_number"],
            field="attempt_number",
        ),
        payment_method=_parse_enum(
            payload["payment_method"],
            PaymentMethod,
            field="payment_method",
        ),
        status=status,
        amount=_parse_money(
            payload["amount"],
            field="amount",
            positive=True,
        ),
        currency=_parse_currency(payload["currency"]),
        effective_at_utc=_parse_timestamp(
            payload["effective_at_utc"],
            field="effective_at_utc",
        ),
        available_on=available_on,
        source_version=_parse_positive_integer(
            payload["source_version"],
            field="source_version",
        ),
    )


def parse_refund(payload: Mapping[str, Any]) -> Refund:
    _require_keys(
        payload,
        required={
            "refund_id",
            "payment_id",
            "status",
            "amount",
            "currency",
            "effective_at_utc",
            "source_version",
        },
        optional={"available_on"},
        record_name="refund",
    )

    status = _parse_enum(
        payload["status"],
        RefundStatus,
        field="status",
    )

    if status is RefundStatus.SUCCEEDED:
        if "available_on" not in payload:
            raise ContractViolation(
                "is required for SUCCEEDED refunds",
                code="MISSING_FIELD",
                field="available_on",
            )

        available_on = _parse_date(
            payload["available_on"],
            field="available_on",
        )
    else:
        if "available_on" in payload:
            raise ContractViolation(
                "must be absent for FAILED refunds",
                field="available_on",
            )

        available_on = None

    return Refund(
        refund_id=_parse_text(
            payload["refund_id"],
            field="refund_id",
        ),
        payment_id=_parse_text(
            payload["payment_id"],
            field="payment_id",
        ),
        status=status,
        amount=_parse_money(
            payload["amount"],
            field="amount",
            positive=True,
        ),
        currency=_parse_currency(payload["currency"]),
        effective_at_utc=_parse_timestamp(
            payload["effective_at_utc"],
            field="effective_at_utc",
        ),
        available_on=available_on,
        source_version=_parse_positive_integer(
            payload["source_version"],
            field="source_version",
        ),
    )


def parse_fee(payload: Mapping[str, Any]) -> Fee:
    _require_keys(
        payload,
        required={
            "fee_id",
            "payment_id",
            "fee_type",
            "amount",
            "currency",
            "effective_at_utc",
            "available_on",
            "source_version",
        },
        record_name="fee",
    )

    return Fee(
        fee_id=_parse_text(
            payload["fee_id"],
            field="fee_id",
        ),
        payment_id=_parse_text(
            payload["payment_id"],
            field="payment_id",
        ),
        fee_type=_parse_enum(
            payload["fee_type"],
            FeeType,
            field="fee_type",
        ),
        amount=_parse_money(
            payload["amount"],
            field="amount",
            positive=True,
        ),
        currency=_parse_currency(payload["currency"]),
        effective_at_utc=_parse_timestamp(
            payload["effective_at_utc"],
            field="effective_at_utc",
        ),
        available_on=_parse_date(
            payload["available_on"],
            field="available_on",
        ),
        source_version=_parse_positive_integer(
            payload["source_version"],
            field="source_version",
        ),
    )


def parse_settlement_line(
    payload: Mapping[str, Any],
) -> SettlementLine:
    _require_keys(
        payload,
        required={
            "settlement_line_id",
            "payout_id",
            "movement_type",
            "movement_id",
            "signed_amount",
            "currency",
            "source_version",
        },
        record_name="settlement line",
    )

    movement_type = _parse_enum(
        payload["movement_type"],
        MovementType,
        field="movement_type",
    )

    signed_amount = _parse_money(
        payload["signed_amount"],
        field="signed_amount",
        allow_negative=True,
    )

    if movement_type is MovementType.CAPTURE:
        if signed_amount <= Decimal("0.00"):
            raise ContractViolation(
                "must be positive for CAPTURE movements",
                code="INVALID_MONEY_SIGN",
                field="signed_amount",
            )
    elif signed_amount >= Decimal("0.00"):
        raise ContractViolation(
            "must be negative for REFUND and FEE movements",
            code="INVALID_MONEY_SIGN",
            field="signed_amount",
        )

    return SettlementLine(
        settlement_line_id=_parse_text(
            payload["settlement_line_id"],
            field="settlement_line_id",
        ),
        payout_id=_parse_text(
            payload["payout_id"],
            field="payout_id",
        ),
        movement_type=movement_type,
        movement_id=_parse_text(
            payload["movement_id"],
            field="movement_id",
        ),
        signed_amount=signed_amount,
        currency=_parse_currency(payload["currency"]),
        source_version=_parse_positive_integer(
            payload["source_version"],
            field="source_version",
        ),
    )


def parse_payout(payload: Mapping[str, Any]) -> Payout:
    _require_keys(
        payload,
        required={
            "payout_id",
            "payout_date",
            "currency",
            "reported_net_amount",
            "source_version",
        },
        record_name="payout",
    )

    return Payout(
        payout_id=_parse_text(
            payload["payout_id"],
            field="payout_id",
        ),
        payout_date=_parse_date(
            payload["payout_date"],
            field="payout_date",
        ),
        currency=_parse_currency(payload["currency"]),
        reported_net_amount=_parse_money(
            payload["reported_net_amount"],
            field="reported_net_amount",
        ),
        source_version=_parse_positive_integer(
            payload["source_version"],
            field="source_version",
        ),
    )


def parse_manifest(payload: Mapping[str, Any]) -> Manifest:
    _require_keys(
        payload,
        required={
            "batch_id",
            "source_system",
            "report_type",
            "business_date",
            "schema_version",
            "generated_at_utc",
            "row_count",
            "sha256",
        },
        record_name="manifest",
    )

    schema_version = _parse_positive_integer(
        payload["schema_version"],
        field="schema_version",
    )

    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        supported = ", ".join(
            str(version)
            for version in sorted(SUPPORTED_SCHEMA_VERSIONS)
        )
        raise ContractViolation(
            f"unsupported schema version; supported versions: {supported}",
            code="UNSUPPORTED_SCHEMA_VERSION",
            field="schema_version",
        )

    checksum = _parse_text(
        payload["sha256"],
        field="sha256",
    )

    if not _SHA256_PATTERN.fullmatch(checksum):
        raise ContractViolation(
            "must contain exactly 64 lowercase hexadecimal characters",
            field="sha256",
        )

    return Manifest(
        batch_id=_parse_text(
            payload["batch_id"],
            field="batch_id",
        ),
        source_system=_parse_enum(
            payload["source_system"],
            SourceSystem,
            field="source_system",
        ),
        report_type=_parse_enum(
            payload["report_type"],
            ReportType,
            field="report_type",
        ),
        business_date=_parse_date(
            payload["business_date"],
            field="business_date",
        ),
        schema_version=schema_version,
        generated_at_utc=_parse_timestamp(
            payload["generated_at_utc"],
            field="generated_at_utc",
        ),
        row_count=_parse_non_negative_integer(
            payload["row_count"],
            field="row_count",
        ),
        sha256=checksum,
    )


RecordParser: TypeAlias = Callable[
    [Mapping[str, Any]],
    FinancialRecord,
]

_RECORD_PARSERS: Final[Mapping[EntityType, RecordParser]] = {
    EntityType.ORDER: parse_order,
    EntityType.PAYMENT: parse_payment,
    EntityType.REFUND: parse_refund,
    EntityType.FEE: parse_fee,
    EntityType.SETTLEMENT_LINE: parse_settlement_line,
    EntityType.PAYOUT: parse_payout,
}


def parse_source_record(
    entity_type: EntityType | str,
    payload: Mapping[str, Any],
) -> FinancialRecord:
    """Validate and parse one financial source record."""

    if not isinstance(entity_type, EntityType):
        try:
            entity_type = EntityType(entity_type)
        except (TypeError, ValueError) as exc:
            allowed = ", ".join(
                member.value for member in EntityType
            )
            raise ContractViolation(
                f"unsupported entity type; expected one of: {allowed}",
                code="UNSUPPORTED_ENTITY_TYPE",
                field="entity_type",
            ) from exc

    return _RECORD_PARSERS[entity_type](payload)


_ID_FIELDS: Final[Mapping[EntityType, str]] = {
    EntityType.ORDER: "order_id",
    EntityType.PAYMENT: "payment_id",
    EntityType.REFUND: "refund_id",
    EntityType.FEE: "fee_id",
    EntityType.SETTLEMENT_LINE: "settlement_line_id",
    EntityType.PAYOUT: "payout_id",
}


def quarantine_from_violation(
    entity_type: EntityType | str,
    payload: Mapping[str, Any],
    violation: ContractViolation,
) -> QuarantinedRecord:
    """Convert a validation failure into deterministic quarantine evidence."""

    if isinstance(entity_type, EntityType):
        entity_name = entity_type.value
        id_field = _ID_FIELDS[entity_type]
    else:
        entity_name = str(entity_type)
        id_field = None

    candidate_id = payload.get(id_field) if id_field else None
    source_id = (
        candidate_id
        if isinstance(candidate_id, str)
        else None
    )

    raw_payload_json = json.dumps(
        dict(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=repr,
    )

    return QuarantinedRecord(
        entity_type=entity_name,
        source_id=source_id,
        reason_code=violation.code,
        reason_detail=str(violation),
        raw_payload_json=raw_payload_json,
    )