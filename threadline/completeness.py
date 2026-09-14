"""Source-report completeness for the Threadline replay engine.

A source report is COMPLETE only when:

1. Its data file is visible.
2. Its manifest is visible and structurally valid.
3. File and manifest identities agree.
4. Row counts agree.
5. SHA-256 checksums agree.

Before the report deadline, unavailable or invalid evidence is PENDING.
At or after the deadline, unavailable or invalid evidence is INCOMPLETE.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Final
from zoneinfo import ZoneInfo

from threadline.contracts import (
    ExceptionCode,
    ExceptionRecord,
    Manifest,
    ReportType,
    SourceCompleteness,
    SourceReportStatus,
    SourceSystem,
)


BERLIN: Final = ZoneInfo("Europe/Berlin")

_SHA256_PATTERN: Final = re.compile(r"[0-9a-f]{64}\Z")


class CompletenessError(ValueError):
    """Raised when completeness evidence itself is malformed."""


class CompletenessIssueCode(str, Enum):
    DATA_FILE_MISSING = "DATA_FILE_MISSING"
    MANIFEST_MISSING = "MANIFEST_MISSING"
    MANIFEST_INVALID = "MANIFEST_INVALID"

    DATA_SOURCE_MISMATCH = "DATA_SOURCE_MISMATCH"
    DATA_REPORT_TYPE_MISMATCH = "DATA_REPORT_TYPE_MISMATCH"
    DATA_BUSINESS_DATE_MISMATCH = "DATA_BUSINESS_DATE_MISMATCH"

    MANIFEST_SOURCE_MISMATCH = "MANIFEST_SOURCE_MISMATCH"
    MANIFEST_REPORT_TYPE_MISMATCH = "MANIFEST_REPORT_TYPE_MISMATCH"
    MANIFEST_BUSINESS_DATE_MISMATCH = (
        "MANIFEST_BUSINESS_DATE_MISMATCH"
    )

    BATCH_ID_MISMATCH = "BATCH_ID_MISMATCH"
    ROW_COUNT_MISMATCH = "ROW_COUNT_MISMATCH"
    CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH"
    MANIFEST_GENERATED_AFTER_RECEIPT = (
        "MANIFEST_GENERATED_AFTER_RECEIPT"
    )


REPORT_DEADLINES: Final[Mapping[ReportType, time]] = MappingProxyType(
    {
        ReportType.ORDERS: time(hour=1),
        ReportType.PAYMENTS: time(hour=2),
        ReportType.REFUNDS: time(hour=2),
        ReportType.FEES: time(hour=12),
        ReportType.SETTLEMENT_LINES: time(hour=12),
        ReportType.PAYOUTS: time(hour=12),
    }
)


REPORT_SOURCES: Final[Mapping[ReportType, SourceSystem]] = MappingProxyType(
    {
        ReportType.ORDERS: SourceSystem.THREADLINE_SHOP,
        ReportType.PAYMENTS: SourceSystem.MOCKPAY,
        ReportType.REFUNDS: SourceSystem.MOCKPAY,
        ReportType.FEES: SourceSystem.MOCKPAY,
        ReportType.SETTLEMENT_LINES: SourceSystem.MOCKPAY,
        ReportType.PAYOUTS: SourceSystem.MOCKPAY,
    }
)


EXPECTED_REPORTS: Final = tuple(
    sorted(
        REPORT_DEADLINES,
        key=lambda report_type: report_type.value,
    )
)


_MISSING_ISSUES: Final = frozenset(
    {
        CompletenessIssueCode.DATA_FILE_MISSING,
        CompletenessIssueCode.MANIFEST_MISSING,
    }
)


def _require_aware_datetime(
    value: datetime,
    *,
    field_name: str,
) -> datetime:
    if not isinstance(value, datetime):
        raise CompletenessError(
            f"{field_name} must be a datetime"
        )

    if value.tzinfo is None or value.utcoffset() is None:
        raise CompletenessError(
            f"{field_name} must include an explicit UTC offset"
        )

    return value.astimezone(timezone.utc)


def _validate_sha256(
    value: str,
    *,
    field_name: str,
) -> str:
    if not isinstance(value, str):
        raise CompletenessError(
            f"{field_name} must be a string"
        )

    if not _SHA256_PATTERN.fullmatch(value):
        raise CompletenessError(
            f"{field_name} must contain exactly 64 lowercase "
            "hexadecimal characters"
        )

    return value


def sha256_bytes(content: bytes) -> str:
    """Return the lowercase SHA-256 digest of source bytes."""

    if not isinstance(content, bytes):
        raise TypeError("content must be bytes")

    return hashlib.sha256(content).hexdigest()


def sha256_file(path: str | Path) -> str:
    """Calculate a file checksum without loading the whole file into memory."""

    digest = hashlib.sha256()

    with Path(path).open("rb") as source_file:
        for block in iter(
            lambda: source_file.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class DataFileEvidence:
    """Metadata observed after parsing one source data file."""

    batch_id: str
    source_system: SourceSystem
    report_type: ReportType
    business_date: date
    received_at_utc: datetime
    parsed_row_count: int
    sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.batch_id, str)
            or not self.batch_id
            or self.batch_id != self.batch_id.strip()
        ):
            raise CompletenessError(
                "batch_id must be a non-empty canonical string"
            )

        if not isinstance(self.source_system, SourceSystem):
            raise CompletenessError(
                "source_system must be a SourceSystem"
            )

        if not isinstance(self.report_type, ReportType):
            raise CompletenessError(
                "report_type must be a ReportType"
            )

        if not isinstance(self.business_date, date):
            raise CompletenessError(
                "business_date must be a date"
            )

        if (
            type(self.parsed_row_count) is not int
            or self.parsed_row_count < 0
        ):
            raise CompletenessError(
                "parsed_row_count must be a non-negative integer"
            )

        _require_aware_datetime(
            self.received_at_utc,
            field_name="received_at_utc",
        )
        _validate_sha256(
            self.sha256,
            field_name="sha256",
        )


@dataclass(frozen=True, slots=True)
class ManifestEvidence:
    """Either a valid parsed manifest or a failed manifest observation."""

    received_at_utc: datetime
    manifest: Manifest | None = None
    validation_error: str | None = None

    def __post_init__(self) -> None:
        _require_aware_datetime(
            self.received_at_utc,
            field_name="manifest received_at_utc",
        )

        has_manifest = self.manifest is not None
        has_error = self.validation_error is not None

        if has_manifest == has_error:
            raise CompletenessError(
                "provide exactly one of manifest or validation_error"
            )

        if has_error:
            if (
                not isinstance(self.validation_error, str)
                or not self.validation_error.strip()
            ):
                raise CompletenessError(
                    "validation_error must be a non-empty string"
                )


@dataclass(frozen=True, slots=True)
class ReportEvidence:
    """File and manifest observations for one expected report."""

    data_file: DataFileEvidence | None = None
    manifest: ManifestEvidence | None = None


@dataclass(frozen=True, slots=True)
class CompletenessIssue:
    code: CompletenessIssueCode
    message: str

    @property
    def sort_key(self) -> tuple[str, str]:
        return self.code.value, self.message


@dataclass(frozen=True, slots=True)
class CompletenessResult:
    status: SourceReportStatus
    deadline_at_utc: datetime
    issues: tuple[CompletenessIssue, ...]

    @property
    def is_complete(self) -> bool:
        return self.status.state is SourceCompleteness.COMPLETE

    @property
    def is_pending(self) -> bool:
        return self.status.state is SourceCompleteness.PENDING

    @property
    def is_incomplete(self) -> bool:
        return self.status.state is SourceCompleteness.INCOMPLETE

    @property
    def sort_key(self) -> tuple[str, str, str]:
        return (
            self.status.business_date.isoformat(),
            self.status.source_system.value,
            self.status.report_type.value,
        )


def report_deadline_local(
    business_date: date,
    report_type: ReportType,
) -> datetime:
    """Return the report deadline in Europe/Berlin.

    Reports for business date D are due on D+1.
    """

    if not isinstance(business_date, date):
        raise TypeError("business_date must be a date")

    if not isinstance(report_type, ReportType):
        raise TypeError("report_type must be a ReportType")

    next_day = business_date + timedelta(days=1)

    return datetime.combine(
        next_day,
        REPORT_DEADLINES[report_type],
        tzinfo=BERLIN,
    )


def report_deadline_utc(
    business_date: date,
    report_type: ReportType,
) -> datetime:
    return report_deadline_local(
        business_date,
        report_type,
    ).astimezone(timezone.utc)


def _visible_data_file(
    evidence: ReportEvidence,
    as_of_utc: datetime,
) -> DataFileEvidence | None:
    data_file = evidence.data_file

    if data_file is None:
        return None

    received_at = _require_aware_datetime(
        data_file.received_at_utc,
        field_name="data-file received_at_utc",
    )

    if received_at > as_of_utc:
        return None

    return data_file


def _visible_manifest(
    evidence: ReportEvidence,
    as_of_utc: datetime,
) -> ManifestEvidence | None:
    manifest_evidence = evidence.manifest

    if manifest_evidence is None:
        return None

    received_at = _require_aware_datetime(
        manifest_evidence.received_at_utc,
        field_name="manifest received_at_utc",
    )

    if received_at > as_of_utc:
        return None

    return manifest_evidence


def _collect_issues(
    *,
    expected_source: SourceSystem,
    report_type: ReportType,
    business_date: date,
    data_file: DataFileEvidence | None,
    manifest_evidence: ManifestEvidence | None,
) -> list[CompletenessIssue]:
    issues: list[CompletenessIssue] = []

    if data_file is None:
        issues.append(
            CompletenessIssue(
                code=CompletenessIssueCode.DATA_FILE_MISSING,
                message="data file was not available by evaluation time",
            )
        )

    if manifest_evidence is None:
        issues.append(
            CompletenessIssue(
                code=CompletenessIssueCode.MANIFEST_MISSING,
                message="manifest was not available by evaluation time",
            )
        )
        return issues

    if manifest_evidence.validation_error is not None:
        issues.append(
            CompletenessIssue(
                code=CompletenessIssueCode.MANIFEST_INVALID,
                message=(
                    "manifest failed validation: "
                    f"{manifest_evidence.validation_error}"
                ),
            )
        )
        return issues

    manifest = manifest_evidence.manifest

    if manifest is None:
        raise AssertionError(
            "validated ManifestEvidence must contain a manifest"
        )

    if manifest.source_system is not expected_source:
        issues.append(
            CompletenessIssue(
                code=(
                    CompletenessIssueCode.MANIFEST_SOURCE_MISMATCH
                ),
                message=(
                    f"manifest source is "
                    f"{manifest.source_system.value}; "
                    f"expected {expected_source.value}"
                ),
            )
        )

    if manifest.report_type is not report_type:
        issues.append(
            CompletenessIssue(
                code=(
                    CompletenessIssueCode
                    .MANIFEST_REPORT_TYPE_MISMATCH
                ),
                message=(
                    f"manifest report type is "
                    f"{manifest.report_type.value}; "
                    f"expected {report_type.value}"
                ),
            )
        )

    if manifest.business_date != business_date:
        issues.append(
            CompletenessIssue(
                code=(
                    CompletenessIssueCode
                    .MANIFEST_BUSINESS_DATE_MISMATCH
                ),
                message=(
                    f"manifest business date is "
                    f"{manifest.business_date.isoformat()}; "
                    f"expected {business_date.isoformat()}"
                ),
            )
        )

    manifest_received_at = _require_aware_datetime(
        manifest_evidence.received_at_utc,
        field_name="manifest received_at_utc",
    )

    generated_at = _require_aware_datetime(
        manifest.generated_at_utc,
        field_name="manifest generated_at_utc",
    )

    if generated_at > manifest_received_at:
        issues.append(
            CompletenessIssue(
                code=(
                    CompletenessIssueCode
                    .MANIFEST_GENERATED_AFTER_RECEIPT
                ),
                message=(
                    "manifest generated_at_utc is later than "
                    "its receipt time"
                ),
            )
        )

    if data_file is None:
        return issues

    if data_file.source_system is not expected_source:
        issues.append(
            CompletenessIssue(
                code=CompletenessIssueCode.DATA_SOURCE_MISMATCH,
                message=(
                    f"data-file source is "
                    f"{data_file.source_system.value}; "
                    f"expected {expected_source.value}"
                ),
            )
        )

    if data_file.report_type is not report_type:
        issues.append(
            CompletenessIssue(
                code=(
                    CompletenessIssueCode
                    .DATA_REPORT_TYPE_MISMATCH
                ),
                message=(
                    f"data-file report type is "
                    f"{data_file.report_type.value}; "
                    f"expected {report_type.value}"
                ),
            )
        )

    if data_file.business_date != business_date:
        issues.append(
            CompletenessIssue(
                code=(
                    CompletenessIssueCode
                    .DATA_BUSINESS_DATE_MISMATCH
                ),
                message=(
                    f"data-file business date is "
                    f"{data_file.business_date.isoformat()}; "
                    f"expected {business_date.isoformat()}"
                ),
            )
        )

    if manifest.batch_id != data_file.batch_id:
        issues.append(
            CompletenessIssue(
                code=CompletenessIssueCode.BATCH_ID_MISMATCH,
                message=(
                    f"manifest batch {manifest.batch_id!r} does not "
                    f"match data batch {data_file.batch_id!r}"
                ),
            )
        )

    if manifest.row_count != data_file.parsed_row_count:
        issues.append(
            CompletenessIssue(
                code=CompletenessIssueCode.ROW_COUNT_MISMATCH,
                message=(
                    f"manifest declares {manifest.row_count} rows; "
                    f"data file contains "
                    f"{data_file.parsed_row_count}"
                ),
            )
        )

    if manifest.sha256 != data_file.sha256:
        issues.append(
            CompletenessIssue(
                code=CompletenessIssueCode.CHECKSUM_MISMATCH,
                message=(
                    f"manifest checksum {manifest.sha256} does not "
                    f"match data checksum {data_file.sha256}"
                ),
            )
        )

    return issues


def evaluate_report(
    *,
    report_type: ReportType,
    business_date: date,
    as_of: datetime,
    evidence: ReportEvidence | None = None,
) -> CompletenessResult:
    """Evaluate one expected source report."""

    if not isinstance(report_type, ReportType):
        raise TypeError("report_type must be a ReportType")

    if not isinstance(business_date, date):
        raise TypeError("business_date must be a date")

    as_of_utc = _require_aware_datetime(
        as_of,
        field_name="as_of",
    )

    expected_source = REPORT_SOURCES[report_type]
    deadline_at_utc = report_deadline_utc(
        business_date,
        report_type,
    )

    evidence = evidence or ReportEvidence()

    data_file = _visible_data_file(
        evidence,
        as_of_utc,
    )
    manifest_evidence = _visible_manifest(
        evidence,
        as_of_utc,
    )

    issues = _collect_issues(
        expected_source=expected_source,
        report_type=report_type,
        business_date=business_date,
        data_file=data_file,
        manifest_evidence=manifest_evidence,
    )

    issues.sort(key=lambda issue: issue.sort_key)
    issue_tuple = tuple(issues)

    if not issue_tuple:
        state = SourceCompleteness.COMPLETE
        reason = None
    elif as_of_utc < deadline_at_utc:
        state = SourceCompleteness.PENDING
        reason = "; ".join(
            issue.message for issue in issue_tuple
        )
    else:
        state = SourceCompleteness.INCOMPLETE
        reason = "; ".join(
            issue.message for issue in issue_tuple
        )

    batch_id: str | None = None

    if (
        manifest_evidence is not None
        and manifest_evidence.manifest is not None
    ):
        batch_id = manifest_evidence.manifest.batch_id
    elif data_file is not None:
        batch_id = data_file.batch_id

    status = SourceReportStatus(
        source_system=expected_source,
        report_type=report_type,
        business_date=business_date,
        state=state,
        batch_id=batch_id,
        reason=reason,
    )

    return CompletenessResult(
        status=status,
        deadline_at_utc=deadline_at_utc,
        issues=issue_tuple,
    )


def evaluate_expected_reports(
    *,
    business_date: date,
    as_of: datetime,
    evidence_by_report: Mapping[
        ReportType,
        ReportEvidence,
    ] | None = None,
) -> tuple[CompletenessResult, ...]:
    """Evaluate all six expected daily reports."""

    evidence_by_report = evidence_by_report or {}

    unknown_reports = (
        set(evidence_by_report) - set(EXPECTED_REPORTS)
    )

    if unknown_reports:
        names = ", ".join(
            str(report_type)
            for report_type in sorted(
                unknown_reports,
                key=str,
            )
        )
        raise CompletenessError(
            f"unexpected report types: {names}"
        )

    results = [
        evaluate_report(
            report_type=report_type,
            business_date=business_date,
            as_of=as_of,
            evidence=evidence_by_report.get(report_type),
        )
        for report_type in EXPECTED_REPORTS
    ]

    return tuple(
        sorted(
            results,
            key=lambda result: result.sort_key,
        )
    )


def overall_completeness(
    results: Iterable[CompletenessResult],
) -> SourceCompleteness:
    """Combine source-level states into one run-level state."""

    states = {
        result.status.state
        for result in results
    }

    if SourceCompleteness.INCOMPLETE in states:
        return SourceCompleteness.INCOMPLETE

    if SourceCompleteness.PENDING in states:
        return SourceCompleteness.PENDING

    return SourceCompleteness.COMPLETE


def required_reports_complete(
    results: Iterable[CompletenessResult],
    required_reports: Iterable[ReportType],
) -> bool:
    """Return whether every required report is COMPLETE."""

    states_by_report = {
        result.status.report_type: result.status.state
        for result in results
    }

    return all(
        states_by_report.get(report_type)
        is SourceCompleteness.COMPLETE
        for report_type in required_reports
    )


def may_emit_missing_payment(
    results: Iterable[CompletenessResult],
) -> bool:
    """Missing-payment checks require complete orders and payments."""

    return required_reports_complete(
        results,
        {
            ReportType.ORDERS,
            ReportType.PAYMENTS,
        },
    )


def may_emit_missing_fee(
    results: Iterable[CompletenessResult],
) -> bool:
    """Missing-fee checks require complete payments and fees."""

    return required_reports_complete(
        results,
        {
            ReportType.PAYMENTS,
            ReportType.FEES,
        },
    )


def source_report_exceptions(
    results: Iterable[CompletenessResult],
) -> tuple[ExceptionRecord, ...]:
    """Convert overdue incomplete reports into exceptions."""

    exceptions: list[ExceptionRecord] = []

    for result in results:
        if not result.is_incomplete:
            continue

        issue_codes = {
            issue.code
            for issue in result.issues
        }

        only_missing = issue_codes.issubset(
            _MISSING_ISSUES
        )

        code = (
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
            ExceptionRecord(
                code=code,
                entity_type="SOURCE_REPORT",
                entity_id=entity_id,
                message=(
                    status.reason
                    or "source report is incomplete"
                ),
            )
        )

    return tuple(
        sorted(
            exceptions,
            key=lambda exception: exception.sort_key,
        )
    )