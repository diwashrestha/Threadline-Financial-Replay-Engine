"""Time-boundary and atomic-publication scenarios.

G03: report deadline has not passed
G04: overdue missing report
G20: failed candidate publication preserves the previous result
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from threadline.cli import run_scenario
from threadline.completeness import (
    BERLIN,
    CompletenessIssueCode,
    ReportEvidence,
    evaluate_report,
    report_deadline_local,
    report_deadline_utc,
    source_report_exceptions,
)
from threadline.contracts import (
    ExceptionCode,
    ReportType,
    SourceCompleteness,
)
from threadline.publication import (
    InMemoryPublicationStore,
    PublicationValidationError,
)
from threadline.reconcile import ReconciliationResult


BUSINESS_DATE = date(2026, 9, 14)

G01_PATH = (
    Path(__file__).parent
    / "fixtures"
    / "golden"
    / "g01_clean_order.json"
)


def _clean_result() -> ReconciliationResult:
    assert G01_PATH.exists(), (
        "Create tests/fixtures/golden/"
        "g01_clean_order.json first"
    )

    return run_scenario(G01_PATH).result


def _with_run_id(
    result: ReconciliationResult,
    run_id: str,
) -> ReconciliationResult:
    """Copy a result while changing every run-scoped identifier."""

    return replace(
        result,
        run_id=run_id,
        transactions=tuple(
            replace(
                transaction,
                run_id=run_id,
            )
            for transaction in result.transactions
        ),
        payouts=tuple(
            replace(
                payout,
                run_id=run_id,
            )
            for payout in result.payouts
        ),
        exceptions=tuple(
            replace(
                exception,
                run_id=run_id,
            )
            for exception in result.exceptions
        ),
    )


# ------------------------------------------------------------------
# G03: payment report deadline has not passed
# ------------------------------------------------------------------


def test_g03_missing_payment_report_before_deadline_is_pending():
    deadline = report_deadline_local(
        BUSINESS_DATE,
        ReportType.PAYMENTS,
    )

    assert deadline == datetime(
        2026,
        9,
        15,
        2,
        0,
        tzinfo=BERLIN,
    )

    evaluation_time = datetime(
        2026,
        9,
        15,
        1,
        59,
        59,
        tzinfo=BERLIN,
    )

    result = evaluate_report(
        report_type=ReportType.PAYMENTS,
        business_date=BUSINESS_DATE,
        as_of=evaluation_time,
        evidence=ReportEvidence(),
    )

    assert result.status.state is SourceCompleteness.PENDING
    assert result.is_pending
    assert not result.is_complete
    assert not result.is_incomplete

    assert {
        issue.code
        for issue in result.issues
    } == {
        CompletenessIssueCode.DATA_FILE_MISSING,
        CompletenessIssueCode.MANIFEST_MISSING,
    }

    # A pending report must not create a missing-source exception.
    assert source_report_exceptions([result]) == ()


# ------------------------------------------------------------------
# G04: payment report deadline has passed
# ------------------------------------------------------------------


def test_g04_missing_payment_report_at_deadline_is_incomplete():
    deadline = report_deadline_local(
        BUSINESS_DATE,
        ReportType.PAYMENTS,
    )

    # The deadline itself is overdue. The implementation uses:
    #
    #     as_of < deadline  -> PENDING
    #     as_of >= deadline -> INCOMPLETE
    result = evaluate_report(
        report_type=ReportType.PAYMENTS,
        business_date=BUSINESS_DATE,
        as_of=deadline,
        evidence=ReportEvidence(),
    )

    assert result.status.state is SourceCompleteness.INCOMPLETE
    assert result.is_incomplete
    assert not result.is_pending
    assert not result.is_complete

    assert {
        issue.code
        for issue in result.issues
    } == {
        CompletenessIssueCode.DATA_FILE_MISSING,
        CompletenessIssueCode.MANIFEST_MISSING,
    }

    exceptions = source_report_exceptions([result])

    assert len(exceptions) == 1

    exception = exceptions[0]

    assert exception.code is ExceptionCode.SOURCE_REPORT_MISSING
    assert exception.entity_type == "SOURCE_REPORT"
    assert (
        exception.entity_id
        == "mockpay:payments:2026-09-14"
    )


def test_payment_deadline_transition_occurs_at_one_second():
    immediately_before = datetime(
        2026,
        9,
        15,
        1,
        59,
        59,
        tzinfo=BERLIN,
    )
    at_deadline = datetime(
        2026,
        9,
        15,
        2,
        0,
        0,
        tzinfo=BERLIN,
    )

    before_result = evaluate_report(
        report_type=ReportType.PAYMENTS,
        business_date=BUSINESS_DATE,
        as_of=immediately_before,
        evidence=ReportEvidence(),
    )

    deadline_result = evaluate_report(
        report_type=ReportType.PAYMENTS,
        business_date=BUSINESS_DATE,
        as_of=at_deadline,
        evidence=ReportEvidence(),
    )

    assert (
        before_result.status.state
        is SourceCompleteness.PENDING
    )
    assert (
        deadline_result.status.state
        is SourceCompleteness.INCOMPLETE
    )


# ------------------------------------------------------------------
# Time-zone correctness
# ------------------------------------------------------------------


def test_berlin_deadlines_respect_daylight_saving_time():
    winter_business_date = date(2026, 1, 1)
    summer_business_date = date(2026, 7, 1)

    winter_local = report_deadline_local(
        winter_business_date,
        ReportType.PAYMENTS,
    )
    summer_local = report_deadline_local(
        summer_business_date,
        ReportType.PAYMENTS,
    )

    assert winter_local.hour == 2
    assert summer_local.hour == 2

    winter_utc = report_deadline_utc(
        winter_business_date,
        ReportType.PAYMENTS,
    )
    summer_utc = report_deadline_utc(
        summer_business_date,
        ReportType.PAYMENTS,
    )

    # January: Europe/Berlin is UTC+1.
    assert winter_utc == datetime(
        2026,
        1,
        2,
        1,
        0,
        tzinfo=timezone.utc,
    )

    # July: Europe/Berlin is UTC+2.
    assert summer_utc == datetime(
        2026,
        7,
        2,
        0,
        0,
        tzinfo=timezone.utc,
    )


# ------------------------------------------------------------------
# G20: candidate fails publication validation
# ------------------------------------------------------------------


def test_g20_invalid_candidate_preserves_published_run():
    base_result = _clean_result()

    run_a = _with_run_id(
        base_result,
        "RUN-A",
    )
    run_b = _with_run_id(
        base_result,
        "RUN-B",
    )

    store = InMemoryPublicationStore()

    receipt_a = store.publish(run_a)

    assert receipt_a.run_id == "RUN-A"
    assert store.current == run_a
    assert store.current is not None
    assert store.current.run_id == "RUN-A"

    transaction_b = run_b.transactions[0]

    # Corrupt a derived value in the candidate. The correct variance
    # should still be captured_total - expected_collection = 0.00.
    invalid_transaction_b = replace(
        transaction_b,
        collection_variance=Decimal("999.00"),
    )

    invalid_run_b = replace(
        run_b,
        transactions=(
            invalid_transaction_b,
            *run_b.transactions[1:],
        ),
    )

    with pytest.raises(
        PublicationValidationError,
        match="invalid collection variance",
    ):
        store.publish(invalid_run_b)

    # RUN-B failed validation and cannot replace trusted output.
    assert store.current == run_a
    assert store.current is not None
    assert store.current.run_id == "RUN-A"

    # The rejected candidate remains available to the caller for
    # diagnosis.
    assert invalid_run_b.run_id == "RUN-B"
    assert (
        invalid_run_b
        .transactions[0]
        .collection_variance
        == Decimal("999.00")
    )


# ------------------------------------------------------------------
# G20: failure after validation but before commit
# ------------------------------------------------------------------


def test_g20_precommit_failure_preserves_published_run():
    base_result = _clean_result()

    run_a = _with_run_id(
        base_result,
        "RUN-A",
    )
    run_b = _with_run_id(
        base_result,
        "RUN-B",
    )

    store = InMemoryPublicationStore()
    store.publish(run_a)

    class SimulatedPrecommitFailure(RuntimeError):
        pass

    def fail_before_commit() -> None:
        raise SimulatedPrecommitFailure(
            "simulated failure before atomic commit"
        )

    with pytest.raises(
        SimulatedPrecommitFailure,
        match="before atomic commit",
    ):
        store.publish(
            run_b,
            before_commit=fail_before_commit,
        )

    # RUN-B passed validation but failed before the assignment that
    # changes the trusted current result.
    assert store.current == run_a
    assert store.current is not None
    assert store.current.run_id == "RUN-A"