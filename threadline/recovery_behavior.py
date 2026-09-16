"""Determine recovery actions from durable PostgreSQL state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from threadline.recovery_fingerprint import (
    FINANCIAL_FINGERPRINT_VERSION,
    verify_stored_financial_result,
)


class RecoveryAction(str, Enum):
    RETRY_NOW = "RETRY_NOW"
    WAIT_FOR_RETRY = "WAIT_FOR_RETRY"
    DO_NOT_REPEAT = "DO_NOT_REPEAT"
    MANUAL_REQUEUE = "MANUAL_REQUEUE"
    CHECK_AGAIN = "CHECK_AGAIN"
    INVESTIGATE_MISSING_REQUEST = "INVESTIGATE_MISSING_REQUEST"


class RecoveryStateError(RuntimeError):
    """Durable state violates the recovery contract."""


@dataclass(frozen=True, slots=True)
class RecoveryObservation:
    request_id: UUID
    request_status: str | None
    action: RecoveryAction
    reason: str

    attempt_count: int | None = None
    max_attempts: int | None = None
    next_attempt_at_utc: datetime | None = None
    result_run_id: UUID | None = None


def inspect_recovery(
    database_url: str,
    request_id: UUID | str,
) -> RecoveryObservation:
    """Inspect a known request using a fresh database connection.

    The returned action is advisory. The queue must still check
    eligibility and acquire its locks when executing a request.
    """

    request_id = UUID(str(request_id))

    try:
        with psycopg.connect(
            database_url,
            autocommit=True,
            row_factory=dict_row,
            connect_timeout=5,
        ) as connection:
            row = connection.execute(
                """
                SELECT
                    q.*,

                    q.next_attempt_at_utc <= clock_timestamp()
                        AS retry_due,

                    b.ingestion_status AS source_batch_status,

                    r.status AS run_status,
                    r.result_hash AS run_result_hash,

                    a.logical_fingerprint,
                    a.result_payload

                FROM recovery_request AS q

                LEFT JOIN source_receipt AS s
                    ON s.receipt_id = q.trigger_receipt_id

                LEFT JOIN ingestion_batch AS b
                    ON b.batch_id = s.batch_id

                LEFT JOIN reconciliation_run AS r
                    ON r.run_id = q.result_run_id

                LEFT JOIN recovery_result AS a
                    ON a.run_id = q.result_run_id

                WHERE q.request_id = %s
                """,
                (request_id,),
            ).fetchone()

    except psycopg.OperationalError:
        # An unavailable connection cannot establish commit outcome.
        return RecoveryObservation(
            request_id=request_id,
            request_status=None,
            action=RecoveryAction.CHECK_AGAIN,
            reason=(
                "Database state could not be read. Reconnect and inspect "
                "the same request before deciding whether to retry."
            ),
        )

    if row is None:
        return RecoveryObservation(
            request_id=request_id,
            request_status=None,
            action=RecoveryAction.INVESTIGATE_MISSING_REQUEST,
            reason=(
                "The request does not exist. Inspect its source ingestion "
                "and scheduling transaction before creating another request."
            ),
        )

    if row["source_batch_status"] != "COMMITTED":
        raise RecoveryStateError(
            "Recovery request does not reference committed source evidence"
        )

    common = {
        "request_id": request_id,
        "request_status": row["status"],
        "attempt_count": row["attempt_count"],
        "max_attempts": row["max_attempts"],
        "next_attempt_at_utc": row["next_attempt_at_utc"],
        "result_run_id": row["result_run_id"],
    }

    if row["status"] == "SUCCEEDED":
        if (
            row["result_run_id"] is None
            or row["completed_at_utc"] is None
            or row["run_status"] != "PUBLISHED"
            or row["result_payload"] is None
        ):
            raise RecoveryStateError(
                "Successful request is missing its committed result"
            )

        if row["run_result_hash"] != row["logical_fingerprint"]:
            raise RecoveryStateError(
                "Run ledger and result artifact hashes disagree"
            )

        payload = row["result_payload"]
        version = payload.get("financial_fingerprint_version", 1)

        if version == FINANCIAL_FINGERPRINT_VERSION:
            try:
                verify_stored_financial_result(row)
            except (AssertionError, ValueError, TypeError, KeyError) as error:
                raise RecoveryStateError(
                    "Committed financial artifact failed verification"
                ) from error

        elif version != 1:
            raise RecoveryStateError(
                f"Unsupported stored fingerprint version: {version}"
            )

        # Legacy version-1 hashes are left unchanged.
        # Version-2 artifacts are verified above.

        return RecoveryObservation(
            **common,
            action=RecoveryAction.DO_NOT_REPEAT,
            reason=(
                "The request already committed a published result. "
                "A later publication may have advanced the current pointer."
            ),
        )

    if row["status"] == "FAILED":
        if (
            row["result_run_id"] is not None
            or row["completed_at_utc"] is None
            or row["last_error"] is None
        ):
            raise RecoveryStateError(
                "Failed request has inconsistent completion state"
            )

        return RecoveryObservation(
            **common,
            action=RecoveryAction.MANUAL_REQUEUE,
            reason=(
                "Automatic attempts are exhausted. Fix the underlying "
                "problem, then explicitly grant additional attempts."
            ),
        )

    if row["status"] == "PENDING":
        if (
            row["result_run_id"] is not None
            or row["completed_at_utc"] is not None
            or row["attempt_count"] >= row["max_attempts"]
        ):
            raise RecoveryStateError(
                "Pending request has inconsistent result or attempt state"
            )

        if row["retry_due"]:
            return RecoveryObservation(
                **common,
                action=RecoveryAction.RETRY_NOW,
                reason=(
                    "The request is eligible. Execute it through the "
                    "normal locked queue consumer."
                ),
            )

        return RecoveryObservation(
            **common,
            action=RecoveryAction.WAIT_FOR_RETRY,
            reason="The request is waiting for its scheduled retry time.",
        )

    raise RecoveryStateError(
        f"Unsupported request status: {row['status']}"
    )