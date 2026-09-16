"""Durable queue operations for Threadline recovery."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row


class RecoveryQueueError(RuntimeError):
    """Raised when a queue operation violates its transaction contract."""


class RecoveryQueue:
    def __init__(self, connection: psycopg.Connection) -> None:
        self.connection = connection

    def _require_transaction(self) -> None:
        if self.connection.info.transaction_status != TransactionStatus.INTRANS:
            raise RecoveryQueueError(
                "RecoveryQueue operations require an active transaction"
            )

    def next_ready(self) -> dict[str, Any] | None:
        """Lock one eligible request until the caller's transaction ends."""

        self._require_transaction()

        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    request_id,
                    as_of_utc,
                    attempt_count,
                    max_attempts,
                    base_retry_delay_seconds
                FROM recovery_request
                WHERE status = 'PENDING'
                  AND attempt_count < max_attempts
                  AND next_attempt_at_utc <= clock_timestamp()
                ORDER BY
                    next_attempt_at_utc,
                    created_at_utc,
                    request_id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            return cursor.fetchone()

    def mark_succeeded(
        self,
        *,
        request_id: UUID,
        run_id: UUID,
    ) -> None:
        """Call inside the transaction that publishes this run."""

        self._require_transaction()

        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT attempt_count, max_attempts, status
                FROM recovery_request
                WHERE request_id = %s
                FOR UPDATE
                """,
                (request_id,),
            )
            request = cursor.fetchone()

            if request is None:
                raise RecoveryQueueError("Recovery request does not exist")

            if request["status"] != "PENDING":
                raise RecoveryQueueError("Recovery request is not pending")

            attempt_number = request["attempt_count"] + 1

            if attempt_number > request["max_attempts"]:
                raise RecoveryQueueError("Recovery attempt limit exceeded")

            cursor.execute(
                """
                INSERT INTO recovery_attempt (
                    request_id,
                    attempt_number,
                    outcome,
                    run_id
                )
                VALUES (%s, %s, 'SUCCEEDED', %s)
                """,
                (request_id, attempt_number, run_id),
            )

            cursor.execute(
                """
                UPDATE recovery_request
                SET
                    status = 'SUCCEEDED',
                    attempt_count = %s,
                    last_error = NULL,
                    result_run_id = %s,
                    completed_at_utc = clock_timestamp()
                WHERE request_id = %s
                """,
                (attempt_number, run_id, request_id),
            )

    def record_failure(
        self,
        *,
        request_id: UUID,
        error: Exception,
    ) -> None:
        """Call in a new transaction after candidate publication rolls back."""

        self._require_transaction()

        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    status,
                    attempt_count,
                    max_attempts,
                    base_retry_delay_seconds
                FROM recovery_request
                WHERE request_id = %s
                FOR UPDATE
                """,
                (request_id,),
            )
            request = cursor.fetchone()

            if request is None:
                raise RecoveryQueueError("Recovery request does not exist")

            # Never overwrite an already-published or terminal request.
            if request["status"] != "PENDING":
                return

            attempt_number = request["attempt_count"] + 1
            exhausted = attempt_number >= request["max_attempts"]

            # First failure: base delay.
            # Second failure: twice the base delay.
            # Maximum delay: one hour.
            exponent = min(attempt_number - 1, 12)
            retry_delay = min(
                3600,
                request["base_retry_delay_seconds"] * (2**exponent),
            )

            error_message = (
                f"{type(error).__name__}: {error}"
            )[:2000]

            cursor.execute(
                """
                INSERT INTO recovery_attempt (
                    request_id,
                    attempt_number,
                    outcome,
                    error_message
                )
                VALUES (%s, %s, 'FAILED', %s)
                """,
                (request_id, attempt_number, error_message),
            )

            cursor.execute(
                """
                UPDATE recovery_request
                SET
                    attempt_count = %s,
                    last_error = %s,
                    status = %s,
                    next_attempt_at_utc =
                        clock_timestamp() + make_interval(secs => %s),
                    completed_at_utc = CASE
                        WHEN %s THEN clock_timestamp()
                        ELSE NULL
                    END
                WHERE request_id = %s
                """,
                (
                    attempt_number,
                    error_message,
                    "FAILED" if exhausted else "PENDING",
                    retry_delay,
                    exhausted,
                    request_id,
                ),
            )

    def requeue_failed(
        self,
        *,
        request_id: UUID,
        additional_attempts: int = 3,
    ) -> None:
        """Reopen a failed request while preserving its attempt history."""

        self._require_transaction()

        if additional_attempts < 1:
            raise ValueError("additional_attempts must be positive")

        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                UPDATE recovery_request
                SET
                    status = 'PENDING',
                    max_attempts = attempt_count + %s,
                    next_attempt_at_utc = clock_timestamp(),
                    completed_at_utc = NULL
                WHERE request_id = %s
                  AND status = 'FAILED'
                RETURNING request_id
                """,
                (additional_attempts, request_id),
            )

            if cursor.fetchone() is None:
                raise RecoveryQueueError(
                    "Request does not exist or is not FAILED"
                )