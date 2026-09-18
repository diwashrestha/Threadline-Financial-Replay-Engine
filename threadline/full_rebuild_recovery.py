"""Durable full-rebuild recovery and atomic PostgreSQL publication."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from threadline.recovery_fingerprint import (
    FINANCIAL_FINGERPRINT_VERSION,
    document_fingerprint,
    json_value,
    logical_fingerprint,
)

from threadline.recovery_queue import RecoveryQueue

from threadline.postgres_result_rows import persist_candidate_rows

from threadline.recovery_fingerprint import (
    FINANCIAL_FINGERPRINT_VERSION,
    document_fingerprint,
    json_value,
    logical_fingerprint,
)


# All financial ingestion, daily publication, and recovery writers
# must acquire this same lock before reading or changing financial state.
FINANCIAL_STATE_LOCK = 740031


CandidateBuilder = Callable[
    [psycopg.Connection, str, datetime],
    Any,
]

DomainValidator = Callable[[Any], None]
FailureHook = Callable[[str], None]


@dataclass(frozen=True, slots=True)
class RecoveryOutcome:
    request_id: UUID
    run_id: UUID
    input_fingerprint: str
    logical_fingerprint: str


def require_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("as_of_utc must be timezone-aware")
    return value.astimezone(timezone.utc)


def lock_financial_state(connection: psycopg.Connection) -> None:
    """Acquire inside an existing ingestion/publication transaction."""

    connection.execute(
        "SELECT pg_advisory_xact_lock(%s)",
        (FINANCIAL_STATE_LOCK,),
    )


def enqueue_full_rebuild(
    connection: psycopg.Connection,
    *,
    trigger_receipt_id: UUID,
    canonical_changed: bool,
    affects_published_history: bool,
    change_key: str,
    reason_code: str,
    as_of_utc: datetime,
) -> UUID | None:
    """Call inside the transaction that commits canonical state.

    This function deliberately does not commit.
    """

    if not canonical_changed or not affects_published_history:
        return None

    if not change_key or not reason_code:
        raise ValueError("change_key and reason_code are required")

    as_of_utc = require_aware_utc(as_of_utc)

    request_key = document_fingerprint(
        {
            "scope": "FULL",
            "change_key": change_key,
        }
    )

    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT b.ingestion_status
            FROM source_receipt AS r
            JOIN ingestion_batch AS b
                ON b.batch_id = r.batch_id
            WHERE r.receipt_id = %s
            """,
            (trigger_receipt_id,),
        )
        receipt = cursor.fetchone()

        if receipt is None:
            raise ValueError("Trigger receipt does not exist")

        if receipt["ingestion_status"] != "COMMITTED":
            raise ValueError(
                "Mark the batch COMMITTED before enqueueing, "
                "within the same database transaction"
            )

        cursor.execute(
            """
            INSERT INTO recovery_request (
                request_key,
                trigger_receipt_id,
                reason_code,
                recovery_scope,
                as_of_utc
            )
            VALUES (%s, %s, %s, 'FULL', %s)
            ON CONFLICT (request_key) DO NOTHING
            """,
            (
                request_key,
                trigger_receipt_id,
                reason_code,
                as_of_utc,
            ),
        )

        cursor.execute(
            """
            SELECT request_id
            FROM recovery_request
            WHERE request_key = %s
            """,
            (request_key,),
        )
        return cursor.fetchone()["request_id"]


def validate_arithmetic(document: dict[str, Any]) -> None:
    """Additional invariants around the existing domain validator."""

    transactions = document["transactions"]
    payouts = document["payouts"]

    order_ids = [row["order_id"] for row in transactions]
    payout_ids = [row["payout_id"] for row in payouts]

    if len(order_ids) != len(set(order_ids)):
        raise ValueError("Duplicate transaction output identity")

    if len(payout_ids) != len(set(payout_ids)):
        raise ValueError("Duplicate payout output identity")

    def amount(row: dict[str, Any], key: str) -> Decimal:
        value = Decimal(str(row[key]))
        if not value.is_finite():
            raise ValueError(f"Invalid monetary field: {key}")
        return value

    for row in transactions:
        if amount(row, "collection_variance") != (
            amount(row, "captured_total")
            - amount(row, "expected_collection")
        ):
            raise ValueError("Collection variance invariant failed")

        if amount(row, "lifetime_net_collection") != (
            amount(row, "captured_total")
            - amount(row, "successful_refund_total")
            - amount(row, "expected_fee_total")
        ):
            raise ValueError("Lifetime net collection invariant failed")

    for row in payouts:
        if amount(row, "provider_report_variance") != (
            amount(row, "reported_net_amount")
            - amount(row, "reported_line_total")
        ):
            raise ValueError("Provider payout variance invariant failed")

        if amount(row, "end_to_end_payout_variance") != (
            amount(row, "reported_net_amount")
            - amount(row, "expected_payout")
        ):
            raise ValueError("End-to-end payout variance invariant failed")


def input_fingerprint(
    connection: psycopg.Connection,
    document: dict[str, Any],
) -> str:
    """Identify durable inputs, separately from financial output."""

    batches = connection.execute(
        """
        SELECT
            batch_id,
            file_checksum,
            manifest_checksum,
            schema_version,
            report_type,
            report_date,
            declared_row_count,
            observed_row_count,
            ingestion_status
        FROM ingestion_batch
        WHERE ingestion_status IN ('COMMITTED', 'FAILED')
        ORDER BY batch_id
        """
    ).fetchall()

    receipts = connection.execute(
        """
        SELECT
            r.receipt_id,
            r.batch_id,
            r.row_number,
            r.entity_type,
            r.source_id,
            r.source_version,
            r.payload_hash,
            r.disposition,
            r.reason_code
        FROM source_receipt AS r
        JOIN ingestion_batch AS b
            ON b.batch_id = r.batch_id
        WHERE b.ingestion_status IN ('COMMITTED', 'FAILED')
        ORDER BY r.batch_id, r.row_number
        """
    ).fetchall()

    resolutions = connection.execute(
        """
        SELECT
            entity_type,
            source_id,
            winning_version,
            resolution_state,
            selected_payload_hash
        FROM entity_resolution
        ORDER BY entity_type, source_id
        """
    ).fetchall()

    # UUID is an audit identifier, converted explicitly for JSON.
    def audit_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                key: str(value) if isinstance(value, UUID)
                else json_value(value)
                for key, value in row.items()
            }
            for row in rows
        ]

    return document_fingerprint(
        {
            "batches": audit_rows(batches),
            "receipts": audit_rows(receipts),
            "resolutions": audit_rows(resolutions),
            "source_completeness": document["source_completeness"],
        }
    )


class FullRebuildRecovery:
    def __init__(
        self,
        *,
        database_url: str,
        build_candidate: CandidateBuilder,
        validate_domain: DomainValidator,
        publication_name: str = "threadline",
        failure_hook: FailureHook | None = None,
    ) -> None:
        self.database_url = database_url
        self.build_candidate = build_candidate
        self.validate_domain = validate_domain
        self.publication_name = publication_name
        self.failure_hook = failure_hook or (lambda point: None)

    def run_next(self) -> RecoveryOutcome | None:
        """Publish one eligible request with controlled failure boundaries."""

        request_id: UUID | None = None

        with psycopg.connect(
            self.database_url,
            autocommit=True,
            row_factory=dict_row,
        ) as connection:
            connection.execute(
                "SELECT pg_advisory_lock(%s)",
                (FINANCIAL_STATE_LOCK,),
            )

            queue = RecoveryQueue(connection)

            try:
                try:
                    with connection.transaction():
                        request = queue.next_ready()

                        if request is None:
                            return None

                        request_id = request["request_id"]
                        as_of_utc = request["as_of_utc"]
                        run_id = uuid4()

                        cutoff = connection.execute(
                            "SELECT clock_timestamp() AS cutoff"
                        ).fetchone()["cutoff"]

                        candidate = self.build_candidate(
                            connection,
                            str(run_id),
                            as_of_utc,
                        )

                        document = json_value(candidate)

                        if not isinstance(document, dict):
                            raise TypeError(
                                "Candidate must serialize to an object"
                            )

                        document["financial_fingerprint_version"] = (
                            FINANCIAL_FINGERPRINT_VERSION
                        )

                        if document["run_id"] != str(run_id):
                            raise ValueError(
                                "Candidate run_id does not match"
                            )

                        if document["detected_at_utc"] != json_value(
                            as_of_utc
                        ):
                            raise ValueError(
                                "Candidate must use the request's "
                                "frozen as_of_utc"
                            )

                        self.validate_domain(candidate)
                        validate_arithmetic(document)

                        logical_hash = logical_fingerprint(candidate)
                        evidence_hash = input_fingerprint(
                            connection,
                            document,
                        )

                        self.failure_hook("after_validation")

                        connection.execute(
                            """
                            INSERT INTO reconciliation_run (
                                run_id,
                                contract_version,
                                as_of_utc,
                                input_watermark_utc,
                                status,
                                result_hash
                            )
                            VALUES (
                                %s, %s, %s, %s, 'CANDIDATE', %s
                            )
                            """,
                            (
                                run_id,
                                document["contract_version"],
                                as_of_utc,
                                cutoff,
                                logical_hash,
                            ),
                        )

                        self.failure_hook("after_run_insert")

                        connection.execute(
                            """
                            INSERT INTO recovery_result (
                                run_id,
                                input_fingerprint,
                                logical_fingerprint,
                                result_payload
                            )
                            VALUES (%s, %s, %s, %s)
                            """,
                            (
                                run_id,
                                evidence_hash,
                                logical_hash,
                                Jsonb(document),
                            ),
                        )

                        persist_candidate_rows(connection, document)

                        self.failure_hook("after_result_insert")
                        self.failure_hook("before_pointer_update")
                        self.failure_hook("after_result_insert")
                        self.failure_hook("before_pointer_update")

                        connection.execute(
                            """
                            UPDATE reconciliation_run
                            SET
                                status = 'PUBLISHED',
                                published_at_utc = CURRENT_TIMESTAMP
                            WHERE run_id = %s
                            """,
                            (run_id,),
                        )

                        connection.execute(
                            """
                            INSERT INTO publication_pointer (
                                publication_name,
                                current_run_id,
                                updated_at_utc
                            )
                            VALUES (
                                %s, %s, CURRENT_TIMESTAMP
                            )
                            ON CONFLICT (publication_name)
                            DO UPDATE SET
                                current_run_id = EXCLUDED.current_run_id,
                                updated_at_utc = EXCLUDED.updated_at_utc
                            """,
                            (self.publication_name, run_id),
                        )

                        self.failure_hook("after_pointer_update")

                        queue.mark_succeeded(
                            request_id=request_id,
                            run_id=run_id,
                        )

                        self.failure_hook("after_queue_completion")
                        self.failure_hook("before_commit")

                        outcome = RecoveryOutcome(
                            request_id=request_id,
                            run_id=run_id,
                            input_fingerprint=evidence_hash,
                            logical_fingerprint=logical_hash,
                        )

                    # Exiting the transaction successfully commits
                    # publication, request completion, and attempt history.

                except Exception as error:
                    # This handler covers candidate/publication failures.
                    # The candidate transaction has already rolled back.
                    if request_id is not None:
                        with connection.transaction():
                            queue.record_failure(
                                request_id=request_id,
                                error=error,
                            )

                    raise

                # Deliberately outside the failed-candidate handler.
                # An exception here cannot roll back the committed result
                # and must not record the request as failed.
                self.failure_hook("after_commit")

                return outcome

            finally:
                connection.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (FINANCIAL_STATE_LOCK,),
                )

def read_published_result(
    connection: psycopg.Connection,
    *,
    publication_name: str = "threadline",
) -> dict[str, Any] | None:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT
                p.current_run_id,
                r.logical_fingerprint,
                r.input_fingerprint,
                r.result_payload
            FROM publication_pointer AS p
            JOIN recovery_result AS r
                ON r.run_id = p.current_run_id
            WHERE p.publication_name = %s
            """,
            (publication_name,),
        )
        return cursor.fetchone()