"""Rebuild Stage 2 inputs from finalized PostgreSQL evidence."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from threadline.canonicalize import canonicalize
from threadline.contracts import EntityType, parse_source_record
from threadline.reconcile import reconcile


EnvelopeFactory = Callable[
    [Mapping[str, Any], Any],
    Any,
]

QuarantineFactory = Callable[
    [Mapping[str, Any]],
    Any,
]

CompletenessLoader = Callable[
    [psycopg.Connection, datetime],
    Iterable[Any],
]


def make_full_rebuild_builder(
    *,
    make_envelope: EnvelopeFactory,
    make_quarantine: QuarantineFactory,
    load_completeness: CompletenessLoader,
):
    """Return a builder accepted by FullRebuildRecovery."""

    def build_candidate(
        connection: psycopg.Connection,
        run_id: str,
        as_of_utc: datetime,
    ):
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    r.receipt_id,
                    r.batch_id,
                    r.row_number,
                    r.entity_type,
                    r.source_id,
                    r.source_version,
                    r.payload_hash,
                    r.raw_payload,
                    r.disposition,
                    r.reason_code,
                    r.received_at_utc,
                    b.ingestion_status
                FROM source_receipt AS r
                JOIN ingestion_batch AS b
                    ON b.batch_id = r.batch_id
                WHERE b.ingestion_status IN ('COMMITTED', 'FAILED')
                ORDER BY r.batch_id, r.row_number
                """
            )
            receipts = cursor.fetchall()

        envelopes = []
        quarantine = []

        for receipt in receipts:
            disposition = receipt["disposition"]

            if disposition == "QUARANTINED":
                # Preserve the stored rejection reason.
                quarantine.append(make_quarantine(receipt))
                continue

            if receipt["ingestion_status"] != "COMMITTED":
                raise ValueError(
                    "A failed batch contains a non-quarantined receipt"
                )

            if disposition not in {
                "ACCEPTED",
                "STALE",
                "DUPLICATE",
                "CONFLICTED",
            }:
                raise ValueError(
                    "A committed batch contains an unresolved receipt: "
                    f"{receipt['receipt_id']}"
                )

            record = parse_source_record(
                EntityType(receipt["entity_type"]),
                receipt["raw_payload"],
            )

            envelopes.append(make_envelope(receipt, record))

        if len(envelopes) + len(quarantine) != len(receipts):
            raise ValueError("Receipt evidence was lost during reconstruction")

        canonicalization = canonicalize(envelopes)

        # This adapter must evaluate durable report evidence.
        # Missing reports must remain missing; do not manufacture COMPLETE.
        completeness_results = tuple(
            load_completeness(connection, as_of_utc)
        )

        return reconcile(
            run_id=run_id,
            detected_at=as_of_utc,
            canonicalization=canonicalization,
            completeness_results=completeness_results,
            quarantine_records=tuple(quarantine),
        )

    return build_candidate