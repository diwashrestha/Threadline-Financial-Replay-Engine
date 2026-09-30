"""Compare independent simulator truth with the current publication."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from threadline.simulator.ground_truth import GroundTruth


TRANSACTION_FIELDS = (
    "expected_collection",
    "captured_total",
    "successful_refund_total",
    "expected_fee_total",
    "reported_fee_total",
    "lifetime_net_collection",
)

PAYOUT_FIELDS = (
    "expected_payout",
    "reported_line_total",
    "reported_net_amount",
)


def _money(value: Any) -> str:
    return format(Decimal(str(value)), ".2f")


def published_financial_document(payload: dict) -> dict:
    return {
        "transactions": {
            row["order_id"]: {
                field: _money(row[field])
                for field in TRANSACTION_FIELDS
            }
            for row in payload["transactions"]
        },
        "payouts": {
            row["payout_id"]: {
                field: _money(row[field])
                for field in PAYOUT_FIELDS
            }
            for row in payload["payouts"]
        },
    }


def fingerprint(document: dict) -> str:
    encoded = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def read_current_publication(
    database_url: str,
    publication_name: str = "threadline",
) -> tuple[str, dict]:
    with psycopg.connect(
        database_url,
        row_factory=dict_row,
    ) as connection:
        row = connection.execute(
            """
            SELECT
                r.logical_fingerprint,
                r.result_payload
            FROM publication_pointer AS p
            JOIN recovery_result AS r
              ON r.run_id = p.current_run_id
            WHERE p.publication_name = %s
            """,
            (publication_name,),
        ).fetchone()

    assert row is not None, "No current publication"
    return row["logical_fingerprint"], row["result_payload"]


def assert_publication_matches_truth(
    database_url: str,
    truth: GroundTruth,
) -> tuple[str, dict]:
    published_logical_fingerprint, payload = (
        read_current_publication(database_url)
    )
    published_document = published_financial_document(payload)
    truth_document = truth.financial_document()

    # Compare the actual rows first. This gives a useful diff on failure.
    assert published_document == truth_document

    # Both sides now use the same small, independently defined document.
    assert fingerprint(published_document) == (
        truth.financial_fingerprint()
    )

    return published_logical_fingerprint, payload


def exception_codes(payload: dict) -> set[str]:
    return {
        str(row["exception_type"])
        for row in payload["exceptions"]
    }