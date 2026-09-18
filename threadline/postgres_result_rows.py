"""Persist candidate result rows inside the caller's transaction."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from psycopg import sql
from psycopg.types.json import Jsonb

from threadline.completeness import evaluate_report
from threadline.contracts import ReportType


TRANSACTION_COLUMNS = (
    "run_id",
    "order_id",
    "order_status",
    "currency",
    "expected_collection",
    "captured_total",
    "collection_variance",
    "successful_refund_total",
    "expected_fee_total",
    "reported_fee_total",
    "lifetime_net_collection",
    "captured_payment_count",
    "successful_refund_count",
    "state",
    "exception_codes",
)

PAYOUT_COLUMNS = (
    "run_id",
    "payout_id",
    "payout_date",
    "currency",
    "expected_payout",
    "reported_line_total",
    "reported_net_amount",
    "provider_report_variance",
    "end_to_end_payout_variance",
    "expected_movement_count",
    "settlement_line_count",
    "state",
    "exception_codes",
)

EXCEPTION_COLUMNS = (
    "run_id",
    "rule_id",
    "exception_type",
    "entity_type",
    "entity_id",
    "expected_amount",
    "actual_amount",
    "variance",
    "detected_at_utc",
    "status",
    "supporting_source_record_ids",
)

MONEY_COLUMNS = {
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
    "expected_amount",
    "actual_amount",
    "variance",
}

JSON_COLUMNS = {
    "exception_codes",
    "supporting_source_record_ids",
    "issue_codes",
}


def _timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _insert(connection, table, columns, rows):
    statement = sql.SQL(
        "INSERT INTO {} ({}) VALUES ({})"
    ).format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(
            sql.Placeholder() for _ in columns
        ),
    )

    for row in rows:
        values = []

        for column in columns:
            value = row[column]

            if value is not None:
                if column in MONEY_COLUMNS:
                    value = Decimal(str(value))
                elif column in JSON_COLUMNS:
                    value = Jsonb(value)
                elif column == "run_id":
                    value = UUID(str(value))
                elif column in {"payout_date", "report_date"}:
                    value = date.fromisoformat(value)
                elif column in {
                    "detected_at_utc",
                    "deadline_utc",
                    "observed_at_utc",
                }:
                    value = _timestamp(value)

            values.append(value)

        connection.execute(statement, values)


def persist_candidate_rows(connection, document):
    _insert(
        connection,
        "transaction_reconciliation",
        TRANSACTION_COLUMNS,
        document["transactions"],
    )
    _insert(
        connection,
        "payout_reconciliation",
        PAYOUT_COLUMNS,
        document["payouts"],
    )
    _insert(
        connection,
        "reconciliation_exception",
        EXCEPTION_COLUMNS,
        document["exceptions"],
    )

    snapshots = []

    for status in document["source_completeness"]:
        report_type = ReportType(status["report_type"])
        business_date = date.fromisoformat(status["business_date"])

        deadline = evaluate_report(
            report_type=report_type,
            business_date=business_date,
            as_of=_timestamp(document["detected_at_utc"]),
        ).deadline_at_utc

        snapshots.append(
            {
                "run_id": document["run_id"],
                "report_type": report_type.value,
                "report_date": business_date.isoformat(),
                "state": status["state"],
                "deadline_utc": deadline.isoformat(),
                "observed_at_utc": document["detected_at_utc"],
                "issue_codes": (
                    []
                    if status["state"] == "COMPLETE"
                    else ["SOURCE_EVIDENCE_INCOMPLETE"]
                ),
            }
        )

    _insert(
        connection,
        "source_completeness_snapshot",
        (
            "run_id",
            "report_type",
            "report_date",
            "state",
            "deadline_utc",
            "observed_at_utc",
            "issue_codes",
        ),
        snapshots,
    )