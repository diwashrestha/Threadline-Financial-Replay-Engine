"""Retain verified file evidence and explicit expected report days.

Revision ID: 009_verified_delivery_evidence
Revises: 008_retryable_archival
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "009_verified_delivery_evidence"
down_revision = "008_retryable_archival"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "expected_report_day",
        sa.Column(
            "business_date",
            sa.Date(),
            primary_key=True,
        ),
    )

    op.create_table(
        "verified_delivery_evidence",
        sa.Column(
            "batch_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
        ),
        sa.Column("data_bytes", sa.LargeBinary(), nullable=False),
        sa.Column("manifest_bytes", sa.LargeBinary(), nullable=False),
        sa.Column(
            "file_protocol_version",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.ForeignKeyConstraint(
            ["batch_id"],
            ["ingestion_batch.batch_id"],
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "file_protocol_version = 1",
            name="ck_verified_delivery_protocol",
        ),
    )

    op.alter_column(
        "recovery_request",
        "trigger_receipt_id",
        nullable=True,
    )

    op.add_column(
        "recovery_request",
        sa.Column(
            "trigger_batch_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
    )

    op.create_foreign_key(
        "fk_recovery_request_trigger_batch",
        "recovery_request",
        "ingestion_batch",
        ["trigger_batch_id"],
        ["batch_id"],
        ondelete="RESTRICT",
    )

    # PENDING is a valid state in the supplied domain contracts.
    op.drop_constraint(
        "ck_transaction_reconciliation_state",
        "transaction_reconciliation",
        type_="check",
    )
    op.create_check_constraint(
        "ck_transaction_reconciliation_state",
        "transaction_reconciliation",
        "state IN ('PENDING', 'RECONCILED', 'INCOMPLETE', 'EXCEPTION')",
    )

    op.drop_constraint(
        "ck_payout_reconciliation_state",
        "payout_reconciliation",
        type_="check",
    )
    op.create_check_constraint(
        "ck_payout_reconciliation_state",
        "payout_reconciliation",
        "state IN ('PENDING', 'RECONCILED', 'INCOMPLETE', 'EXCEPTION')",
    )


def downgrade() -> None:
    # Downgrade cannot safely discard valid PENDING rows.
    for table, constraint in (
        (
            "transaction_reconciliation",
            "ck_transaction_reconciliation_state",
        ),
        (
            "payout_reconciliation",
            "ck_payout_reconciliation_state",
        ),
    ):
        op.drop_constraint(constraint, table, type_="check")
        op.create_check_constraint(
            constraint,
            table,
            "state IN ('RECONCILED', 'INCOMPLETE', 'EXCEPTION')",
        )

    op.drop_constraint(
        "fk_recovery_request_trigger_batch",
        "recovery_request",
        type_="foreignkey",
    )
    op.drop_column("recovery_request", "trigger_batch_id")
    op.drop_table("verified_delivery_evidence")
    op.drop_table("expected_report_day")