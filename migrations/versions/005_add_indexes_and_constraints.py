"""Add operational indexes and immutable history protection.

Revision ID: 005_indexes_constraints
Revises: 004_reconciliation_publication
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "005_indexes_constraints"
down_revision = "004_reconciliation_publication"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_ingestion_batch_status_received",
        "ingestion_batch",
        [
            "ingestion_status",
            "received_at_utc",
        ],
    )

    op.create_index(
        "ix_ingestion_batch_report_content",
        "ingestion_batch",
        [
            "report_type",
            "report_date",
            "file_checksum",
        ],
    )

    op.create_index(
        "ix_ingestion_batch_archive_retry",
        "ingestion_batch",
        ["received_at_utc"],
        postgresql_where=sa.text(
            """
            ingestion_status = 'COMMITTED'
            AND archive_status IN ('PENDING', 'FAILED')
            """
        ),
    )

    op.create_index(
        "ix_source_receipt_batch_disposition",
        "source_receipt",
        [
            "batch_id",
            "disposition",
        ],
    )

    op.create_index(
        "ix_source_receipt_entity",
        "source_receipt",
        [
            "entity_type",
            "source_id",
            "source_version",
        ],
    )

    op.create_index(
        "ix_source_record_version_entity_version",
        "source_record_version",
        [
            "entity_type",
            "source_id",
            sa.text("source_version DESC"),
        ],
    )

    op.create_index(
        "ix_source_record_version_first_batch",
        "source_record_version",
        ["first_seen_batch_id"],
    )

    op.create_index(
        "ix_entity_resolution_state",
        "entity_resolution",
        ["resolution_state"],
    )

    op.create_index(
        "ix_reconciliation_run_status_created",
        "reconciliation_run",
        [
            "status",
            "created_at_utc",
        ],
    )

    op.create_index(
        "ix_transaction_reconciliation_run_state",
        "transaction_reconciliation",
        [
            "run_id",
            "state",
        ],
    )

    op.create_index(
        "ix_payout_reconciliation_date",
        "payout_reconciliation",
        [
            "payout_date",
            "payout_id",
        ],
    )

    op.create_index(
        "ix_reconciliation_exception_run_type",
        "reconciliation_exception",
        [
            "run_id",
            "exception_type",
        ],
    )

    op.execute(
        """
        CREATE FUNCTION reject_source_record_version_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION
                'source_record_version is immutable';
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE TRIGGER trg_source_record_version_immutable
        BEFORE UPDATE OR DELETE
        ON source_record_version
        FOR EACH ROW
        EXECUTE FUNCTION
            reject_source_record_version_mutation();
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TRIGGER IF EXISTS
            trg_source_record_version_immutable
        ON source_record_version
        """
    )

    op.execute(
        """
        DROP FUNCTION IF EXISTS
            reject_source_record_version_mutation()
        """
    )

    op.drop_index(
        "ix_reconciliation_exception_run_type",
        table_name="reconciliation_exception",
    )
    op.drop_index(
        "ix_payout_reconciliation_date",
        table_name="payout_reconciliation",
    )
    op.drop_index(
        "ix_transaction_reconciliation_run_state",
        table_name="transaction_reconciliation",
    )
    op.drop_index(
        "ix_reconciliation_run_status_created",
        table_name="reconciliation_run",
    )
    op.drop_index(
        "ix_entity_resolution_state",
        table_name="entity_resolution",
    )
    op.drop_index(
        "ix_source_record_version_first_batch",
        table_name="source_record_version",
    )
    op.drop_index(
        "ix_source_record_version_entity_version",
        table_name="source_record_version",
    )
    op.drop_index(
        "ix_source_receipt_entity",
        table_name="source_receipt",
    )
    op.drop_index(
        "ix_source_receipt_batch_disposition",
        table_name="source_receipt",
    )
    op.drop_index(
        "ix_ingestion_batch_archive_retry",
        table_name="ingestion_batch",
    )
    op.drop_index(
        "ix_ingestion_batch_report_content",
        table_name="ingestion_batch",
    )
    op.drop_index(
        "ix_ingestion_batch_status_received",
        table_name="ingestion_batch",
    )