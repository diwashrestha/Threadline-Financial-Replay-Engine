"""Create ingestion batch and source receipt ledger.

Revision ID: 001_ingestion_ledger
Revises: None
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "001_ingestion_ledger"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ingestion_batch",
        sa.Column(
            "batch_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "delivery_key",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "source_system",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "report_type",
            sa.String(length=32),
            nullable=False,
        ),
        sa.Column(
            "report_date",
            sa.Date(),
            nullable=False,
        ),
        sa.Column(
            "original_filename",
            sa.Text(),
            nullable=False,
        ),
        sa.Column(
            "file_checksum",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "manifest_checksum",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "schema_version",
            sa.String(length=32),
            nullable=False,
        ),
        sa.Column(
            "declared_row_count",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "observed_row_count",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "ingestion_status",
            sa.String(length=16),
            nullable=False,
            server_default=sa.text("'RECEIVED'"),
        ),
        sa.Column(
            "archive_status",
            sa.String(length=16),
            nullable=False,
            server_default=sa.text("'PENDING'"),
        ),
        sa.Column(
            "archive_path",
            sa.Text(),
            nullable=True,
        ),
        sa.Column(
            "received_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "committed_at_utc",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "archived_at_utc",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "error_message",
            sa.Text(),
            nullable=True,
        ),
        sa.UniqueConstraint(
            "delivery_key",
            name="uq_ingestion_batch_delivery_key",
        ),
        sa.CheckConstraint(
            "delivery_key ~ '^[0-9a-f]{64}$'",
            name="ck_ingestion_batch_delivery_key_sha256",
        ),
        sa.CheckConstraint(
            "file_checksum ~ '^[0-9a-f]{64}$'",
            name="ck_ingestion_batch_file_checksum_sha256",
        ),
        sa.CheckConstraint(
            "manifest_checksum ~ '^[0-9a-f]{64}$'",
            name="ck_ingestion_batch_manifest_checksum_sha256",
        ),
        sa.CheckConstraint(
            "declared_row_count >= 0",
            name="ck_ingestion_batch_declared_rows_nonnegative",
        ),
        sa.CheckConstraint(
            "observed_row_count >= 0",
            name="ck_ingestion_batch_observed_rows_nonnegative",
        ),
        sa.CheckConstraint(
            """
            ingestion_status IN (
                'RECEIVED',
                'PROCESSING',
                'COMMITTED',
                'FAILED'
            )
            """,
            name="ck_ingestion_batch_ingestion_status",
        ),
        sa.CheckConstraint(
            """
            archive_status IN (
                'PENDING',
                'ARCHIVED',
                'FAILED'
            )
            """,
            name="ck_ingestion_batch_archive_status",
        ),
        sa.CheckConstraint(
            """
            ingestion_status <> 'COMMITTED'
            OR committed_at_utc IS NOT NULL
            """,
            name="ck_ingestion_batch_commit_timestamp",
        ),
        sa.CheckConstraint(
            """
            archive_status <> 'ARCHIVED'
            OR (
                archived_at_utc IS NOT NULL
                AND archive_path IS NOT NULL
            )
            """,
            name="ck_ingestion_batch_archive_metadata",
        ),
    )

    op.create_table(
        "source_receipt",
        sa.Column(
            "receipt_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "batch_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "row_number",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "entity_type",
            sa.String(length=32),
            nullable=True,
        ),
        sa.Column(
            "source_id",
            sa.String(length=128),
            nullable=True,
        ),
        sa.Column(
            "source_version",
            sa.Integer(),
            nullable=True,
        ),
        sa.Column(
            "payload_hash",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "raw_payload",
            postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column(
            "disposition",
            sa.String(length=16),
            nullable=False,
            server_default=sa.text("'PENDING'"),
        ),
        sa.Column(
            "reason_code",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column(
            "received_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["batch_id"],
            ["ingestion_batch.batch_id"],
            name="fk_source_receipt_batch",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "batch_id",
            "row_number",
            name="uq_source_receipt_batch_row",
        ),
        sa.CheckConstraint(
            "row_number > 0",
            name="ck_source_receipt_row_number_positive",
        ),
        sa.CheckConstraint(
            """
            source_version IS NULL
            OR source_version > 0
            """,
            name="ck_source_receipt_version_positive",
        ),
        sa.CheckConstraint(
            "payload_hash ~ '^[0-9a-f]{64}$'",
            name="ck_source_receipt_payload_hash_sha256",
        ),
        sa.CheckConstraint(
            """
            disposition IN (
                'PENDING',
                'ACCEPTED',
                'STALE',
                'DUPLICATE',
                'CONFLICTED',
                'QUARANTINED'
            )
            """,
            name="ck_source_receipt_disposition",
        ),
        sa.CheckConstraint(
            """
            disposition <> 'QUARANTINED'
            OR reason_code IS NOT NULL
            """,
            name="ck_source_receipt_quarantine_reason",
        ),
    )


def downgrade() -> None:
    op.drop_table("source_receipt")
    op.drop_table("ingestion_batch")