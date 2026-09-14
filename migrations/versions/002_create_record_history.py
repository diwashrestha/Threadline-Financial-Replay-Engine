"""Create immutable source record version history.

Revision ID: 002_record_history
Revises: 001_ingestion_ledger
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "002_record_history"
down_revision = "001_ingestion_ledger"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "source_record_version",
        sa.Column(
            "record_version_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "entity_type",
            sa.String(length=32),
            nullable=False,
        ),
        sa.Column(
            "source_id",
            sa.String(length=128),
            nullable=False,
        ),
        sa.Column(
            "source_version",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "payload_hash",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "canonical_payload",
            postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column(
            "first_seen_batch_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "first_seen_receipt_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "first_seen_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["first_seen_batch_id"],
            ["ingestion_batch.batch_id"],
            name="fk_record_version_first_batch",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["first_seen_receipt_id"],
            ["source_receipt.receipt_id"],
            name="fk_record_version_first_receipt",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "entity_type",
            "source_id",
            "source_version",
            "payload_hash",
            name="uq_source_record_version_identity_payload",
        ),
        sa.CheckConstraint(
            "source_version > 0",
            name="ck_source_record_version_positive",
        ),
        sa.CheckConstraint(
            "payload_hash ~ '^[0-9a-f]{64}$'",
            name="ck_source_record_version_payload_hash_sha256",
        ),
    )

    op.add_column(
        "source_receipt",
        sa.Column(
            "record_version_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
    )

    op.create_foreign_key(
        "fk_source_receipt_record_version",
        "source_receipt",
        "source_record_version",
        ["record_version_id"],
        ["record_version_id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_source_receipt_record_version",
        "source_receipt",
        type_="foreignkey",
    )
    op.drop_column(
        "source_receipt",
        "record_version_id",
    )
    op.drop_table("source_record_version")