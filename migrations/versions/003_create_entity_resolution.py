"""Create durable entity resolution state.

Revision ID: 003_entity_resolution
Revises: 002_record_history
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "003_entity_resolution"
down_revision = "002_record_history"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "entity_resolution",
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
            "winning_version",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "resolution_state",
            sa.String(length=16),
            nullable=False,
        ),
        sa.Column(
            "selected_payload_hash",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.PrimaryKeyConstraint(
            "entity_type",
            "source_id",
            name="pk_entity_resolution",
        ),
        sa.ForeignKeyConstraint(
            [
                "entity_type",
                "source_id",
                "winning_version",
                "selected_payload_hash",
            ],
            [
                "source_record_version.entity_type",
                "source_record_version.source_id",
                "source_record_version.source_version",
                "source_record_version.payload_hash",
            ],
            name="fk_entity_resolution_selected_version",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "winning_version > 0",
            name="ck_entity_resolution_winning_version",
        ),
        sa.CheckConstraint(
            """
            resolution_state IN (
                'ACCEPTED',
                'CONFLICTED'
            )
            """,
            name="ck_entity_resolution_state",
        ),
        sa.CheckConstraint(
            """
            (
                resolution_state = 'ACCEPTED'
                AND selected_payload_hash IS NOT NULL
            )
            OR
            (
                resolution_state = 'CONFLICTED'
                AND selected_payload_hash IS NULL
            )
            """,
            name="ck_entity_resolution_selected_payload",
        ),
        sa.CheckConstraint(
            """
            selected_payload_hash IS NULL
            OR selected_payload_hash ~ '^[0-9a-f]{64}$'
            """,
            name="ck_entity_resolution_payload_hash_sha256",
        ),
    )


def downgrade() -> None:
    op.drop_table("entity_resolution")