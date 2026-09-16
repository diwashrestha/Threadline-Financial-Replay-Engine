"""Add durable archival retry metadata.

Revision ID: 008_retryable_archival
Revises: 007_recovery_queue_retries
"""

from alembic import op


revision = "008_retryable_archival"
down_revision = "007_recovery_queue_retries"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE ingestion_batch
            ADD COLUMN source_relative_path TEXT,
            ADD COLUMN archive_attempt_count INTEGER
                NOT NULL DEFAULT 0,
            ADD COLUMN archive_last_attempt_at_utc TIMESTAMPTZ,
            ADD COLUMN archive_error_message TEXT;

        ALTER TABLE ingestion_batch
            ADD CONSTRAINT ck_archive_attempt_count
                CHECK (archive_attempt_count >= 0),
            ADD CONSTRAINT ck_source_relative_path_nonempty
                CHECK (
                    source_relative_path IS NULL
                    OR length(source_relative_path) > 0
                );
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE ingestion_batch
            DROP CONSTRAINT ck_source_relative_path_nonempty,
            DROP CONSTRAINT ck_archive_attempt_count,
            DROP COLUMN archive_error_message,
            DROP COLUMN archive_last_attempt_at_utc,
            DROP COLUMN archive_attempt_count,
            DROP COLUMN source_relative_path;
        """
    )