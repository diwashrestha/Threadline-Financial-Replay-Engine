"""Add retry scheduling and recovery attempt history.

Revision ID: 007_recovery_queue_retries
Revises: 006_full_rebuild_recovery
"""

from alembic import op


revision = "007_recovery_queue_retries"
down_revision = "006_full_rebuild_recovery"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE recovery_request
            ADD COLUMN next_attempt_at_utc TIMESTAMPTZ
                NOT NULL DEFAULT CURRENT_TIMESTAMP,
            ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 5,
            ADD COLUMN base_retry_delay_seconds INTEGER
                NOT NULL DEFAULT 30;

        ALTER TABLE recovery_request
            ADD CONSTRAINT ck_recovery_max_attempts
                CHECK (max_attempts >= 1),
            ADD CONSTRAINT ck_recovery_retry_delay
                CHECK (base_retry_delay_seconds BETWEEN 0 AND 3600);

        ALTER TABLE recovery_request
            DROP CONSTRAINT ck_recovery_status,
            DROP CONSTRAINT ck_recovery_completion;

        ALTER TABLE recovery_request
            ADD CONSTRAINT ck_recovery_status
                CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED')),
            ADD CONSTRAINT ck_recovery_completion
                CHECK (
                    (
                        status = 'PENDING'
                        AND result_run_id IS NULL
                        AND completed_at_utc IS NULL
                    )
                    OR
                    (
                        status = 'SUCCEEDED'
                        AND result_run_id IS NOT NULL
                        AND completed_at_utc IS NOT NULL
                    )
                    OR
                    (
                        status = 'FAILED'
                        AND result_run_id IS NULL
                        AND completed_at_utc IS NOT NULL
                        AND last_error IS NOT NULL
                    )
                );

        -- Handle any already-exhausted pending requests.
        UPDATE recovery_request
        SET
            status = 'FAILED',
            completed_at_utc = CURRENT_TIMESTAMP,
            last_error = COALESCE(
                last_error,
                'Attempt limit reached before migration 007'
            )
        WHERE status = 'PENDING'
          AND attempt_count >= max_attempts;

        ALTER TABLE recovery_request
            ADD CONSTRAINT ck_recovery_pending_budget
                CHECK (
                    status <> 'PENDING'
                    OR attempt_count < max_attempts
                );

        DROP INDEX ix_recovery_request_pending;

        CREATE INDEX ix_recovery_request_pending
        ON recovery_request (
            next_attempt_at_utc,
            created_at_utc,
            request_id
        )
        WHERE status = 'PENDING';

        CREATE TABLE recovery_attempt (
            request_id UUID NOT NULL
                REFERENCES recovery_request(request_id)
                ON DELETE RESTRICT,

            attempt_number INTEGER NOT NULL,
            outcome VARCHAR(16) NOT NULL,

            run_id UUID
                REFERENCES reconciliation_run(run_id)
                ON DELETE RESTRICT,

            error_message TEXT,

            recorded_at_utc TIMESTAMPTZ NOT NULL
                DEFAULT CURRENT_TIMESTAMP,

            PRIMARY KEY (request_id, attempt_number),

            CONSTRAINT ck_recovery_attempt_number
                CHECK (attempt_number >= 1),

            CONSTRAINT ck_recovery_attempt_outcome
                CHECK (
                    (
                        outcome = 'SUCCEEDED'
                        AND run_id IS NOT NULL
                        AND error_message IS NULL
                    )
                    OR
                    (
                        outcome = 'FAILED'
                        AND run_id IS NULL
                        AND error_message IS NOT NULL
                    )
                )
        );
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE recovery_attempt;

        ALTER TABLE recovery_request
            DROP CONSTRAINT ck_recovery_pending_budget,
            DROP CONSTRAINT ck_recovery_status,
            DROP CONSTRAINT ck_recovery_completion;

        -- The preceding schema does not support terminal failures.
        UPDATE recovery_request
        SET
            status = 'PENDING',
            completed_at_utc = NULL
        WHERE status = 'FAILED';

        ALTER TABLE recovery_request
            ADD CONSTRAINT ck_recovery_status
                CHECK (status IN ('PENDING', 'SUCCEEDED')),
            ADD CONSTRAINT ck_recovery_completion
                CHECK (
                    (
                        status = 'PENDING'
                        AND result_run_id IS NULL
                        AND completed_at_utc IS NULL
                    )
                    OR
                    (
                        status = 'SUCCEEDED'
                        AND result_run_id IS NOT NULL
                        AND completed_at_utc IS NOT NULL
                    )
                );

        DROP INDEX ix_recovery_request_pending;

        CREATE INDEX ix_recovery_request_pending
        ON recovery_request (created_at_utc, request_id)
        WHERE status = 'PENDING';

        ALTER TABLE recovery_request
            DROP CONSTRAINT ck_recovery_max_attempts,
            DROP CONSTRAINT ck_recovery_retry_delay,
            DROP COLUMN next_attempt_at_utc,
            DROP COLUMN max_attempts,
            DROP COLUMN base_retry_delay_seconds;
        """
    )