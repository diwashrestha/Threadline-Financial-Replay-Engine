"""Add durable full-rebuild recovery.

Revision ID: 006_full_rebuild_recovery
Revises: 005_indexes_constraints
"""

from alembic import op


revision = "006_full_rebuild_recovery"
down_revision = "005_indexes_constraints"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE recovery_request (
            request_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

            request_key VARCHAR(64) NOT NULL UNIQUE,
            trigger_receipt_id UUID NOT NULL
                REFERENCES source_receipt(receipt_id)
                ON DELETE RESTRICT,

            reason_code VARCHAR(64) NOT NULL,
            recovery_scope VARCHAR(16) NOT NULL DEFAULT 'FULL',

            -- Frozen evaluation time: retries use the same time.
            as_of_utc TIMESTAMPTZ NOT NULL,

            status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
            attempt_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,

            result_run_id UUID
                REFERENCES reconciliation_run(run_id)
                ON DELETE RESTRICT,

            created_at_utc TIMESTAMPTZ NOT NULL
                DEFAULT CURRENT_TIMESTAMP,
            completed_at_utc TIMESTAMPTZ,

            CONSTRAINT ck_recovery_request_key
                CHECK (request_key ~ '^[0-9a-f]{64}$'),

            CONSTRAINT ck_recovery_scope
                CHECK (recovery_scope = 'FULL'),

            CONSTRAINT ck_recovery_status
                CHECK (status IN ('PENDING', 'SUCCEEDED')),

            CONSTRAINT ck_recovery_attempt_count
                CHECK (attempt_count >= 0),

            CONSTRAINT ck_recovery_completion
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
                )
        )
        """
    )

    op.execute(
        """
        CREATE INDEX ix_recovery_request_pending
        ON recovery_request (created_at_utc, request_id)
        WHERE status = 'PENDING'
        """
    )

    op.execute(
        """
        CREATE TABLE recovery_result (
            run_id UUID PRIMARY KEY
                REFERENCES reconciliation_run(run_id)
                ON DELETE RESTRICT,

            input_fingerprint VARCHAR(64) NOT NULL,
            logical_fingerprint VARCHAR(64) NOT NULL,

            -- Complete output, including audit and quarantine evidence.
            result_payload JSONB NOT NULL,

            created_at_utc TIMESTAMPTZ NOT NULL
                DEFAULT CURRENT_TIMESTAMP,

            CONSTRAINT ck_recovery_input_fingerprint
                CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),

            CONSTRAINT ck_recovery_logical_fingerprint
                CHECK (logical_fingerprint ~ '^[0-9a-f]{64}$')
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE recovery_result")
    op.execute("DROP TABLE recovery_request")