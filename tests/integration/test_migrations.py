"""Integration tests for the Threadline Alembic migration chain."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import psycopg
import pytest
from alembic import command
from alembic.config import Config


pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]

EXPECTED_TABLES = {
    "alembic_version",
    "ingestion_batch",
    "source_receipt",
    "source_record_version",
    "entity_resolution",
    "reconciliation_run",
    "transaction_reconciliation",
    "payout_reconciliation",
    "reconciliation_exception",
    "source_completeness_snapshot",
    "publication_pointer",
}


@pytest.fixture(scope="session")
def database_url() -> str:
    value = os.environ.get(
        "THREADLINE_TEST_DATABASE_URL"
    )

    if not value:
        pytest.skip(
            "THREADLINE_TEST_DATABASE_URL is not configured"
        )

    with psycopg.connect(value) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT current_database()"
            )
            database_name = cursor.fetchone()[0]

    if not database_name.endswith("_test"):
        raise RuntimeError(
            "Migration integration tests require a database "
            "whose name ends with '_test'"
        )

    return value


@pytest.fixture()
def clean_database(
    database_url: str,
) -> Iterator[str]:
    with psycopg.connect(
        database_url,
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "DROP SCHEMA public CASCADE"
            )
            cursor.execute(
                "CREATE SCHEMA public"
            )

    yield database_url


def _alembic_config(database_url: str) -> Config:
    configuration = Config(
        str(PROJECT_ROOT / "alembic.ini")
    )

    configuration.set_main_option(
        "script_location",
        str(PROJECT_ROOT / "migrations"),
    )

    configuration.attributes[
        "database_url"
    ] = database_url

    return configuration


def _current_revision(database_url: str) -> str:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT version_num FROM alembic_version"
            )
            return cursor.fetchone()[0]


def _table_names(database_url: str) -> set[str]:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                """
            )

            return {
                row[0]
                for row in cursor.fetchall()
            }


def _index_names(database_url: str) -> set[str]:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = 'public'
                """
            )

            return {
                row[0]
                for row in cursor.fetchall()
            }


def test_fresh_database_upgrades_to_head(
    clean_database: str,
):
    configuration = _alembic_config(clean_database)

    command.upgrade(
        configuration,
        "head",
    )

    assert _current_revision(
        clean_database
    ) == "005_indexes_constraints"

    assert EXPECTED_TABLES <= _table_names(
        clean_database
    )

    indexes = _index_names(clean_database)

    assert "ix_ingestion_batch_archive_retry" in indexes
    assert "ix_source_receipt_entity" in indexes
    assert (
        "ix_source_record_version_entity_version"
        in indexes
    )
    assert (
        "ix_reconciliation_exception_run_type"
        in indexes
    )


def test_upgrade_from_preceding_revision_preserves_data(
    clean_database: str,
):
    configuration = _alembic_config(clean_database)

    command.upgrade(
        configuration,
        "004_reconciliation_publication",
    )

    assert _current_revision(
        clean_database
    ) == "004_reconciliation_publication"

    delivery_key = "a" * 64
    file_checksum = "b" * 64
    manifest_checksum = "c" * 64

    with psycopg.connect(clean_database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ingestion_batch (
                    delivery_key,
                    source_system,
                    report_type,
                    report_date,
                    original_filename,
                    file_checksum,
                    manifest_checksum,
                    schema_version,
                    declared_row_count,
                    observed_row_count
                )
                VALUES (
                    %s,
                    'TEST_PROVIDER',
                    'PAYMENTS',
                    DATE '2026-09-14',
                    'payments-before-upgrade.json',
                    %s,
                    %s,
                    '1',
                    1,
                    1
                )
                """,
                (
                    delivery_key,
                    file_checksum,
                    manifest_checksum,
                ),
            )

        connection.commit()

    command.upgrade(
        configuration,
        "head",
    )

    assert _current_revision(
        clean_database
    ) == "005_indexes_constraints"

    with psycopg.connect(clean_database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    original_filename,
                    declared_row_count,
                    ingestion_status
                FROM ingestion_batch
                WHERE delivery_key = %s
                """,
                (delivery_key,),
            )

            row = cursor.fetchone()

    assert row == (
        "payments-before-upgrade.json",
        1,
        "RECEIVED",
    )

    indexes = _index_names(clean_database)

    assert "ix_ingestion_batch_archive_retry" in indexes
    assert "ix_source_receipt_entity" in indexes


def test_last_revision_can_downgrade_and_reapply(
    clean_database: str,
):
    configuration = _alembic_config(clean_database)

    command.upgrade(
        configuration,
        "head",
    )

    command.downgrade(
        configuration,
        "004_reconciliation_publication",
    )

    assert _current_revision(
        clean_database
    ) == "004_reconciliation_publication"

    assert (
        "ix_ingestion_batch_archive_retry"
        not in _index_names(clean_database)
    )

    command.upgrade(
        configuration,
        "head",
    )

    assert _current_revision(
        clean_database
    ) == "005_indexes_constraints"

    assert (
        "ix_ingestion_batch_archive_retry"
        in _index_names(clean_database)
    )