"""Alembic environment for the Threadline business database."""

from __future__ import annotations

import os
from logging.config import fileConfig

import sqlalchemy as sa
from alembic import context
from sqlalchemy import pool


config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Migrations are written explicitly. Stage 3 does not require ORM models.
target_metadata = None


def _database_url() -> str:
    """Return the Threadline business database URL.

    Programmatic tests provide the URL through config.attributes.
    Docker and command-line runs use THREADLINE_DATABASE_URL.
    """

    configured_url = config.attributes.get("database_url")

    if configured_url:
        return str(configured_url)

    environment_url = os.environ.get(
        "THREADLINE_DATABASE_URL"
    )

    if not environment_url:
        raise RuntimeError(
            "THREADLINE_DATABASE_URL is required"
        )

    return environment_url


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={
            "paramstyle": "named",
        },
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = sa.create_engine(
        _database_url(),
        poolclass=pool.NullPool,
        future=True,
    )

    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
            transaction_per_migration=True,
        )

        with context.begin_transaction():
            context.run_migrations()

    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()