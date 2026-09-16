"""PostgreSQL connection handling for Threadline."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg import Connection


class DatabaseConfigurationError(RuntimeError):
    """Raised when the business database URL is unavailable."""


def threadline_database_url(
    explicit_url: str | None = None,
) -> str:
    if explicit_url:
        return explicit_url

    environment_url = os.environ.get(
        "THREADLINE_DATABASE_URL"
    )

    if not environment_url:
        raise DatabaseConfigurationError(
            "THREADLINE_DATABASE_URL is required"
        )

    return environment_url


@contextmanager
def transaction_connection(
    database_url: str | None = None,
) -> Iterator[Connection]:
    """Yield one connection with one explicit transaction."""

    connection = psycopg.connect(
        threadline_database_url(database_url)
    )

    try:
        with connection.transaction():
            yield connection
    finally:
        connection.close()