"""Verify ordered acquisition of PostgreSQL entity locks."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import psycopg
import pytest

from threadline.entity_locks import (
    acquire_entity_locks,
    ordered_entity_keys,
)


pytestmark = pytest.mark.integration


INPUT_KEYS = (
    ("SETTLEMENT_LINE", "LINE-002"),
    ("PAYMENT", "PAY-002"),
    ("ORDER", "ORD-002"),
    ("REFUND", "REF-001"),
    ("FEE", "FEE-001"),
    ("PAYOUT", "OUT-001"),
    ("ORDER", "ORD-001"),
    ("PAYMENT", "PAY-001"),
    ("SETTLEMENT_LINE", "LINE-001"),
)


EXPECTED_ORDER = (
    ("ORDER", "ORD-001"),
    ("ORDER", "ORD-002"),
    ("FEE", "FEE-001"),
    ("PAYMENT", "PAY-001"),
    ("PAYMENT", "PAY-002"),
    ("PAYOUT", "OUT-001"),
    ("REFUND", "REF-001"),
    ("SETTLEMENT_LINE", "LINE-001"),
    ("SETTLEMENT_LINE", "LINE-002"),
)


def test_entity_order_matches_contract():
    assert ordered_entity_keys(INPUT_KEYS) == EXPECTED_ORDER

    assert ordered_entity_keys(
        reversed(INPUT_KEYS)
    ) == EXPECTED_ORDER

    assert ordered_entity_keys(
        INPUT_KEYS + INPUT_KEYS
    ) == EXPECTED_ORDER


def test_opposite_input_orders_acquire_same_lock_sequence(
    scenario_database_url,
):
    barrier = Barrier(2)

    def acquire(keys):
        with psycopg.connect(
            scenario_database_url,
            autocommit=True,
            connect_timeout=3,
        ) as connection:
            backend_pid = connection.execute(
                "SELECT pg_backend_pid()"
            ).fetchone()[0]

            with connection.transaction():
                connection.execute(
                    "SET LOCAL lock_timeout = '5s'"
                )

                barrier.wait(timeout=10)

                acquired = acquire_entity_locks(
                    connection,
                    keys,
                )

            return backend_pid, acquired

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(acquire, INPUT_KEYS),
            executor.submit(acquire, tuple(reversed(INPUT_KEYS))),
        ]

        results = [
            future.result(timeout=20)
            for future in futures
        ]

    assert len({
        backend_pid
        for backend_pid, _ in results
    }) == 2

    assert all(
        acquired == EXPECTED_ORDER
        for _, acquired in results
    )