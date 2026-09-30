"""Acquire transaction-level entity locks in a consistent order."""

from __future__ import annotations

import hashlib
import json

from collections.abc import Iterable

import psycopg
from psycopg.pq import TransactionStatus

from threadline.contracts import EntityType


# Separate advisory-lock namespace from the global financial lock.
ENTITY_LOCK_NAMESPACE = 740033


ENTITY_ORDER = (
    EntityType.ORDER,
    EntityType.FEE,
    EntityType.PAYMENT,
    EntityType.PAYOUT,
    EntityType.REFUND,
    EntityType.SETTLEMENT_LINE,
)


ENTITY_RANK = {
    entity_type.value: rank
    for rank, entity_type in enumerate(ENTITY_ORDER)
}


EntityKey = tuple[str, str]


def normalize_entity_key(
    key: tuple[EntityType | str, str],
) -> EntityKey:
    entity_type, source_id = key

    normalized_type = (
        entity_type
        if isinstance(entity_type, EntityType)
        else EntityType(entity_type)
    )

    if (
        not isinstance(source_id, str)
        or not source_id
        or source_id != source_id.strip()
    ):
        raise ValueError(
            "source_id must be a non-empty canonical string"
        )

    return normalized_type.value, source_id


def entity_sort_key(
    key: tuple[EntityType | str, str],
) -> tuple[int, str]:
    entity_type, source_id = normalize_entity_key(key)

    return ENTITY_RANK[entity_type], source_id


def ordered_entity_keys(
    keys: Iterable[tuple[EntityType | str, str]],
) -> tuple[EntityKey, ...]:
    normalized = {
        normalize_entity_key(key)
        for key in keys
    }

    return tuple(
        sorted(
            normalized,
            key=entity_sort_key,
        )
    )


def _entity_lock_number(key: EntityKey) -> int:
    """Generate a stable signed PostgreSQL integer lock key.

    Do not use Python's hash(): its output varies between processes.
    """

    encoded = json.dumps(
        key,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

    digest = hashlib.sha256(encoded).digest()

    return int.from_bytes(
        digest[:4],
        byteorder="big",
        signed=True,
    )


def acquire_entity_locks(
    connection: psycopg.Connection,
    keys: Iterable[tuple[EntityType | str, str]],
) -> tuple[EntityKey, ...]:
    """Acquire the complete lock set once, before entity writes.

    The caller must already hold the global financial lock.

    Ordering:
        ORDER
        FEE
        PAYMENT
        PAYOUT
        REFUND
        SETTLEMENT_LINE

    Within each type, source_id is sorted lexicographically.
    """

    if connection.info.transaction_status != TransactionStatus.INTRANS:
        raise RuntimeError(
            "Entity locks require an active transaction"
        )

    ordered = ordered_entity_keys(keys)

    for key in ordered:
        connection.execute(
            """
            SELECT pg_advisory_xact_lock(
                %s::integer,
                %s::integer
            )
            """,
            (
                ENTITY_LOCK_NAMESPACE,
                _entity_lock_number(key),
            ),
        )

    return ordered