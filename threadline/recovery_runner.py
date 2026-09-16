"""Drain eligible recovery requests within a bounded batch.

Retry timing and publication remain responsibilities of the worker.
"""

from __future__ import annotations

import math
import time

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol


class RecoveryWorker(Protocol):
    def run_next(self) -> Any | None:
        ...


@dataclass(frozen=True)
class RecoveryDrainSummary:
    processed_count: int
    stop_reason: str
    elapsed_seconds: float
    last_request_id: str | None
    last_run_id: str | None
    last_logical_fingerprint: str | None


def drain_recovery_queue(
    worker: RecoveryWorker,
    *,
    max_requests: int = 10,
    max_seconds: float = 120.0,
    clock: Callable[[], float] = time.monotonic,
) -> RecoveryDrainSummary:
    """Process requests until no work is ready or a limit is reached.

    The time budget is checked before starting each request. It does
    not interrupt a request already executing.

    Worker exceptions propagate. The worker records durable failure
    information before raising, when the database remains available.
    """
    if type(max_requests) is not int or max_requests <= 0:
        raise ValueError("max_requests must be a positive integer")

    if (
        isinstance(max_seconds, bool)
        or not math.isfinite(max_seconds)
        or max_seconds <= 0
    ):
        raise ValueError("max_seconds must be positive and finite")

    started = clock()
    processed = 0

    last_request_id = None
    last_run_id = None
    last_logical_fingerprint = None

    while processed < max_requests:
        if clock() - started >= max_seconds:
            stop_reason = "TIME_BUDGET"
            break

        outcome = worker.run_next()

        if outcome is None:
            # None means no request is currently eligible.
            # Pending requests may still be waiting for retry time.
            stop_reason = "NO_READY_REQUEST"
            break

        processed += 1

        last_request_id = str(outcome.request_id)
        last_run_id = str(outcome.run_id)
        last_logical_fingerprint = outcome.logical_fingerprint

    else:
        stop_reason = "BATCH_LIMIT"

    return RecoveryDrainSummary(
        processed_count=processed,
        stop_reason=stop_reason,
        elapsed_seconds=round(clock() - started, 3),
        last_request_id=last_request_id,
        last_run_id=last_run_id,
        last_logical_fingerprint=last_logical_fingerprint,
    )