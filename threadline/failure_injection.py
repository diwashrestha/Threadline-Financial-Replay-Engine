"""Deterministic failure injection for recovery tests.

Injectors are explicitly supplied to a worker.
No random failures or environment-variable switches are used.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class FailurePoint(str, Enum):
    AFTER_VALIDATION = "after_validation"
    AFTER_RUN_INSERT = "after_run_insert"
    AFTER_RESULT_INSERT = "after_result_insert"
    BEFORE_POINTER_UPDATE = "before_pointer_update"
    AFTER_POINTER_UPDATE = "after_pointer_update"
    AFTER_QUEUE_COMPLETION = "after_queue_completion"
    BEFORE_COMMIT = "before_commit"
    AFTER_COMMIT = "after_commit"


class InjectedFailure(RuntimeError):
    """A deliberately injected failure at a named execution point."""

    def __init__(self, point: FailurePoint) -> None:
        self.point = point
        super().__init__(f"Controlled failure at {point.value}")


@dataclass(frozen=True, slots=True)
class FailureVisit:
    sequence: int
    point: FailurePoint
    injected: bool


class FailOnce:
    """Fail on the first visit to the selected point.

    Reuse the same instance for a retry: subsequent visits do not fail.
    """

    def __init__(self, point: FailurePoint | str) -> None:
        self.point = FailurePoint(point)
        self._fired = False
        self._visits: list[FailureVisit] = []

    @property
    def fired(self) -> bool:
        return self._fired

    @property
    def visits(self) -> tuple[FailureVisit, ...]:
        return tuple(self._visits)

    def __call__(self, point: FailurePoint | str) -> None:
        visited_point = FailurePoint(point)

        should_fail = (
            visited_point == self.point
            and not self._fired
        )

        self._visits.append(
            FailureVisit(
                sequence=len(self._visits) + 1,
                point=visited_point,
                injected=should_fail,
            )
        )

        if should_fail:
            self._fired = True
            raise InjectedFailure(visited_point)