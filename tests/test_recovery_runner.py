from types import SimpleNamespace

import pytest

from threadline.recovery_runner import drain_recovery_queue


class FakeWorker:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def run_next(self):
        self.calls += 1

        if not self.outcomes:
            return None

        outcome = self.outcomes.pop(0)

        if isinstance(outcome, Exception):
            raise outcome

        return outcome


def outcome(number):
    return SimpleNamespace(
        request_id=f"request-{number}",
        run_id=f"run-{number}",
        logical_fingerprint="a" * 64,
    )


def test_no_eligible_request_is_a_successful_noop():
    worker = FakeWorker([])

    summary = drain_recovery_queue(
        worker,
        clock=lambda: 0.0,
    )

    assert summary.processed_count == 0
    assert summary.stop_reason == "NO_READY_REQUEST"
    assert summary.last_run_id is None


def test_batch_limit_leaves_remaining_requests_unclaimed():
    worker = FakeWorker([
        outcome(1),
        outcome(2),
        outcome(3),
    ])

    summary = drain_recovery_queue(
        worker,
        max_requests=2,
        clock=lambda: 0.0,
    )

    assert summary.processed_count == 2
    assert summary.stop_reason == "BATCH_LIMIT"
    assert summary.last_run_id == "run-2"

    assert worker.calls == 2
    assert len(worker.outcomes) == 1


def test_time_budget_stops_before_claiming_another_request():
    worker = FakeWorker([
        outcome(1),
        outcome(2),
    ])

    # Start, first request check, second request check, final elapsed.
    times = iter([0.0, 0.0, 25.0, 25.0])

    summary = drain_recovery_queue(
        worker,
        max_seconds=20.0,
        clock=lambda: next(times),
    )

    assert summary.processed_count == 1
    assert summary.stop_reason == "TIME_BUDGET"

    assert worker.calls == 1
    assert len(worker.outcomes) == 1


def test_worker_failure_propagates_without_processing_another_request():
    worker = FakeWorker([
        RuntimeError("Candidate rejected"),
        outcome(2),
    ])

    with pytest.raises(RuntimeError, match="Candidate rejected"):
        drain_recovery_queue(
            worker,
            clock=lambda: 0.0,
        )

    assert worker.calls == 1
    assert len(worker.outcomes) == 1