import pytest

from threadline.failure_injection import (
    FailOnce,
    FailurePoint,
    InjectedFailure,
)


def test_injector_fails_once_and_records_visits():
    injector = FailOnce(FailurePoint.BEFORE_COMMIT)

    injector("after_validation")

    with pytest.raises(InjectedFailure) as caught:
        injector("before_commit")

    assert caught.value.point == FailurePoint.BEFORE_COMMIT
    assert injector.fired is True

    # Reusing the injector does not inject another failure.
    injector("before_commit")

    assert [visit.sequence for visit in injector.visits] == [1, 2, 3]
    assert [visit.injected for visit in injector.visits] == [
        False,
        True,
        False,
    ]


@pytest.mark.parametrize("point", list(FailurePoint))
def test_each_named_point_can_be_injected(point):
    injector = FailOnce(point)

    with pytest.raises(
        InjectedFailure,
        match=point.value,
    ):
        injector(point.value)

    injector(point.value)


def test_invalid_point_is_rejected():
    with pytest.raises(ValueError):
        FailOnce("misspelled_failure_point")