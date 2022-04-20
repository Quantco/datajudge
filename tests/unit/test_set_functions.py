import pytest

from datajudge.constraints.uniques import (
    _is_subset,
    _is_superset,
    _subset_violation_counts,
)


@pytest.mark.parametrize(
    "test_data",
    [
        (range(10), range(9), True, set()),
        (range(10), range(10), True, set()),
        (range(9), range(10), False, {9}),
    ],
)
def test_is_superset(test_data):
    set1, set2, expected_is_superset, expected_remainder = test_data
    actual_is_superset, actual_remainder = _is_superset(set1, set2)
    assert actual_is_superset == expected_is_superset
    assert actual_remainder == expected_remainder


@pytest.mark.parametrize(
    "test_data",
    [
        (range(10), range(9), False, {9}),
        (range(10), range(10), True, set()),
        (range(9), range(10), True, set()),
    ],
)
def test_is_subset(test_data):
    set1, set2, expected_is_subset, expected_remainder = test_data
    actual_is_subset, actual_remainder = _is_subset(set1, set2)
    assert actual_is_subset == expected_is_subset
    assert actual_remainder == expected_remainder


@pytest.mark.parametrize(
    "test_data",
    [
        (range(10), [1] * 10, range(9), False, {9: 1}),
        (range(10), [1] * 10, range(10), True, dict()),
        (range(9), [1] * 9, range(10), True, dict()),
        (
            range(10),
            range(10),
            range(5),
            False,
            {item: count for item, count in zip(range(5, 10), range(5, 10))},
        ),
    ],
)
def test_subset_violation_counts(test_data):
    set1, counts, set2, expected_is_subset, expected_remainder = test_data
    actual_is_subset, actual_remainder = _subset_violation_counts(set1, counts, set2)
    assert actual_is_subset == expected_is_subset
    assert actual_remainder == expected_remainder
