import pytest

from datajudge.constraints.numeric import NumericPercentile
from datajudge.db_access import DataReference, TableDataSource


@pytest.fixture(scope="module")
def ref():
    data_source = TableDataSource("my_db", "my_table")
    return DataReference(data_source, ["column"], None)


@pytest.mark.parametrize("k", [-0.5, 100.5])
def test_invalid_k(ref, k):
    with pytest.raises(ValueError):
        NumericPercentile(ref, k, 0, 0)


@pytest.mark.parametrize(
    "max_absolute_deviation,max_relative_deviation",
    [
        (None, None),
        (-1, 1),
        (1, -1),
        (-1, -1),
        (-1, None),
        (None, -1),
    ],
)
def test_invalid_deviations(ref, max_absolute_deviation, max_relative_deviation):
    with pytest.raises(ValueError):
        NumericPercentile(ref, 50, max_absolute_deviation, max_relative_deviation)
