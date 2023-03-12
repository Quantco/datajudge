import pytest

from datajudge.constraints.numeric import NumericPercentile
from datajudge.db_access import DataReference, TableDataSource

data_source = TableDataSource("my_db", "my_table")
ref = DataReference(data_source, ["column"], None)


@pytest.mark.parametrize("k", [-0.5, 100.5])
def test_invalid_k(k):
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
def test_invalid_deviations(max_absolute_deviation, max_relative_deviation):
    with pytest.raises(ValueError):
        NumericPercentile(ref, 50, max_absolute_deviation, max_relative_deviation)
