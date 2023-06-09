import pytest

from datajudge import BetweenRequirement, WithinRequirement, pytest_integration


def test_collect_data_tests():
    req = WithinRequirement.from_raw_query("select * from example", "example_table")
    req.add_n_rows_equality_constraint(42)
    test_func = pytest_integration.collect_data_tests([req])
    (parametrize_mark,) = test_func.pytestmark
    assert parametrize_mark.name == "parametrize"
    assert parametrize_mark.args[1] == list(req)
    assert [parametrize_mark.kwargs["ids"](c) for c in parametrize_mark.args[1]] == [
        "NRowsEquality::example_table"
    ]


@pytest.mark.parametrize(
    "method_name,arguments",
    [
        ("add_date_min_constraint", ["column", "'2021-11-03'"]),
        ("add_varchar_min_length_constraint", ["column", 3]),
        ("add_numeric_mean_constraint", ["column", 1, 0.1]),
    ],
)
def test_test_name_within(method_name, arguments):
    req = WithinRequirement.from_raw_query("select * from example", "example_table")
    name = "my_amazing_constraint"
    getattr(req, method_name)(*arguments, name=name)
    test_func = pytest_integration.collect_data_tests([req])
    (parametrize_mark,) = test_func.pytestmark
    assert [parametrize_mark.kwargs["ids"](c) for c in parametrize_mark.args[1]] == [
        name
    ]


@pytest.mark.parametrize(
    "method_name,arguments",
    [
        ("add_date_min_constraint", ["column", "column"]),
        ("add_varchar_min_length_constraint", ["column", "column"]),
        ("add_ks_2sample_constraint", ["column", "column"]),
        ("add_numeric_mean_constraint", ["column", "column", 0.1]),
        ("add_row_superset_constraint", [["column"], ["column"], 0]),
    ],
)
def test_test_name_between(method_name, arguments):
    req = BetweenRequirement.from_raw_queries(
        "select * from example",
        "example_table",
        "select * from example",
        "example_table",
    )
    name = "my_amazing_constraint"
    getattr(req, method_name)(*arguments, name=name)
    test_func = pytest_integration.collect_data_tests([req])
    (parametrize_mark,) = test_func.pytestmark
    assert [parametrize_mark.kwargs["ids"](c) for c in parametrize_mark.args[1]] == [
        name
    ]
