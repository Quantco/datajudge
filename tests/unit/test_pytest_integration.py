from datajudge import WithinRequirement, pytest_integration


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
