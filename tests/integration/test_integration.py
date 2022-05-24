import functools

import pytest

import datajudge.requirements as requirements
from datajudge.db_access import Condition, is_mssql, is_postgresql, is_snowflake


def skip_if_mssql(engine):
    if is_mssql(engine):
        pytest.skip("array_agg not supported by SQL Server")


def identity(boolean_value):
    return boolean_value


def negation(boolean_value):
    return not boolean_value


@pytest.mark.parametrize(
    "data",
    [
        (identity, 19, None),
        (identity, 9, Condition(raw_string="col_int > 10")),
        (negation, 20, None),
    ],
)
def test_n_rows_equality_within(engine, int_table1, data):
    (operation, n_rows, condition) = data
    req = requirements.WithinRequirement.from_table(*int_table1)
    req.add_n_rows_equality_constraint(n_rows, condition=condition)
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, 18, None),
        (identity, 19, None),
        (negation, 20, None),
        (identity, 9, Condition(raw_string="col_int > 10")),
        (negation, 10, Condition(raw_string="col_int > 10")),
    ],
)
def test_n_rows_min_within(engine, int_table1, data):
    (operation, n_rows, condition) = data
    req = requirements.WithinRequirement.from_table(*int_table1)
    req.add_n_rows_min_constraint(n_rows, condition=condition)
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (negation, 18, None),
        (identity, 19, None),
        (identity, 20, None),
        (negation, 8, Condition(raw_string="col_int > 10")),
        (identity, 9, Condition(raw_string="col_int > 10")),
    ],
)
def test_n_rows_max_within(engine, int_table1, data):
    (operation, n_rows, condition) = data
    req = requirements.WithinRequirement.from_table(*int_table1)
    req.add_n_rows_max_constraint(n_rows, condition=condition)
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (negation, None, None),
        (
            identity,
            Condition(raw_string="col_int >= 10"),
            Condition(raw_string="col_int >= 10"),
        ),
    ],
)
def test_n_rows_equality_between(engine, int_table1, int_table2, data):
    (operation, condition1, condition2) = data
    req = requirements.BetweenRequirement.from_tables(*int_table1, *int_table2)
    req.add_n_rows_equality_constraint(condition1=condition1, condition2=condition2)
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, 0.06, None, None, None),
        (negation, 0.05, None, None, None),
        (identity, None, -1, None, None),
        (negation, None, -1.5, None, None),
    ],
)
def test_n_rows_max_gain_between(engine, mix_table1, mix_table2, data):
    (
        operation,
        constant_max_relative_gain,
        date_range_gain_deviation,
        condition1,
        condition2,
    ) = data
    req = requirements.BetweenRequirement.from_tables(
        *mix_table1,
        *mix_table2,
        date_column="col_date",
        date_column2="col_date",
    )
    req.add_n_rows_max_gain_constraint(
        constant_max_relative_gain=constant_max_relative_gain,
        date_range_gain_deviation=date_range_gain_deviation,
        condition1=condition1,
        condition2=condition2,
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, 0.05, None, None, None),
        (negation, 0.06, None, None, None),
        (identity, 1, None, None, Condition(raw_string="col_int >= 100")),
        (negation, 0, None, Condition(raw_string="col_int >= 100"), None),
    ],
)
def test_n_rows_min_gain_between(engine, min_gain_table1, min_gain_table2, data):
    (
        operation,
        constant_min_relative_gain,
        date_range_gain_deviation,
        condition1,
        condition2,
    ) = data
    req = requirements.BetweenRequirement.from_tables(
        *min_gain_table1, *min_gain_table2
    )
    req.add_n_rows_min_gain_constraint(
        constant_min_relative_gain=constant_min_relative_gain,
        date_range_gain_deviation=date_range_gain_deviation,
        condition1=condition1,
        condition2=condition2,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, 0.053, None, None, None),
        (negation, 0.042, None, None, None),
        (
            identity,
            0,
            None,
            Condition(raw_string="col_int >= 9"),
            Condition(raw_string="col_int >= 9"),
        ),
        (
            identity,
            0.5,
            None,
            Condition(raw_string="col_int >= 15"),
            Condition(raw_string="col_int >= 10"),
        ),
        (
            negation,
            0.49,
            None,
            Condition(raw_string="col_int >= 15"),
            Condition(raw_string="col_int >= 10"),
        ),
    ],
)
def test_n_rows_max_loss_between(engine, int_table2, int_table1, data):
    (
        operation,
        constant_max_relative_loss,
        date_range_loss_deviation,
        condition2,
        condition1,
    ) = data
    req = requirements.BetweenRequirement.from_tables(*int_table2, *int_table1)
    req.add_n_rows_max_loss_constraint(
        constant_max_relative_loss=constant_max_relative_loss,
        date_range_loss_deviation=date_range_loss_deviation,
        condition1=condition2,
        condition2=condition1,
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, ["col_int"], 31, None),
        (negation, ["col_int"], 30, None),
        (negation, ["col_int"], 32, None),
        (
            identity,
            ["col_int"],
            10,
            Condition(
                conditions=[
                    Condition(raw_string="col_int >= 5"),
                    Condition(raw_string="col_int < 15"),
                ],
                reduction_operator="and",
            ),
        ),
        (
            negation,
            ["col_int"],
            11,
            Condition(
                conditions=[
                    Condition(raw_string="col_int >= 5"),
                    Condition(raw_string="col_int < 15"),
                ],
                reduction_operator="and",
            ),
        ),
        (
            negation,
            ["col_int"],
            9,
            Condition(
                conditions=[
                    Condition(raw_string="col_int >= 5"),
                    Condition(raw_string="col_int < 15"),
                ],
                reduction_operator="and",
            ),
        ),
        (negation, ["col_int", "col_varchar"], 41, None),
        (negation, None, 41, None),
        (identity, ["col_int", "col_varchar"], 42, None),
        (identity, None, 42, None),
        (negation, ["col_int", "col_varchar"], 43, None),
        (negation, None, 43, None),
    ],
)
def test_n_uniques_equality_within(engine, unique_table1, data):
    (operation, columns, n_uniques, condition) = data
    req = requirements.WithinRequirement.from_table(*unique_table1)
    req.add_n_uniques_equality_constraint(columns, n_uniques, condition=condition)
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, ["col_int"], range(30), None, None),
        (negation, ["col_int"], range(29), None, None),
        (negation, ["col_int"], range(31), None, None),
        (
            negation,
            ["col_varchar"],
            [f"HI{i}" for i in range(20)] + ["HI"],
            None,
            None,
        ),
        (
            identity,
            ["col_varchar"],
            [f"HI{i}" for i in range(20)] + ["HI"],
            str.upper,
            None,
        ),
        (negation, ["col_varchar"], {f"Hi{i}" for i in range(20)}, None, None),
        (
            identity,
            ["col_int", "col_varchar"],
            [(0, "hi0"), (1, "hi0")],
            None,
            Condition(raw_string="col_varchar = 'hi0'"),
        ),
        (negation, ["col_int", "col_varchar"], [(0, "hi0"), (1, "hi0")], None, None),
    ],
)
def test_uniques_equality_within(engine, unique_table1, data):
    (operation, columns, uniques, function, condition) = data
    req = requirements.WithinRequirement.from_table(*unique_table1)
    req.add_uniques_equality_constraint(
        columns, uniques, condition=condition, map_func=function
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (negation, ["col_int"], ["col_int"], None, None, None),
        (
            identity,
            ["col_int"],
            ["col_int"],
            None,
            Condition(raw_string="col_int < 20"),
            None,
        ),
        (
            negation,
            ["col_int"],
            ["col_int"],
            None,
            Condition(raw_string="col_int <= 20"),
            None,
        ),
        (negation, ["col_int"], ["col_int"], lambda x: x // 2, None, None),
        (
            identity,
            ["col_int"],
            ["col_int"],
            lambda x: x // 2,
            Condition(raw_string="col_int <= 18"),
            None,
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            ["col_int", "col_varchar"],
            None,
            None,
            None,
        ),
        (
            identity,
            ["col_int", "col_varchar"],
            ["col_int", "col_varchar"],
            None,
            Condition(raw_string="col_int < 20"),
            None,
        ),
    ],
)
def test_uniques_equality_between(engine, unique_table1, unique_table2, data):
    (operation, columns1, columns2, map_func, condition1, condition2) = data
    req = requirements.BetweenRequirement.from_tables(*unique_table1, *unique_table2)
    req.add_uniques_equality_constraint(
        columns1,
        columns2,
        map_func=map_func,
        condition1=condition1,
        condition2=condition2,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, ["col_int"], range(30), 0, None, None),
        (identity, ["col_int"], range(29), 0, None, None),
        (negation, ["col_int"], range(31), 0, None, None),
        (identity, ["col_int"], range(31), 1 / 31, None, None),
        (
            negation,
            ["col_varchar"],
            [f"HI{i}" for i in range(20)] + ["HI"],
            0,
            None,
            None,
        ),
        (
            identity,
            ["col_varchar"],
            [f"HI{i}" for i in range(20)] + ["HI"],
            0,
            str.upper,
            None,
        ),
        (identity, ["col_varchar"], {f"Hi{i}" for i in range(20)}, 0, str.title, None),
        (negation, ["col_varchar"], {f"Hi{i}" for i in range(20)}, 0, None, None),
    ],
)
def test_uniques_superset_within(engine, unique_table1, data):
    (operation, columns, uniques, max_relative_violations, function, condition) = data
    req = requirements.WithinRequirement.from_table(*unique_table1)
    req.add_uniques_superset_constraint(
        columns,
        uniques=uniques,
        max_relative_violations=max_relative_violations,
        condition=condition,
        map_func=function,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, ["col_int"], ["col_int"], 0, None, None, None),
        (
            identity,
            ["col_int"],
            ["col_int"],
            0,
            None,
            Condition(raw_string="col_int < 20"),
            None,
        ),
        (
            negation,
            ["col_int"],
            ["col_int"],
            0,
            None,
            Condition(raw_string="col_int < 19"),
            None,
        ),
        (
            identity,
            ["col_int"],
            ["col_int"],
            1 / 20,
            None,
            Condition(raw_string="col_int < 19"),
            None,
        ),
        (
            negation,
            ["col_int"],
            ["col_int"],
            1 / 21,
            None,
            Condition(raw_string="col_int < 19"),
            None,
        ),
    ],
)
def test_uniques_superset_between(engine, unique_table1, unique_table2, data):
    (
        operation,
        columns1,
        columns2,
        max_relative_violations,
        map_func,
        condition1,
        condition2,
    ) = data
    req = requirements.BetweenRequirement.from_tables(*unique_table1, *unique_table2)
    req.add_uniques_superset_constraint(
        columns1,
        columns2,
        max_relative_violations=max_relative_violations,
        map_func=map_func,
        condition1=condition1,
        condition2=condition2,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, ["col_int"], range(30), 0, None, None),
        (negation, ["col_int"], range(29), 0, None, None),
        (identity, ["col_int"], range(29), 2 / 60, None, None),
        (negation, ["col_int"], range(29), 2 / 61, None, None),
        (identity, ["col_int"], range(31), 0, None, None),
        (
            identity,
            ["col_varchar"],
            [f"Hi{i}" for i in range(20)] + ["Hi"],
            0,
            str.title,
            None,
        ),
        (
            negation,
            ["col_varchar"],
            [f"Hi{i}" for i in range(19)] + ["Hi"],
            0,
            str.title,
            None,
        ),
        (
            negation,
            ["col_varchar"],
            [f"Hi{i}" for i in range(20)] + ["Hi"],
            0,
            None,
            None,
        ),
        (
            negation,
            ["col_varchar"],
            [f"hi{i}" for i in range(5)] + ["hi"],
            0.2,
            None,
            None,
        ),
        (
            identity,
            ["col_varchar"],
            [f"hi{i}" for i in range(18)],
            0.2,
            None,
            None,
        ),
    ],
)
def test_uniques_subset_within(engine, unique_table1, data):
    (operation, columns, uniques, max_relative_violations, function, condition) = data
    req = requirements.WithinRequirement.from_table(*unique_table1)
    req.add_uniques_subset_constraint(
        columns,
        uniques,
        max_relative_violations,
        condition=condition,
        map_func=function,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (negation, ["col_int"], ["col_int"], 0, None, None, None),
        (identity, ["col_int"], ["col_int"], 20 / 60, None, None, None),
        (negation, ["col_int"], ["col_int"], 19 / 60, None, None, None),
        (
            negation,
            ["col_int"],
            ["col_int"],
            0,
            None,
            Condition(raw_string="col_int < 21"),
            None,
        ),
        (
            identity,
            ["col_int"],
            ["col_int"],
            0,
            None,
            Condition(raw_string="col_int < 20"),
            None,
        ),
    ],
)
def test_uniques_subset_between(engine, unique_table1, unique_table2, data):
    (
        operation,
        columns1,
        columns2,
        max_relative_violations,
        map_func,
        condition1,
        condition2,
    ) = data
    req = requirements.BetweenRequirement.from_tables(*unique_table1, *unique_table2)
    req.add_uniques_subset_constraint(
        columns1,
        columns2,
        max_relative_violations=max_relative_violations,
        map_func=map_func,
        condition1=condition1,
        condition2=condition2,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


def _flatten_and_filter(data):
    # Flattening one level
    res = []
    for d in data:
        res.extend(d)

    # returning an Iterable (set to get rid of duplicates in this case) without the empty string
    return {e for e in res if e}


def test_uniques_nested(engine, nested_table):
    req = requirements.WithinRequirement.from_table(*nested_table)
    splitter = functools.partial(str.split, sep=",")
    req.add_uniques_superset_constraint(
        ["nested_varchar"],
        ["ABC#1", "DEF#2", "GHI#3", "JKL#4"],
        map_func=splitter,
        reduce_func=_flatten_and_filter,
    )
    assert identity(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (
            negation,
            ["col_int", "col_varchar"],
            ["col_int", "col_varchar"],
            None,
            None,
        ),
        (
            identity,
            ["col_int", "col_varchar"],
            ["col_int", "col_varchar"],
            Condition(raw_string="col_int <= 19"),
            None,
        ),
        (negation, ["col_int", "col_varchar"], ["col_int"], None, None),
    ],
)
def test_n_uniques_equality_between(engine, unique_table1, unique_table2, data):
    (operation, columns1, columns2, condition1, condition2) = data
    req = requirements.BetweenRequirement.from_tables(*unique_table1, *unique_table2)
    req.add_n_uniques_equality_constraint(
        columns1, columns2, condition1=condition1, condition2=condition2
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (
            identity,
            ["col_int", "col_varchar"],
            ["col_int", "col_varchar"],
            0.56,
            None,
            None,
            None,
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            ["col_int", "col_varchar"],
            0.55,
            None,
            None,
            None,
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            ["col_int", "col_varchar"],
            0.1,
            None,
            None,
            Condition(raw_string="col_int >= 100"),
        ),
        (
            identity,
            ["col_int", "col_varchar"],
            ["col_int", "col_varchar"],
            0,
            None,
            Condition(raw_string="col_int >= 100"),
            None,
        ),
    ],
)
def test_n_uniques_gain_between(engine, unique_table1, unique_table2, data):
    (
        operation,
        columns1,
        columns2,
        constant_max_relative_gain,
        date_range_gain_deviation,
        condition1,
        condition2,
    ) = data
    req = requirements.BetweenRequirement.from_tables(*unique_table1, *unique_table2)
    req.add_n_uniques_max_gain_constraint(
        columns1,
        columns2,
        constant_max_relative_gain=constant_max_relative_gain,
        date_range_gain_deviation=date_range_gain_deviation,
        condition1=condition1,
        condition2=condition2,
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (
            identity,
            ["col_int", "col_varchar"],
            ["col_int", "col_varchar"],
            0.36,
            None,
            None,
            None,
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            ["col_int", "col_varchar"],
            0.35,
            None,
            None,
            None,
        ),
    ],
)
def test_n_uniques_loss_between(engine, unique_table2, unique_table1, data):
    (
        operation,
        columns2,
        columns1,
        constant_max_relative_loss,
        date_range_loss_deviation,
        condition2,
        condition1,
    ) = data
    req = requirements.BetweenRequirement.from_tables(*unique_table2, *unique_table1)
    req.add_n_uniques_max_loss_constraint(
        columns2,
        columns1,
        constant_max_relative_loss=constant_max_relative_loss,
        date_range_loss_deviation=date_range_loss_deviation,
        condition1=condition2,
        condition2=condition1,
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, 1, None),
        (identity, 3, Condition(raw_string="col_int >= 3")),
        (negation, 2, None),
        (negation, 4, Condition(raw_string="col_int = 3")),
    ],
)
def test_numeric_min_within(engine, int_table1, data):
    (operation, min_value, condition) = data
    req = requirements.WithinRequirement.from_table(*int_table1)
    req.add_numeric_min_constraint("col_int", min_value, condition=condition)
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (negation, None, None),
        (
            identity,
            Condition(raw_string="col_int >= 3"),
            Condition(raw_string="col_int >= 3"),
        ),
    ],
)
def test_numeric_min_between(engine, int_table1, int_table2, data):
    (operation, condition1, condition2) = data
    req = requirements.BetweenRequirement.from_tables(*int_table1, *int_table2)
    req.add_numeric_min_constraint(
        "col_int", "col_int", condition1=condition1, condition2=condition2
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, 19, None),
        (negation, 18, None),
        (identity, 10, Condition(raw_string="col_int <= 10")),
        (negation, 9, Condition(raw_string="col_int <= 10")),
    ],
)
def test_numeric_max_within(engine, int_table1, data):
    (operation, max_value, condition) = data
    req = requirements.WithinRequirement.from_table(*int_table1)
    req.add_numeric_max_constraint("col_int", max_value, condition=condition)
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [(identity, None, None), (negation, None, Condition(raw_string="col_int <= 18"))],
)
def test_numeric_max_between(engine, int_table1, int_table2, data):
    (operation, condition1, condition2) = data
    req = requirements.BetweenRequirement.from_tables(*int_table1, *int_table2)
    req.add_numeric_max_constraint(
        "col_int", "col_int", condition1=condition1, condition2=condition2
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, 5, 15, 0.57, None),
        (negation, 5, 15, 0.58, None),
        (negation, 5, 15, 0.58, Condition(raw_string="col_int IS NOT NULL")),
    ],
)
def test_numeric_between_within(engine, int_table1, data):
    (operation, lower_bound, upper_bound, min_fraction, condition) = data
    req = requirements.WithinRequirement.from_table(*int_table1)
    req.add_numeric_between_constraint(
        "col_int", lower_bound, upper_bound, min_fraction, condition
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize("data", [(identity, 9, 1, None), (negation, 9, 0.9, None)])
def test_numeric_mean_within(engine, int_table1, data):
    (operation, mean_value, max_absolute_deviation, condition) = data
    req = requirements.WithinRequirement.from_table(*int_table1)
    req.add_numeric_mean_constraint(
        "col_int", mean_value, max_absolute_deviation, condition=condition
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, 0.5, None, None),
        (negation, 0.4, None, None),
        (negation, 100, Condition(raw_string="col_int > 100"), None),
    ],
)
def test_numeric_mean_between(engine, int_table1, int_table2, data):
    (operation, max_absolute_deviation, condition1, condition2) = data
    req = requirements.BetweenRequirement.from_tables(*int_table1, *int_table2)
    req.add_numeric_mean_constraint(
        "col_int",
        "col_int",
        max_absolute_deviation,
        condition1=condition1,
        condition2=condition2,
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, "'2016-01-01'", None, True),
        (negation, "'2016-01-02'", None, True),
        (identity, "'2016-01-01'", None, False),
        (negation, "'2015-12-31'", None, False),
    ],
)
def test_date_min_within(engine, date_table1, data):
    (operation, min_value, condition, use_lower_bound_reference) = data
    req = requirements.WithinRequirement.from_table(*date_table1)
    req.add_date_min_constraint(
        "col_date",
        min_value,
        use_lower_bound_reference=use_lower_bound_reference,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (negation, None, None),
        (
            identity,
            Condition(raw_string="col_date >= '2016-01-03'"),
            Condition(raw_string="col_date >= '2016-01-03'"),
        ),
    ],
)
def test_date_min_between(engine, date_table1, date_table2, data):
    (operation, condition1, condition2) = data
    req = requirements.BetweenRequirement.from_tables(*date_table1, *date_table2)
    req.add_date_min_constraint(
        "col_date", "col_date", condition1=condition1, condition2=condition2
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, "'2016-01-19'", None, True),
        (negation, "'2016-01-18'", None, True),
        (identity, "'2016-01-19'", None, False),
        (negation, "'2016-01-20'", None, False),
    ],
)
def test_date_max_within(engine, date_table1, data):
    (operation, max_value, condition, use_upper_bound_reference) = data
    req = requirements.WithinRequirement.from_table(*date_table1)
    req.add_date_max_constraint(
        "col_date",
        max_value,
        use_upper_bound_reference=use_upper_bound_reference,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, None, None),
        (negation, None, Condition(raw_string="col_date <= '2016-01-18'")),
    ],
)
def test_date_max_between(engine, date_table1, date_table2, data):
    (operation, condition1, condition2) = data
    req = requirements.BetweenRequirement.from_tables(*date_table1, *date_table2)
    req.add_date_max_constraint(
        "col_date", "col_date", condition1=condition1, condition2=condition2
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, "'2016-01-05'", "'2016-01-15'", 0.57, None),
        (negation, "'2016-01-05'", "'2016-01-15'", 0.58, None),
    ],
)
def test_date_between_within(engine, date_table1, data):
    (operation, lower_bound, upper_bound, min_fraction, condition) = data
    req = requirements.WithinRequirement.from_table(*date_table1)
    req.add_date_between_constraint(
        "col_date", lower_bound, upper_bound, min_fraction, condition
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, 0, Condition(raw_string="id1 = 1")),
        (identity, 0, Condition(raw_string="id1 = 2")),
        (negation, 0, Condition(raw_string="id1 = 3")),
        (identity, 1, Condition(raw_string="id1 = 3")),
        (negation, 0, Condition(raw_string="id1 = 4")),
        (identity, 1, Condition(raw_string="id1 = 4")),
        (negation, 0, Condition(raw_string="id1 = 5")),
        (identity, 1, Condition(raw_string="id1 = 5")),
    ],
)
@pytest.mark.parametrize("key_columns", [["id1"], [], None])
def test_date_no_overlap_within_varying_key_columns(
    engine, date_table_overlap, data, key_columns
):
    operation, max_relative_n_violations, condition = data
    req = requirements.WithinRequirement.from_table(*date_table_overlap)
    req.add_date_no_overlap_constraint(
        key_columns=key_columns,
        start_column="date_start",
        end_column="date_end",
        max_relative_n_violations=max_relative_n_violations,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (negation, 0.59, None),
        (identity, 0.6, None),
        (identity, 0, Condition(raw_string="id1 IN (1, 2)")),
    ],
)
def test_date_no_overlap_within_fixed_key_column(engine, date_table_overlap, data):
    operation, max_relative_n_violations, condition = data
    req = requirements.WithinRequirement.from_table(*date_table_overlap)
    req.add_date_no_overlap_constraint(
        key_columns=["id1"],
        start_column="date_start",
        end_column="date_end",
        max_relative_n_violations=max_relative_n_violations,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, 0, None),
    ],
)
def test_date_no_overlap_within_several_key_columns(engine, date_table_keys, data):
    operation, max_relative_n_violations, condition = data
    req = requirements.WithinRequirement.from_table(*date_table_keys)
    req.add_date_no_overlap_constraint(
        key_columns=["id1", "id2"],
        start_column="date_start1",
        end_column="date_end1",
        max_relative_n_violations=max_relative_n_violations,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (negation, 0, Condition(raw_string="id1 = 4"), True),
        (identity, 0, Condition(raw_string="id1 = 4"), False),
    ],
)
def test_date_no_overlap_within_inclusion_exclusion(engine, date_table_overlap, data):
    operation, max_relative_n_violations, condition, end_included = data
    req = requirements.WithinRequirement.from_table(*date_table_overlap)
    req.add_date_no_overlap_constraint(
        key_columns=["id1"],
        start_column="date_start",
        end_column="date_end",
        end_included=end_included,
        max_relative_n_violations=max_relative_n_violations,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, 0, Condition(raw_string="id1 = 1")),
        (identity, 0, Condition(raw_string="id1 = 2")),
        (identity, 0, Condition(raw_string="id1 = 3")),
        (identity, 0, Condition(raw_string="id1 = 4")),
        (negation, 0, Condition(raw_string="id1 = 5")),
        (identity, 1, Condition(raw_string="id1 = 5")),
        (negation, 0, Condition(raw_string="id1 = 6")),
        (identity, 1, Condition(raw_string="id1 = 6")),
        (negation, 0, Condition(raw_string="id1 = 7")),
        (identity, 1, Condition(raw_string="id1 = 7")),
    ],
)
@pytest.mark.parametrize("key_columns", [["id1"], [], None])
def test_date_no_overlap_2d_within_varying_key_column(
    engine, date_table_overlap_2d, data, key_columns
):
    operation, max_relative_n_violations, condition = data
    req = requirements.WithinRequirement.from_table(*date_table_overlap_2d)
    req.add_date_no_overlap_2d_constraint(
        key_columns=key_columns,
        start_column1="date_start1",
        end_column1="date_end1",
        start_column2="date_start2",
        end_column2="date_end2",
        max_relative_n_violations=max_relative_n_violations,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, 0, Condition(raw_string="id1 IN (1, 2, 3, 4)")),
        # 3/7 ids have violations.
        (negation, 0.42, None),
        (identity, 0.43, None),
    ],
)
def test_date_no_overlap_2d_within_fixed_key_column(
    engine, date_table_overlap_2d, data
):
    operation, max_relative_n_violations, condition = data
    req = requirements.WithinRequirement.from_table(*date_table_overlap_2d)
    req.add_date_no_overlap_2d_constraint(
        key_columns=["id1"],
        start_column1="date_start1",
        end_column1="date_end1",
        start_column2="date_start2",
        end_column2="date_end2",
        max_relative_n_violations=max_relative_n_violations,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, 0, None),
    ],
)
def test_date_no_overlap_2d_within_several_key_columns(engine, date_table_keys, data):
    operation, max_relative_n_violations, condition = data
    req = requirements.WithinRequirement.from_table(*date_table_keys)
    req.add_date_no_overlap_2d_constraint(
        key_columns=["id1", "id2"],
        start_column1="date_start1",
        end_column1="date_end1",
        start_column2="date_start2",
        end_column2="date_end2",
        max_relative_n_violations=max_relative_n_violations,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        # No overlap is no overlap
        (identity, 0, Condition(raw_string="id1 = 2"), True),
        (identity, 0, Condition(raw_string="id1 = 2"), False),
        # Overlap on the treshold
        (negation, 0, Condition(raw_string="id1 = 6"), True),
        (identity, 0, Condition(raw_string="id1 = 6"), False),
        # Overlap on more than the threshold
        (negation, 0, Condition(raw_string="id1 = 5"), True),
        (negation, 0, Condition(raw_string="id1 = 5"), False),
    ],
)
def test_date_no_overlap_2d_within_inclusion_exclusion(
    engine, date_table_overlap_2d, data
):
    operation, max_relative_n_violations, condition, end_included = data
    req = requirements.WithinRequirement.from_table(*date_table_overlap_2d)
    req.add_date_no_overlap_2d_constraint(
        key_columns=["id1"],
        start_column1="date_start1",
        end_column1="date_end1",
        start_column2="date_start2",
        end_column2="date_end2",
        end_included=end_included,
        max_relative_n_violations=max_relative_n_violations,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, 0, Condition(raw_string="id1 = 1")),
        (identity, 0, Condition(raw_string="id1 = 2")),
        (identity, 0, Condition(raw_string="id1 = 3")),
        (negation, 0, Condition(raw_string="id1 = 4")),
        (identity, 0, None),
        (negation, 0, Condition(raw_string="id1 IN (1, 4)")),
        (negation, 0.49, Condition(raw_string="id1 IN (1, 4)")),
        (identity, 0.5, Condition(raw_string="id1 IN (1, 4)")),
    ],
)
def test_date_no_gap_within_fixed_key_columns(engine, date_table_gap, data):
    operation, max_relative_n_violations, condition = data
    req = requirements.WithinRequirement.from_table(*date_table_gap)
    req.add_date_no_gap_constraint(
        key_columns=["id1"],
        start_column="date_start",
        end_column="date_end",
        max_relative_n_violations=max_relative_n_violations,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, 0, Condition(raw_string="id1 = 1")),
        (identity, 0, Condition(raw_string="id1 = 2")),
        (identity, 0, Condition(raw_string="id1 = 3")),
        (negation, 0, Condition(raw_string="id1 = 4")),
    ],
)
@pytest.mark.parametrize("key_columns", [["id1"], [], None])
def test_date_no_gap_within_varying_key_column(
    engine, date_table_gap, data, key_columns
):
    operation, max_relative_n_violations, condition = data
    req = requirements.WithinRequirement.from_table(*date_table_gap)
    req.add_date_no_gap_constraint(
        key_columns=key_columns,
        start_column="date_start",
        end_column="date_end",
        max_relative_n_violations=max_relative_n_violations,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, 0, None),
    ],
)
def test_date_no_gap_within_several_key_columns(engine, date_table_keys, data):
    operation, max_relative_n_violations, condition = data
    req = requirements.WithinRequirement.from_table(*date_table_keys)
    req.add_date_no_gap_constraint(
        key_columns=["id1", "id2"],
        start_column="date_start1",
        end_column="date_end1",
        max_relative_n_violations=max_relative_n_violations,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, 0, Condition(raw_string="id1 = 5"), True),
        (negation, 0, Condition(raw_string="id1 = 5"), False),
    ],
)
def test_date_no_gap_within_inclusion_exclusion(engine, date_table_gap, data):
    operation, max_relative_n_violations, condition, end_included = data
    req = requirements.WithinRequirement.from_table(*date_table_gap)
    req.add_date_no_gap_constraint(
        key_columns=["id1"],
        start_column="date_start",
        end_column="date_end",
        end_included=end_included,
        max_relative_n_violations=max_relative_n_violations,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, "^hi[0-9]{1,2}$", None),
        (negation, "^hi[0-9]$", None),
    ],
)
def test_varchar_regex_within(engine, mix_table1, data):
    (operation, regex, condition) = data
    req = requirements.WithinRequirement.from_table(*mix_table1)
    req.add_varchar_regex_constraint("col_varchar", regex, condition=condition)
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, "q+", None, True),
        (negation, "q+", None, False),
    ],
)
def test_varchar_regex_with_none_within(engine, varchar_table1, data):
    (operation, regex, condition, allow) = data
    req = requirements.WithinRequirement.from_table(*varchar_table1)
    req.add_varchar_regex_constraint(
        "col_varchar",
        regex,
        condition=condition,
        allow_none=allow,
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, None, True, 0.3),
        (negation, None, True, 0.1),
        (identity, None, False, 0.3),
        (negation, None, False, 0.15),
    ],
)
def test_varchar_regex_tolerance(engine, varchar_table_real, data):
    (operation, condition, aggregated, tolerance) = data
    req = requirements.WithinRequirement.from_table(*varchar_table_real)
    req.add_varchar_regex_constraint(
        "col_varchar",
        r"[A-Z][0-9]{2}\.[0-9]{0,2}$",
        condition=condition,
        relative_tolerance=tolerance,
        aggregated=aggregated,
    )
    assert operation(req[0].test(engine).outcome)


def test_backend_dependent_condition(engine, mix_table1):
    if is_mssql(engine):
        condition = Condition(raw_string="DATALENGTH(col_varchar) = 3")
    elif is_postgresql(engine) or is_snowflake(engine):
        condition = Condition(raw_string="LENGTH(col_varchar) = 3")
    else:
        raise NotImplementedError(f"Unexpected backend: {engine.name}")
    regex = "^hi[0-9]$"
    req = requirements.WithinRequirement.from_table(*mix_table1)
    req.add_varchar_regex_constraint("col_varchar", regex, condition=condition)
    assert identity(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, 2, Condition(raw_string="col_varchar IS NOT NULL")),
        (negation, 3, Condition(raw_string="col_varchar IS NOT NULL")),
        (identity, 2, None),
        (negation, 3, None),
        (identity, 0, Condition(raw_string="col_varchar LIKE 'hi%'")),
    ],
)
def test_varchar_min_length_within(engine, varchar_table1, data):
    (operation, min_length, condition) = data
    req = requirements.WithinRequirement.from_table(*varchar_table1)
    req.add_varchar_min_length_constraint(
        "col_varchar", min_length, condition=condition
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (negation, None, None),
        (negation, Condition(raw_string="col_varchar LIKE 'hi%'"), None),
        (
            identity,
            Condition(raw_string="col_varchar LIKE 'hi%'"),
            Condition(raw_string="col_varchar LIKE 'hi%'"),
        ),
        (identity, None, Condition(raw_string="col_varchar LIKE 'hi%'")),
    ],
)
def test_varchar_min_length_between(engine, varchar_table1, varchar_table2, data):
    (operation, condition1, condition2) = data
    req = requirements.BetweenRequirement.from_tables(*varchar_table1, *varchar_table2)
    req.add_varchar_min_length_constraint(
        "col_varchar",
        "col_varchar",
        condition1=condition1,
        condition2=condition2,
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, 18, None),
        (negation, 17, None),
        (identity, 0, Condition(raw_string="col_varchar LIKE 'hi%'")),
    ],
)
def test_varchar_max_length_within(engine, varchar_table1, data):
    (operation, max_length, condition) = data
    req = requirements.WithinRequirement.from_table(*varchar_table1)
    req.add_varchar_max_length_constraint(
        "col_varchar", max_length, condition=condition
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, None, None),
        (identity, Condition(raw_string="col_varchar LIKE 'hi%'"), None),
        (
            identity,
            Condition(raw_string="col_varchar LIKE 'hi%'"),
            Condition(raw_string="col_varchar LIKE 'hi%'"),
        ),
        (negation, None, Condition(raw_string="col_varchar LIKE 'hi%'")),
    ],
)
def test_varchar_max_length_between(engine, varchar_table1, varchar_table2, data):
    (operation, condition1, condition2) = data
    req = requirements.BetweenRequirement.from_tables(*varchar_table1, *varchar_table2)
    req.add_varchar_max_length_constraint(
        "col_varchar",
        "col_varchar",
        condition1=condition1,
        condition2=condition2,
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, []),
        (identity, ["col_varchar"]),
        (identity, ["col_int", "col_varchar"]),
        (negation, ["col_nope"]),
        (negation, ["col_int", "col_nope"]),
    ],
)
def test_column_existence_within(engine, mix_table1, data):
    (operation, columns) = data
    req = requirements.WithinRequirement.from_table(*mix_table1)
    req.add_column_existence_constraint(columns)
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, "varchar_table1", "mix_table1"),
        (identity, "mix_table1", "mix_table1"),
        (negation, "mix_table1", "varchar_table1"),
    ],
)
def test_column_subset_between(engine, get_fixture, data):
    (operation, table_name1, table_name2) = data
    table1 = get_fixture(table_name1)
    table2 = get_fixture(table_name2)
    req = requirements.BetweenRequirement.from_tables(*table1, *table2)
    req.add_column_subset_constraint()
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, "mix_table1", "varchar_table1"),
        (identity, "mix_table1", "mix_table2"),
    ],
)
def test_column_superset_between(engine, get_fixture, data):
    (operation, table_name1, table_name2) = data
    table1 = get_fixture(table_name1)
    table2 = get_fixture(table_name2)
    req = requirements.BetweenRequirement.from_tables(*table1, *table2)
    req.add_column_superset_constraint()
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, ["col_int1", "col_int2"]),
        (identity, ["col_int2", "col_int1"]),
        (negation, ["col_int1"]),
        (negation, ["col_int1", "col_int2", "col_nope"]),
    ],
)
def test_primary_key_definition_within(engine, pk_table, data):
    (operation, columns) = data
    req = requirements.WithinRequirement.from_table(*pk_table)
    req.add_primary_key_definition_constraint(columns)
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, ["col_int"], 0, None, 0),
        (identity, ["col_int", "col_varchar"], 0, None, 0),
        (identity, ["col_int", "col_varchar", "col_date"], 0, None, 0),
        (negation, ["col_date"], 0.49, None, 0),
        (identity, ["col_date"], 0.5, None, 0),
        (identity, ["col_date"], 0, Condition(raw_string="col_int = 3"), 0),
        (identity, ["col_date"], 0, Condition(raw_string="col_int % 2 = 0"), 0),
        (negation, ["col_date"], 0, None, 8),
        (identity, ["col_date"], 0, None, 9),
    ],
)
def test_uniqueness_within(engine, mix_table2, data):
    (
        operation,
        columns,
        max_duplicate_fraction,
        condition,
        max_absolute_n_duplicates,
    ) = data
    # For an unknown reason, the condition `Condition(raw_string="col_int % 2 = 0")`
    # is not correctly compiled when dealing with snowflake.
    if (
        is_snowflake(engine)
        and condition is not None
        and condition.raw_string is not None
        and "% 2 = 0" in condition.raw_string
    ):
        condition = Condition(raw_string="mod(col_int, 2) = 0")
    req = requirements.WithinRequirement.from_table(*mix_table2)
    req.add_uniqueness_constraint(
        columns, max_duplicate_fraction, condition, max_absolute_n_duplicates
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [(identity, "int_table1", "col_int"), (negation, "unique_table1", "col_varchar")],
)
def test_null_absence_within(engine, get_fixture, data):
    (operation, table_name, col_name) = data
    table = get_fixture(table_name)
    req = requirements.WithinRequirement.from_table(*table)
    req.add_null_absence_constraint(col_name)
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, "col_varchar", "VARCHAR"),
        (identity, "col_int", "INTEGER"),
        (negation, "col_varchar", "INTEGER"),
    ],
)
def test_column_type_within(engine, mix_table1, data):
    (operation, col_name, type_name) = data
    req = requirements.WithinRequirement.from_table(*mix_table1)
    req.add_column_type_constraint(col_name, type_name)
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, ["col_int", "col_varchar"], 0.15, None, None),
        (negation, ["col_int", "col_varchar"], 0.14, None, None),
        (
            identity,
            ["col_int", "col_varchar"],
            0.059,
            Condition(raw_string="col_int >= 3"),
            Condition(
                conditions=[
                    Condition(raw_string="col_int >= 3"),
                    Condition(raw_string="col_varchar LIKE 'hi%'"),
                ],
                reduction_operator="and",
            ),
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            0.058,
            Condition(raw_string="col_int >= 3"),
            Condition(
                conditions=[
                    Condition(raw_string="col_int >= 3"),
                    Condition(raw_string="col_varchar LIKE 'hi%'"),
                ],
                reduction_operator="and",
            ),
        ),
    ],
)
def test_row_equality_between(engine, mix_table1, mix_table2, data):
    (operation, columns, max_missing_fraction, condition1, condition2) = data
    req = requirements.BetweenRequirement.from_tables(*mix_table1, *mix_table2)
    req.add_row_equality_constraint(
        columns, columns, max_missing_fraction, condition1, condition2
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, ["col_int", "col_varchar"], 0.11, None, None, None),
        (negation, ["col_int", "col_varchar"], 0.10, None, None, None),
        (
            identity,
            ["col_int"],
            0,
            None,
            Condition(raw_string="col_int = 100"),
            None,
        ),
        (
            identity,
            ["col_int"],
            1,
            None,
            None,
            Condition(raw_string="col_int = 100"),
        ),
        (
            negation,
            ["col_int"],
            0.9,
            None,
            None,
            Condition(raw_string="col_int = 100"),
        ),
        (negation, None, 0.9, None, None, None),
    ],
)
def test_row_subset_between(engine, mix_table1, mix_table2, data):
    (
        operation,
        columns,
        constant_max_missing_fraction,
        date_range_loss_fraction,
        condition1,
        condition2,
    ) = data
    req = requirements.BetweenRequirement.from_tables(*mix_table1, *mix_table2)
    req.add_row_subset_constraint(
        columns,
        columns,
        constant_max_missing_fraction,
        date_range_loss_fraction,
        condition1,
        condition2,
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, ["col_int", "col_varchar"], 0.11, None, None, None),
        (negation, ["col_int", "col_varchar"], 0.10, None, None, None),
        (
            identity,
            ["col_int"],
            0,
            None,
            None,
            Condition(raw_string="col_int = 100"),
        ),
        (
            identity,
            ["col_int"],
            1,
            None,
            Condition(raw_string="col_int = 100"),
            None,
        ),
        (
            negation,
            ["col_int"],
            0.9,
            None,
            Condition(raw_string="col_int = 100"),
            None,
        ),
    ],
)
def test_row_superset_between(engine, mix_table2, mix_table1, data):
    (
        operation,
        columns,
        constant_max_missing_fraction,
        date_range_loss_fraction,
        condition1,
        condition2,
    ) = data
    req = requirements.BetweenRequirement.from_tables(*mix_table2, *mix_table1)
    req.add_row_superset_constraint(
        columns,
        columns,
        constant_max_missing_fraction,
        date_range_loss_fraction,
        condition1,
        condition2,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (
            identity,
            ["col_match1", "col_match2"],
            ["col_compare1", "col_compare2"],
            0.43,
            None,
            None,
        ),
        (
            negation,
            ["col_match1", "col_match2"],
            ["col_compare1", "col_compare2"],
            0.41,
            None,
            None,
        ),
    ],
)
def test_row_matching_equality(engine, row_match_table1, row_match_table2, data):
    (
        operation,
        matching_columns,
        comparison_columns,
        max_missing_fraction,
        condition1,
        condition2,
    ) = data
    req = requirements.BetweenRequirement.from_tables(
        *row_match_table1, *row_match_table2
    )
    req.add_row_matching_equality_constraint(
        matching_columns,
        matching_columns,
        comparison_columns,
        comparison_columns,
        max_missing_fraction,
        condition1=condition1,
        condition2=condition2,
    )
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize("key", [("some_id",), ("some_id", "extra_id")])
def test_groupby_aggregation_within(engine, groupby_aggregation_table_correct, key):
    skip_if_mssql(engine)
    req = requirements.WithinRequirement.from_table(*groupby_aggregation_table_correct)
    req.add_groupby_aggregation_constraint(key, "value", 1)
    test_result = req[0].test(engine)
    assert identity(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize("key", [("some_id",), ("some_id", "extra_id")])
@pytest.mark.parametrize("tolerance, operation", [(0, negation), (0.5, identity)])
def test_groupby_aggregation_within_with_failures(
    engine, groupby_aggregation_table_incorrect, tolerance, operation, key
):
    skip_if_mssql(engine)
    req = requirements.WithinRequirement.from_table(
        *groupby_aggregation_table_incorrect
    )
    req.add_groupby_aggregation_constraint(key, "value", 1, tolerance)
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


def test_diff_average_between():
    return
