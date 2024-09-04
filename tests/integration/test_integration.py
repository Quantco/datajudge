from __future__ import annotations

import functools
import textwrap

import pytest
import sqlalchemy as sa
from sqlalchemy import event

import datajudge.requirements as requirements
from datajudge.db_access import (
    Condition,
    is_bigquery,
    is_db2,
    is_impala,
    is_mssql,
    is_postgresql,
    is_snowflake,
)
from datajudge.utils import (
    filternull_element,
    filternull_element_or_tuple_all,
    filternull_element_or_tuple_any,
    filternull_never,
    output_processor_limit,
    output_processor_sort,
)


def skip_if_mssql(engine):
    if is_mssql(engine):
        pytest.skip("functionality not supported by SQL Server")


def identity(boolean_value):
    return boolean_value


def negation(boolean_value):
    return not boolean_value


class QueryCollector:
    def __init__(self):
        self.queries = []

    def __call__(self, conn, cursor, statement, parameters, context, executemany):
        self.queries.append(statement)

    def __enter__(self) -> QueryCollector:
        event.listen(sa.engine.Engine, "before_cursor_execute", self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        event.remove(sa.engine.Engine, "before_cursor_execute", self)

    def __len__(self):
        return len(self.queries)


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
    req = requirements.BetweenRequirement.from_tables(  # type: ignore[misc]
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
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


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
        (
            negation,
            ["col_int", "col_varchar"],
            [(0, "hi0"), (1, "hi0")],
            filternull_element_or_tuple_any,
            None,
            output_processor_sort,
            None,
            "column(s) 'col_int', 'col_varchar' has the excess element(s) '[(1, 'hi1'), (2, 'hi1'), (3, 'hi2'), (4, 'hi2'), (4, 'hi3'), (5, 'hi3'), (6, 'hi4'), (7, 'hi4'), (7, 'hi5'), (8, 'hi5'), (9, 'hi6'), (10, 'hi6'), (10, 'hi7'), (11, 'hi7'), (12, 'hi8'), (13, 'hi8'), (13, 'hi9'), (14, 'hi9'), (15, 'hi10'), (16, 'hi10'), (16, 'hi11'), (17, 'hi11'), (18, 'hi12'), (19, 'hi12'), (19, 'hi13'), (20, 'hi13'), (21, 'hi14'), (22, 'hi14'), (22, 'hi15'), (23, 'hi15'), (24, 'hi16'), (25, 'hi16'), (25, 'hi17'), (26, 'hi17'), (27, 'hi18'), (28, 'hi18'), (28, 'hi19'), (29, 'hi19')]' when compared with the reference values. ",
        ),
    ],
)
def test_uniques_equality_within_with_outputcheck(engine, unique_table1, data):
    (
        operation,
        columns,
        uniques,
        filter_func,
        map_func,
        output_processors,
        condition,
        failure_message_suffix,
    ) = data
    req = requirements.WithinRequirement.from_table(*unique_table1)
    req.add_uniques_equality_constraint(
        columns,
        uniques,
        condition=condition,
        filter_func=filter_func,
        map_func=map_func,
        output_processors=output_processors,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message
    assert test_result.failure_message.endswith(
        failure_message_suffix
    ), test_result.failure_message


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
        (
            negation,
            ["col_int", "col_varchar"],
            ["col_int", "col_varchar"],
            filternull_element_or_tuple_all,
            None,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has the excess element(s) '[(None, 'hi'), (20, 'hi13'), (21, 'hi14'), (22, 'hi14'), (22, 'hi15'), (23, 'hi15'), (24, 'hi16'), (25, 'hi16'), (25, 'hi17'), (26, 'hi17'), (27, 'hi18'), (28, 'hi18'), (28, 'hi19'), (29, 'hi19')]' when compared with the reference values. ",
        ),
    ],
)
def test_uniques_equality_between_with_outputcheck(
    engine, unique_table1, unique_table2, data
):
    (
        operation,
        columns1,
        columns2,
        filter_func,
        map_func,
        output_processors,
        condition1,
        condition2,
        failure_message_suffix,
    ) = data
    req = requirements.BetweenRequirement.from_tables(*unique_table1, *unique_table2)
    req.add_uniques_equality_constraint(
        columns1,
        columns2,
        filter_func=filter_func,
        map_func=map_func,
        output_processors=output_processors,
        condition1=condition1,
        condition2=condition2,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message
    assert test_result.failure_message.endswith(
        failure_message_suffix
    ), test_result.failure_message


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
        (
            negation,
            ["col_int", "col_varchar"],
            [(1337, "hi0"), (None, "hi"), (None, None)],
            0,
            filternull_never,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.3333333333333333 > 0 (1 / 3) lacking unique values of '[(None, None), (None, 'hi'), (1337, 'hi0')]'. It doesn't have the unique value(s) '[(1337, 'hi0')]'.",
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            [(1337, "hi0"), (None, "hi"), (None, None)],
            0,
            None,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.3333333333333333 > 0 (1 / 3) lacking unique values of '[(None, None), (None, 'hi'), (1337, 'hi0')]'. It doesn't have the unique value(s) '[(1337, 'hi0')]'.",
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            [(1337, "hi0"), (None, "hi"), (None, None)],
            0,
            filternull_element_or_tuple_all,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.6666666666666666 > 0 (2 / 3) lacking unique values of '[(None, None), (None, 'hi'), (1337, 'hi0')]'. It doesn't have the unique value(s) '[(None, None), (1337, 'hi0')]'.",
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            [(1337, "hi0"), (None, "hi"), (None, None)],
            0,
            filternull_element_or_tuple_any,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 1.0 > 0 (3 / 3) lacking unique values of '[(None, None), (None, 'hi'), (1337, 'hi0')]'. It doesn't have the unique value(s) '[(None, None), (None, 'hi'), (1337, 'hi0')]'.",
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            [
                (0, "hi0"),
                (1234, "hi"),
                (1234, "hi2"),
                (1234, "hi3"),
                (1234, "hi4"),
                (1234, "hi5"),
                (9999, "hi4"),
            ],
            0,
            filternull_element_or_tuple_any,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.8571428571428571 > 0 (6 / 7) lacking unique values of '[(0, 'hi0'), (1234, 'hi'), (1234, 'hi2'), (1234, 'hi3'), (1234, 'hi4'), (1234, 'hi5'), (9999, 'hi4')]'. It doesn't have the unique value(s) '[(1234, 'hi'), (1234, 'hi2'), (1234, 'hi3'), (1234, 'hi4'), (1234, 'hi5'), (9999, 'hi4')]'.",
        ),
    ],
)
def test_uniques_superset_within_with_outputcheck(engine, unique_table1, data):
    (
        operation,
        columns,
        uniques,
        max_relative_violations,
        filter_func,
        output_processors,
        function,
        condition,
        failure_message_suffix,
    ) = data
    req = requirements.WithinRequirement.from_table(*unique_table1)
    req.add_uniques_superset_constraint(
        columns,
        uniques=uniques,
        max_relative_violations=max_relative_violations,
        filter_func=filter_func,
        condition=condition,
        map_func=function,
        output_processors=output_processors,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message
    assert test_result.failure_message.endswith(
        failure_message_suffix
    ), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (
            negation,
            ["col_int", "col_varchar"],
            ["col_int", "col_varchar"],
            0,
            filternull_element_or_tuple_any,
            [output_processor_sort],
            None,
            Condition(raw_string="col_int < 19"),
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.375 > 0 (15 / 40) lacking unique values of '[(0, 'hi0'), (1, 'hi0'), (1, 'hi1'), (2, 'hi1'), (3, 'hi2'), (4, 'hi2'), (4, 'hi3'), (5, 'hi3'), (6, 'hi4'), (7, 'hi4'), (7, 'hi5'), (8, 'hi5'), (9, 'hi6'), (10, 'hi6'), (10, 'hi7'), (11, 'hi7'), (12, 'hi8'), (13, 'hi8'), (13, 'hi9'), (14, 'hi9'), (15, 'hi10'), (16, 'hi10'), (16, 'hi11'), (17, 'hi11'), (18, 'hi12'), (19, 'hi12'), (19, 'hi13'), (20, 'hi13'), (21, 'hi14'), (22, 'hi14'), (22, 'hi15'), (23, 'hi15'), (24, 'hi16'), (25, 'hi16'), (25, 'hi17'), (26, 'hi17'), (27, 'hi18'), (28, 'hi18'), (28, 'hi19'), (29, 'hi19')]'. It doesn't have the unique value(s) '[(19, 'hi12'), (19, 'hi13'), (20, 'hi13'), (21, 'hi14'), (22, 'hi14'), (22, 'hi15'), (23, 'hi15'), (24, 'hi16'), (25, 'hi16'), (25, 'hi17'), (26, 'hi17'), (27, 'hi18'), (28, 'hi18'), (28, 'hi19'), (29, 'hi19')]'.Condition on first table: WHERE col_int < 19; ",
        ),
    ],
)
def test_uniques_superset_between_with_outputcheck(
    engine, unique_table1, unique_table2, data
):
    (
        operation,
        columns1,
        columns2,
        max_relative_violations,
        filter_func,
        output_processors,
        map_func,
        condition1,
        condition2,
        failure_message_suffix,
    ) = data
    req = requirements.BetweenRequirement.from_tables(*unique_table2, *unique_table1)
    req.add_uniques_superset_constraint(
        columns1,
        columns2,
        max_relative_violations=max_relative_violations,
        filter_func=filter_func,
        output_processors=output_processors,
        map_func=map_func,
        condition1=condition1,
        condition2=condition2,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message
    assert test_result.failure_message.endswith(
        failure_message_suffix
    ), test_result.failure_message


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
        (
            negation,
            ["col_int", "col_varchar"],
            [(0, "hi0"), (1, "hi0")],
            0,
            None,
            False,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.9516129032258065 > 0 values (59 / 62) not being an element of '[(0, 'hi0'), (1, 'hi0')]'. It has excess elements '[(2, 'hi1'), (3, 'hi2'), (5, 'hi3'), (6, 'hi4'), (8, 'hi5'), (9, 'hi6'), (11, 'hi7'), (12, 'hi8'), (14, 'hi9'), (15, 'hi10'), (17, 'hi11'), (18, 'hi12'), (20, 'hi13'), (21, 'hi14'), (23, 'hi15'), (24, 'hi16'), (26, 'hi17'), (27, 'hi18'), (29, 'hi19'), (None, None), (None, 'hi'), (1, 'hi1'), (4, 'hi2'), (4, 'hi3'), (7, 'hi4'), (7, 'hi5'), (10, 'hi6'), (10, 'hi7'), (13, 'hi8'), (13, 'hi9'), (16, 'hi10'), (16, 'hi11'), (19, 'hi12'), (19, 'hi13'), (22, 'hi14'), (22, 'hi15'), (25, 'hi16'), (25, 'hi17'), (28, 'hi18'), (28, 'hi19')]' with counts [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1].",
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            [(0, "hi0"), (1, "hi0")],
            0,
            None,
            True,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.9523809523809523 > 0 DISTINCT values (40 / 42) not being an element of '[(0, 'hi0'), (1, 'hi0')]'. It has excess elements '[(2, 'hi1'), (3, 'hi2'), (5, 'hi3'), (6, 'hi4'), (8, 'hi5'), (9, 'hi6'), (11, 'hi7'), (12, 'hi8'), (14, 'hi9'), (15, 'hi10'), (17, 'hi11'), (18, 'hi12'), (20, 'hi13'), (21, 'hi14'), (23, 'hi15'), (24, 'hi16'), (26, 'hi17'), (27, 'hi18'), (29, 'hi19'), (None, None), (None, 'hi'), (1, 'hi1'), (4, 'hi2'), (4, 'hi3'), (7, 'hi4'), (7, 'hi5'), (10, 'hi6'), (10, 'hi7'), (13, 'hi8'), (13, 'hi9'), (16, 'hi10'), (16, 'hi11'), (19, 'hi12'), (19, 'hi13'), (22, 'hi14'), (22, 'hi15'), (25, 'hi16'), (25, 'hi17'), (28, 'hi18'), (28, 'hi19')]' with counts [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1].",
        ),
        (
            negation,
            ["col_int"],
            [i for i in range(30) if i != 3],
            0,
            None,
            False,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int' has a fraction of 0.03333333333333333 > 0 values (2 / 60) not being an element of '[0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]'. It has excess elements '[3]' with counts [2].",
        ),
        (
            negation,
            ["col_int"],
            [i for i in range(30) if i != 3],
            0,
            None,
            False,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int' has a fraction of 0.03333333333333333 > 0 values (2 / 60) not being an element of '[0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]'. It has excess elements '[3]' with counts [2].",
        ),
        (
            negation,
            ["col_int"],
            [i for i in range(30) if i != 3],
            0,
            None,
            True,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int' has a fraction of 0.03333333333333333 > 0 DISTINCT values (1 / 30) not being an element of '[0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]'. It has excess elements '[3]' with counts [2].",
        ),
        (
            negation,
            ["col_int"],
            [i for i in range(30) if i != 3],
            0,
            filternull_element,
            False,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int' has a fraction of 0.03333333333333333 > 0 values (2 / 60) not being an element of '[0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]'. It has excess elements '[3]' with counts [2].",
        ),
        (
            negation,
            ["col_int"],
            [i for i in range(30) if i != 3],
            0,
            filternull_element_or_tuple_all,
            False,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int' has a fraction of 0.03333333333333333 > 0 values (2 / 60) not being an element of '[0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]'. It has excess elements '[3]' with counts [2].",
        ),
        (
            negation,
            ["col_int"],
            [i for i in range(30) if i != 3],
            0,
            filternull_element_or_tuple_any,
            False,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int' has a fraction of 0.03333333333333333 > 0 values (2 / 60) not being an element of '[0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]'. It has excess elements '[3]' with counts [2].",
        ),
        (
            negation,
            ["col_int"],
            [i for i in range(30) if i != 3],
            0,
            filternull_never,
            False,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int' has a fraction of 0.06451612903225806 > 0 values (4 / 62) not being an element of '[0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]'. It has excess elements '[None, 3]' with counts [2, 2].",
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            [(0, "hi0"), (1, "hi0")],
            0,
            filternull_element,
            True,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.9523809523809523 > 0 DISTINCT values (40 / 42) not being an element of '[(0, 'hi0'), (1, 'hi0')]'. It has excess elements '[(2, 'hi1'), (3, 'hi2'), (5, 'hi3'), (6, 'hi4'), (8, 'hi5'), (9, 'hi6'), (11, 'hi7'), (12, 'hi8'), (14, 'hi9'), (15, 'hi10'), (17, 'hi11'), (18, 'hi12'), (20, 'hi13'), (21, 'hi14'), (23, 'hi15'), (24, 'hi16'), (26, 'hi17'), (27, 'hi18'), (29, 'hi19'), (None, None), (None, 'hi'), (1, 'hi1'), (4, 'hi2'), (4, 'hi3'), (7, 'hi4'), (7, 'hi5'), (10, 'hi6'), (10, 'hi7'), (13, 'hi8'), (13, 'hi9'), (16, 'hi10'), (16, 'hi11'), (19, 'hi12'), (19, 'hi13'), (22, 'hi14'), (22, 'hi15'), (25, 'hi16'), (25, 'hi17'), (28, 'hi18'), (28, 'hi19')]' with counts [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1].",
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            [(0, "hi0"), (1, "hi0")],
            0,
            filternull_never,
            True,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.9523809523809523 > 0 DISTINCT values (40 / 42) not being an element of '[(0, 'hi0'), (1, 'hi0')]'. It has excess elements '[(2, 'hi1'), (3, 'hi2'), (5, 'hi3'), (6, 'hi4'), (8, 'hi5'), (9, 'hi6'), (11, 'hi7'), (12, 'hi8'), (14, 'hi9'), (15, 'hi10'), (17, 'hi11'), (18, 'hi12'), (20, 'hi13'), (21, 'hi14'), (23, 'hi15'), (24, 'hi16'), (26, 'hi17'), (27, 'hi18'), (29, 'hi19'), (None, None), (None, 'hi'), (1, 'hi1'), (4, 'hi2'), (4, 'hi3'), (7, 'hi4'), (7, 'hi5'), (10, 'hi6'), (10, 'hi7'), (13, 'hi8'), (13, 'hi9'), (16, 'hi10'), (16, 'hi11'), (19, 'hi12'), (19, 'hi13'), (22, 'hi14'), (22, 'hi15'), (25, 'hi16'), (25, 'hi17'), (28, 'hi18'), (28, 'hi19')]' with counts [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1].",
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            [(0, "hi0"), (1, "hi0")],
            0,
            filternull_element_or_tuple_all,
            True,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.9512195121951219 > 0 DISTINCT values (39 / 41) not being an element of '[(0, 'hi0'), (1, 'hi0')]'. It has excess elements '[(2, 'hi1'), (3, 'hi2'), (5, 'hi3'), (6, 'hi4'), (8, 'hi5'), (9, 'hi6'), (11, 'hi7'), (12, 'hi8'), (14, 'hi9'), (15, 'hi10'), (17, 'hi11'), (18, 'hi12'), (20, 'hi13'), (21, 'hi14'), (23, 'hi15'), (24, 'hi16'), (26, 'hi17'), (27, 'hi18'), (29, 'hi19'), (None, 'hi'), (1, 'hi1'), (4, 'hi2'), (4, 'hi3'), (7, 'hi4'), (7, 'hi5'), (10, 'hi6'), (10, 'hi7'), (13, 'hi8'), (13, 'hi9'), (16, 'hi10'), (16, 'hi11'), (19, 'hi12'), (19, 'hi13'), (22, 'hi14'), (22, 'hi15'), (25, 'hi16'), (25, 'hi17'), (28, 'hi18'), (28, 'hi19')]' with counts [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1].",
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            [(0, "hi0"), (1, "hi0")],
            0,
            filternull_element_or_tuple_any,
            True,
            [output_processor_sort],
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.95 > 0 DISTINCT values (38 / 40) not being an element of '[(0, 'hi0'), (1, 'hi0')]'. It has excess elements '[(2, 'hi1'), (3, 'hi2'), (5, 'hi3'), (6, 'hi4'), (8, 'hi5'), (9, 'hi6'), (11, 'hi7'), (12, 'hi8'), (14, 'hi9'), (15, 'hi10'), (17, 'hi11'), (18, 'hi12'), (20, 'hi13'), (21, 'hi14'), (23, 'hi15'), (24, 'hi16'), (26, 'hi17'), (27, 'hi18'), (29, 'hi19'), (1, 'hi1'), (4, 'hi2'), (4, 'hi3'), (7, 'hi4'), (7, 'hi5'), (10, 'hi6'), (10, 'hi7'), (13, 'hi8'), (13, 'hi9'), (16, 'hi10'), (16, 'hi11'), (19, 'hi12'), (19, 'hi13'), (22, 'hi14'), (22, 'hi15'), (25, 'hi16'), (25, 'hi17'), (28, 'hi18'), (28, 'hi19')]' with counts [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1].",
        ),
    ],
)
def test_uniques_subset_within_complex_with_outputcheck(engine, unique_table1, data):
    (
        operation,
        columns,
        uniques,
        max_relative_violations,
        filter_func,
        compare_distinct,
        output_processors,
        function,
        condition,
        failure_message_suffix,
    ) = data
    req = requirements.WithinRequirement.from_table(*unique_table1)
    req.add_uniques_subset_constraint(
        columns,
        uniques,
        max_relative_violations,
        filter_func=filter_func,
        compare_distinct=compare_distinct,
        output_processors=output_processors,
        condition=condition,
        map_func=function,
    )

    test_result = req[0].test(engine)
    print(test_result)
    print(test_result.failure_message)
    assert operation(test_result.outcome), test_result.failure_message
    assert test_result.failure_message.endswith(
        failure_message_suffix
    ), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (
            negation,
            ["col_int", "col_varchar"],
            [(0, "hi0"), (1, "hi0")],
            0,
            filternull_element_or_tuple_any,
            True,
            [output_processor_sort, output_processor_limit],
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.9997569866342649 > 0 DISTINCT values (8228 / 8230) not being an element of '[(0, 'hi0'), (1, 'hi0')]'. It has excess elements '[(2, 'hi1'), (3, 'hi2'), (5, 'hi3'), (6, 'hi4'), (8, 'hi5'), (9, 'hi6'), (11, 'hi7'), (12, 'hi8'), (14, 'hi9'), (15, 'hi10'), (17, 'hi11'), (18, 'hi12'), (20, 'hi13'), (21, 'hi14'), (23, 'hi15'), (24, 'hi16'), (26, 'hi17'), (27, 'hi18'), (29, 'hi19'), (30, 'hi20'), (32, 'hi21'), (33, 'hi22'), (35, 'hi23'), (36, 'hi24'), (38, 'hi25'), (39, 'hi26'), (41, 'hi27'), (42, 'hi28'), (44, 'hi29'), (45, 'hi30'), (47, 'hi31'), (48, 'hi32'), (50, 'hi33'), (51, 'hi34'), (53, 'hi35'), (54, 'hi36'), (56, 'hi37'), (57, 'hi38'), (59, 'hi39'), (60, 'hi40'), (62, 'hi41'), (63, 'hi42'), (65, 'hi43'), (66, 'hi44'), (68, 'hi45'), (69, 'hi46'), (71, 'hi47'), (72, 'hi48'), (74, 'hi49'), (75, 'hi50'), (77, 'hi51'), (78, 'hi52'), (80, 'hi53'), (81, 'hi54'), (83, 'hi55'), (84, 'hi56'), (86, 'hi57'), (87, 'hi58'), (89, 'hi59'), (90, 'hi60'), (92, 'hi61'), (93, 'hi62'), (95, 'hi63'), (96, 'hi64'), (98, 'hi65'), (99, 'hi66'), (101, 'hi67'), (102, 'hi68'), (104, 'hi69'), (105, 'hi70'), (107, 'hi71'), (108, 'hi72'), (110, 'hi73'), (111, 'hi74'), (113, 'hi75'), (114, 'hi76'), (116, 'hi77'), (117, 'hi78'), (119, 'hi79'), (120, 'hi80'), (122, 'hi81'), (123, 'hi82'), (125, 'hi83'), (126, 'hi84'), (128, 'hi85'), (129, 'hi86'), (131, 'hi87'), (132, 'hi88'), (134, 'hi89'), (135, 'hi90'), (137, 'hi91'), (138, 'hi92'), (140, 'hi93'), (141, 'hi94'), (143, 'hi95'), (144, 'hi96'), (146, 'hi97'), (147, 'hi98'), (149, 'hi99'), (150, 'hi100'), '<SHORTENED OUTPUT, displaying the first 100 / 8228 elements above>']' with counts [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, '<SHORTENED OUTPUT, displaying the first 100 / 8228 counts above>'].",
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            [(0, "hi0"), (1, "hi0")],
            0,
            filternull_element_or_tuple_any,
            True,
            [output_processor_sort, functools.partial(output_processor_limit, limit=5)],
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.9997569866342649 > 0 DISTINCT values (8228 / 8230) not being an element of '[(0, 'hi0'), (1, 'hi0')]'. It has excess elements '[(2, 'hi1'), (3, 'hi2'), (5, 'hi3'), (6, 'hi4'), (8, 'hi5'), '<SHORTENED OUTPUT, displaying the first 5 / 8228 elements above>']' with counts [2, 2, 2, 2, 2, '<SHORTENED OUTPUT, displaying the first 5 / 8228 counts above>'].",
        ),
    ],
)
def test_uniques_subset_within_complex_with_outputcheck_extralong(
    engine, unique_table_extralong, data
):
    (
        operation,
        columns,
        uniques,
        max_relative_violations,
        filter_func,
        compare_distinct,
        output_processors,
        function,
        condition,
        failure_message_suffix,
    ) = data
    req = requirements.WithinRequirement.from_table(*unique_table_extralong)
    req.add_uniques_subset_constraint(
        columns,
        uniques,
        max_relative_violations,
        filter_func=filter_func,
        compare_distinct=compare_distinct,
        output_processors=output_processors,
        condition=condition,
        map_func=function,
    )

    test_result = req[0].test(engine)
    print(test_result)
    print(test_result.failure_message)
    assert operation(test_result.outcome), test_result.failure_message
    assert test_result.failure_message.endswith(
        failure_message_suffix
    ), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (
            negation,
            ["col_int", "col_varchar"],
            [(0, "hi0"), (1, "hi0")],
            0,
            filternull_element_or_tuple_any,
            False,
            [output_processor_sort, functools.partial(output_processor_limit, limit=5)],
            None,
            None,
            None,
        ),
    ],
)
@pytest.mark.limit_memory(limit="60 MB")
def test_memory_no_caching(engine, unique_table_largesize, data):
    (
        operation,
        columns,
        uniques,
        max_relative_violations,
        filter_func,
        compare_distinct,
        output_processors,
        function,
        condition,
        failure_message_suffix,
    ) = data

    req = requirements.WithinRequirement.from_table(*unique_table_largesize)
    for i in range(20):
        # typical use-case of datajudge: many (possibly memory-intensive) constraints
        # are added, and subsequently tested
        # if the user specifies a cache_size of 0,
        # this should work even if keeping the intermediate results of all checks in memory is infeasible
        req.add_uniques_subset_constraint(
            columns,
            uniques,
            max_relative_violations,
            filter_func=filter_func,
            compare_distinct=compare_distinct,
            output_processors=output_processors,
            condition=condition,
            map_func=function,
            cache_size=0,
        )

    with QueryCollector() as query_collector:
        for constraint in req:
            test_result = constraint.test(engine)
            assert operation(test_result.outcome)
        assert len(query_collector) > 20, query_collector.queries


@pytest.mark.parametrize(
    "data",
    [
        (
            negation,
            ["col_int", "col_varchar"],
            [(0, "hi0"), (1, "hi0")],
            0,
            filternull_element_or_tuple_any,
            False,
            [functools.partial(output_processor_limit, limit=5)],
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.9997569866342649 > 0 DISTINCT values (8228 / 8230) not being an element of '[(0, 'hi0'), (1, 'hi0')]'. It has excess elements '[(2, 'hi1'), (3, 'hi2'), (5, 'hi3'), (6, 'hi4'), (8, 'hi5'), '<SHORTENED OUTPUT, displaying the first 5 / 8228 elements above>']' with counts [2, 2, 2, 2, 2, '<SHORTENED OUTPUT, displaying the first 5 / 8228 counts above>'].",
        ),
    ],
)
def test_caching(engine, unique_table_largesize, data):
    (
        operation,
        columns,
        uniques,
        max_relative_violations,
        filter_func,
        compare_distinct,
        output_processors,
        function,
        condition,
        failure_message_suffix,
    ) = data

    req = requirements.WithinRequirement.from_table(*unique_table_largesize)
    req.add_uniques_subset_constraint(
        columns,
        uniques,
        max_relative_violations,
        filter_func=filter_func,
        compare_distinct=compare_distinct,
        output_processors=output_processors,
        condition=condition,
        map_func=function,
    )

    with QueryCollector() as query_collector:
        for constraint in req:
            for i in range(20):
                test_result = constraint.test(engine)
                assert operation(test_result.outcome)
        assert len(query_collector) < 20, query_collector.queries


@pytest.mark.parametrize(
    "data",
    [
        (
            negation,
            ["col_int", "col_varchar"],
            ["col_int", "col_varchar"],
            0,
            filternull_element_or_tuple_any,
            True,
            [output_processor_sort],
            None,
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.325 > 0 DISTINCT values (13 / 40) not being an element of '[(0, 'hi0'), (1, 'hi0'), (1, 'hi1'), (2, 'hi1'), (3, 'hi2'), (4, 'hi2'), (4, 'hi3'), (5, 'hi3'), (6, 'hi4'), (7, 'hi4'), (7, 'hi5'), (8, 'hi5'), (9, 'hi6'), (10, 'hi6'), (10, 'hi7'), (11, 'hi7'), (12, 'hi8'), (13, 'hi8'), (13, 'hi9'), (14, 'hi9'), (15, 'hi10'), (16, 'hi10'), (16, 'hi11'), (17, 'hi11'), (18, 'hi12'), (19, 'hi12'), (19, 'hi13')]'. It has excess elements '[(20, 'hi13'), (21, 'hi14'), (23, 'hi15'), (24, 'hi16'), (26, 'hi17'), (27, 'hi18'), (29, 'hi19'), (22, 'hi14'), (22, 'hi15'), (25, 'hi16'), (25, 'hi17'), (28, 'hi18'), (28, 'hi19')]' with counts [2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1].",
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            ["col_int", "col_varchar"],
            0,
            filternull_element_or_tuple_all,
            True,
            [output_processor_sort],
            None,
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.34146341463414637 > 0 DISTINCT values (14 / 41) not being an element of '[(0, 'hi0'), (1, 'hi0'), (1, 'hi1'), (2, 'hi1'), (3, 'hi2'), (4, 'hi2'), (4, 'hi3'), (5, 'hi3'), (6, 'hi4'), (7, 'hi4'), (7, 'hi5'), (8, 'hi5'), (9, 'hi6'), (10, 'hi6'), (10, 'hi7'), (11, 'hi7'), (12, 'hi8'), (13, 'hi8'), (13, 'hi9'), (14, 'hi9'), (15, 'hi10'), (16, 'hi10'), (16, 'hi11'), (17, 'hi11'), (18, 'hi12'), (19, 'hi12'), (19, 'hi13')]'. It has excess elements '[(20, 'hi13'), (21, 'hi14'), (23, 'hi15'), (24, 'hi16'), (26, 'hi17'), (27, 'hi18'), (29, 'hi19'), (None, 'hi'), (22, 'hi14'), (22, 'hi15'), (25, 'hi16'), (25, 'hi17'), (28, 'hi18'), (28, 'hi19')]' with counts [2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1].",
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            ["col_int", "col_varchar"],
            0,
            filternull_never,
            True,
            [output_processor_sort],
            None,
            None,
            None,
            "column(s) 'col_int', 'col_varchar' has a fraction of 0.35714285714285715 > 0 DISTINCT values (15 / 42) not being an element of '[(0, 'hi0'), (1, 'hi0'), (1, 'hi1'), (2, 'hi1'), (3, 'hi2'), (4, 'hi2'), (4, 'hi3'), (5, 'hi3'), (6, 'hi4'), (7, 'hi4'), (7, 'hi5'), (8, 'hi5'), (9, 'hi6'), (10, 'hi6'), (10, 'hi7'), (11, 'hi7'), (12, 'hi8'), (13, 'hi8'), (13, 'hi9'), (14, 'hi9'), (15, 'hi10'), (16, 'hi10'), (16, 'hi11'), (17, 'hi11'), (18, 'hi12'), (19, 'hi12'), (19, 'hi13')]'. It has excess elements '[(20, 'hi13'), (21, 'hi14'), (23, 'hi15'), (24, 'hi16'), (26, 'hi17'), (27, 'hi18'), (29, 'hi19'), (None, None), (None, 'hi'), (22, 'hi14'), (22, 'hi15'), (25, 'hi16'), (25, 'hi17'), (28, 'hi18'), (28, 'hi19')]' with counts [2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1].",
        ),
    ],
)
def test_uniques_subset_between_with_outputcheck(
    engine, unique_table1, unique_table2, data
):
    (
        operation,
        columns1,
        columns2,
        max_relative_violations,
        filter_func,
        compare_distinct,
        output_processors,
        map_func,
        condition1,
        condition2,
        failure_message_suffix,
    ) = data
    req = requirements.BetweenRequirement.from_tables(*unique_table1, *unique_table2)
    req.add_uniques_subset_constraint(
        columns1,
        columns2,
        max_relative_violations=max_relative_violations,
        filter_func=filter_func,
        compare_distinct=compare_distinct,
        output_processors=output_processors,
        map_func=map_func,
        condition1=condition1,
        condition2=condition2,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message
    assert test_result.failure_message.endswith(
        failure_message_suffix
    ), test_result.failure_message


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


@pytest.mark.parametrize(
    "data",
    [
        (identity, ["a"], ["b"]),
        (identity, ["c"], ["b"]),
        (negation, ["a"], ["c"]),
    ],
)
def test_functional_dependency_within(engine, functional_dependency_table, data):
    (
        operation,
        key_columns,
        value_columns,
    ) = data
    req = requirements.WithinRequirement.from_table(*functional_dependency_table)
    req.add_functional_dependency_constraint(key_columns, value_columns)
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (identity, ["a", "b"], ["c"]),
        (negation, ["a", "b"], ["d"]),
    ],
)
def test_functional_dependency_within_multi_key(
    engine, functional_dependency_table_multi_key, data
):
    (
        operation,
        key_columns,
        value_columns,
    ) = data
    req = requirements.WithinRequirement.from_table(
        *functional_dependency_table_multi_key
    )
    req.add_functional_dependency_constraint(key_columns, value_columns)
    assert operation(req[0].test(engine).outcome)


@pytest.mark.parametrize(
    "data",
    [
        (
            negation,
            ["a", "b"],
            ["d"],
            [output_processor_sort],
            textwrap.dedent(
                """\
                column(s) 'a', 'b', 'd' has violations of functional dependence (in total 10 rows):
                (1, 1, 3)
                (1, 1, 4)
                (1, 2, 5)
                (1, 2, 6)
                (2, 1, 7)
                (2, 1, 8)
                (2, 2, 9)
                (2, 2, 10)
                (44, 44, 12)
                (44, 44, 13)
                """
            ).strip(),
        ),
        (
            negation,
            ["a", "b"],
            ["c", "e"],
            [output_processor_sort],
            textwrap.dedent(
                """\
                column(s) 'a', 'b', 'c', 'e' has violations of functional dependence (in total 4 rows):
                (43, 43, 6, 6)
                (43, 43, 6, 7)
                (44, 44, None, None)
                (44, 44, None, 99)
                """
            ).strip(),
        ),
    ],
)
def test_functional_dependency_within_multi_key_with_outputcheck(
    engine, functional_dependency_table_multi_key, data
):
    (
        operation,
        key_columns,
        value_columns,
        output_processors,
        failure_message_suffix,
    ) = data
    req = requirements.WithinRequirement.from_table(
        *functional_dependency_table_multi_key
    )
    req.add_functional_dependency_constraint(
        key_columns,
        value_columns,
        output_processors=output_processors,
    )

    test_result = req[0].test(engine)
    assert operation(test_result.outcome)
    assert test_result.failure_message.endswith(
        failure_message_suffix
    ), test_result.failure_message


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
        (identity, ["col_int"], {0: (0.45, 0.55), 1: (0.45, 0.55)}, (0, 0)),
        (negation, ["col_int"], {0: (0.35, 0.55), 1: (0.55, 0.6)}, (0, 0)),
        (negation, ["col_int"], {0: (0.35, 0.55), 1: (0.35, 0.45)}, (0, 0)),
        (
            identity,
            ["col_int", "col_varchar"],
            {(0, "hi0"): (0.45, 0.55), (1, "hi0"): (0.25, 0.3), (1, "hi1"): (0.2, 0.3)},
            (0, 0),
        ),
        (
            negation,
            ["col_int", "col_varchar"],
            {
                (0, "hi0"): (0.45, 0.55),
                (1, "hi0"): (0.25, 0.3),
            },
            (0, 0),
        ),
        (identity, ["col_varchar"], {"hi0": (0.65, 0.85), "hi1": (0.1, 0.35)}, (0, 0)),
        (negation, ["col_varchar"], {"hi0": (0.65, 0.85)}, (0, 0)),
        (identity, ["col_varchar"], {"hi0": (0.65, 0.85)}, (0, 0.35)),
        (negation, ["col_varchar"], {}, (0, 0.35)),
    ],
)
def test_categorical_bound_within(engine, distribution_table, data):
    (operation, columns, distribution, default_bounds) = data
    req = requirements.WithinRequirement.from_table(*distribution_table)
    req.add_categorical_bound_constraint(columns, distribution, default_bounds)
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (negation, ["col_int"], {0: (0.40, 0.45), 1: (0.55, 0.60)}, 0),
        (negation, ["col_int"], {0: (0.40, 0.45), 1: (0.55, 0.60)}, 0.05),
        (identity, ["col_int"], {0: (0.40, 0.45), 1: (0.55, 0.60)}, 0.125),
    ],
)
def test_categorical_bound_within_relative_violations(engine, distribution_table, data):
    (operation, columns, distribution, max_relative_violations) = data
    req = requirements.WithinRequirement.from_table(*distribution_table)
    req.add_categorical_bound_constraint(
        columns, distribution, max_relative_violations=max_relative_violations
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


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


# There are 62 rows in the table's column.
# 60 of these values are numeric, 2 are NULL.
# Of these 60, 22 are part of the interval [5, 15].
@pytest.mark.parametrize(
    "data",
    [
        # 22/62 ~ .355
        (identity, 5, 15, 0.35, None),
        (negation, 5, 15, 0.36, None),
        # 22/60 ~ .366
        (identity, 5, 15, 0.36, Condition(raw_string="col_int IS NOT NULL")),
        (negation, 5, 15, 0.37, Condition(raw_string="col_int IS NOT NULL")),
    ],
)
def test_numeric_between_within(engine, unique_table1, data):
    (operation, lower_bound, upper_bound, min_fraction, condition) = data
    req = requirements.WithinRequirement.from_table(*unique_table1)
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
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        # The data at hand in int_table1 are [1, 2, ..., 19].
        # According to the definition of percentile in our doc string,
        # the 20th percentile should be the smallest value in our data
        # for which 20% of the data is less or equal that value.
        # For the value 3, we have that |{1,2,3}|/19 ~ .16 of the values
        # are less or equal.
        # For the value 4, we have that |{1,2,3,4}|/19 ~ .21 of the values
        # are less or equal.
        # Hence the expected 20th percentile should be 4.
        (identity, 20, 4, 0, 0, None),
        (identity, 20, 3.8, 0.21, None, None),
        (identity, 20, 3.8, None, 0.1, None),
        (negation, 20, 3.8, 0, None, None),
        (negation, 20, 3.8, None, 0, None),
        (negation, 20, 3.8, 0, 0, None),
        (negation, 20, 3.2, 0, 0, None),
        # The expected percentile changes when conditioning.
        # |{1,2}|/11 ~ .18
        # |{1,2,3}|/11 ~ .27
        (identity, 20, 3, 0, 0, Condition(raw_string="col_int <= 11")),
        (negation, 20, 2.8, 0, 0, Condition(raw_string="col_int <= 11")),
    ],
)
def test_numeric_percentile_within(engine, int_table1, data):
    (
        operation,
        percentage,
        expected_percentile,
        max_absolute_deviation,
        max_relative_deviation,
        condition,
    ) = data
    req = requirements.WithinRequirement.from_table(*int_table1)
    req.add_numeric_percentile_constraint(
        column="col_int",
        percentage=20,
        expected_percentile=expected_percentile,
        max_absolute_deviation=max_absolute_deviation,
        max_relative_deviation=max_relative_deviation,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        # With the following condition, we expect the following values
        # to be present in unique_table1's column col_int:
        # [0, 0, 1, 1, None]
        (
            identity,
            24,
            0,
            0,
            None,
            Condition(raw_string="col_int <= 1 or col_int IS NULL"),
        ),
        (
            identity,
            25,
            0,
            0,
            None,
            Condition(raw_string="col_int <= 1 or col_int IS NULL"),
        ),
        (
            identity,
            74,
            1,
            0,
            None,
            Condition(raw_string="col_int <= 1 or col_int IS NULL"),
        ),
        (
            identity,
            75,
            1,
            0,
            None,
            Condition(raw_string="col_int <= 1 or col_int IS NULL"),
        ),
        (
            identity,
            100,
            1,
            0,
            0,
            Condition(raw_string="col_int <= 1 or col_int IS NULL"),
        ),
    ],
)
def test_numeric_percentile_within_null(engine, unique_table1, data):
    (
        operation,
        percentage,
        expected_percentile,
        max_absolute_deviation,
        max_relative_deviation,
        condition,
    ) = data
    req = requirements.WithinRequirement.from_table(*unique_table1)
    req.add_numeric_percentile_constraint(
        column="col_int",
        percentage=percentage,
        expected_percentile=expected_percentile,
        max_absolute_deviation=max_absolute_deviation,
        max_relative_deviation=max_relative_deviation,
        condition=condition,
    )
    test_result = req[0].test(engine)
    assert test_result.outcome, test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        # The 20th percentile of int_table1 is 4.
        # The 20th percentile of int_table2 is 5.
        # Hence, the absolute deviation is 1 and
        # the relative deviation is 1/5 = .2.
        (identity, 20, 1, None, None, None),
        (identity, 20, None, 0.20, None, None),
        (identity, 20, 1, 0.20, None, None),
        (negation, 20, 0, 0, None, None),
        (negation, 20, 0.9, None, None, None),
        (negation, 20, None, 0.19, None, None),
        (identity, 20, 0, 0, Condition(raw_string="col_int >=2"), None),
    ],
)
def test_numeric_percentile_between(engine, int_table1, int_table2, data):
    (
        operation,
        percentage,
        max_absolute_deviation,
        max_relative_deviation,
        condition1,
        condition2,
    ) = data
    req = requirements.BetweenRequirement.from_tables(*int_table1, *int_table2)
    req.add_numeric_percentile_constraint(
        "col_int",
        "col_int",
        percentage=percentage,
        max_absolute_deviation=max_absolute_deviation,
        max_relative_deviation=max_relative_deviation,
        condition1=condition1,
        condition2=condition2,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


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
def test_integer_no_overlap_within_varying_key_columns(
    engine, integer_table_overlap, data, key_columns
):
    operation, max_relative_n_violations, condition = data
    req = requirements.WithinRequirement.from_table(*integer_table_overlap)
    req.add_numeric_no_overlap_constraint(
        key_columns=key_columns,
        start_column="range_start",
        end_column="range_end",
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
        # Overlap on the threshold
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
        (identity, 0, None),
    ],
)
def test_integer_no_gap_within_fixed_key_columns(engine, integer_table_gap, data):
    operation, max_relative_n_violations, condition = data
    req = requirements.WithinRequirement.from_table(*integer_table_gap)
    req.add_numeric_no_gap_constraint(
        key_columns=["id1"],
        start_column="range_start",
        end_column="range_end",
        max_relative_n_violations=max_relative_n_violations,
        legitimate_gap_size=0,
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
        (negation, 0, Condition(raw_string="id1 = 5")),
        (identity, 0.6, Condition(raw_string="id1 = 5")),
    ],
)
def test_float_no_gap_within_fixed_key_columns(engine, float_table_gap, data):
    operation, legitimate_gap_size, condition = data
    req = requirements.WithinRequirement.from_table(*float_table_gap)
    req.add_numeric_no_gap_constraint(
        key_columns=["id1"],
        start_column="range_start",
        end_column="range_end",
        legitimate_gap_size=legitimate_gap_size,
        max_relative_n_violations=0,
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


@pytest.mark.parametrize("computation_in_db", [True, False])
@pytest.mark.parametrize(
    "data",
    [
        (identity, "^hi[0-9]{1,2}$", None),
        (negation, "^hi[0-9]$", None),
    ],
)
def test_varchar_regex_within(engine, mix_table1, computation_in_db, data):
    (operation, regex, condition) = data
    req = requirements.WithinRequirement.from_table(*mix_table1)
    if computation_in_db:
        # bigquery dialect does not support regular expressions (sqlalchemy-bigquery 1.4.4)
        if is_mssql(engine) or is_bigquery(engine) or is_db2(engine):
            pytest.skip("Functionality not supported by given dialect.")
        req.add_varchar_regex_constraint_db(
            column="col_varchar",
            regex=regex,
            condition=condition,
        )
    else:
        req.add_varchar_regex_constraint(
            column="col_varchar",
            regex=regex,
            condition=condition,
        )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


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
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize("computation_in_db", [True, False])
@pytest.mark.parametrize(
    "data",
    [
        (identity, None, True, 0.3),
        (negation, None, True, 0.1),
        (identity, None, False, 0.3),
        (negation, None, False, 0.15),
    ],
)
def test_varchar_regex_tolerance(engine, varchar_table_real, computation_in_db, data):
    (operation, condition, aggregated, tolerance) = data
    req = requirements.WithinRequirement.from_table(*varchar_table_real)
    if computation_in_db:
        # The feature is not supported in sqlalchemy-bigquery 1.4.4
        if is_mssql(engine) or is_bigquery(engine) or is_db2(engine):
            pytest.skip("Functionality not supported by given dialect.")
        req.add_varchar_regex_constraint_db(
            "col_varchar",
            r"[A-Z][0-9]{2}\.[0-9]{0,2}$",
            condition=condition,
            relative_tolerance=tolerance,
            aggregated=aggregated,
        )
    else:
        req.add_varchar_regex_constraint(
            "col_varchar",
            r"[A-Z][0-9]{2}\.[0-9]{0,2}$",
            condition=condition,
            relative_tolerance=tolerance,
            aggregated=aggregated,
        )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize("computation_in_db", [True, False])
@pytest.mark.parametrize(
    "n_counterexamples, n_received_counterexamples",
    [
        (-1, 2),
        (0, 0),
        (1, 1),
        (2, 2),
        (3, 2),
    ],
)
def test_varchar_regex_counterexample(
    engine,
    varchar_table_real,
    computation_in_db,
    n_counterexamples,
    n_received_counterexamples,
):
    req = requirements.WithinRequirement.from_table(*varchar_table_real)
    if computation_in_db:
        # The feature is not supported in sqlalchemy-bigquery 1.4.4
        if is_mssql(engine) or is_bigquery(engine) or is_db2(engine):
            pytest.skip("Functionality not supported by given dialect.")
        req.add_varchar_regex_constraint_db(
            "col_varchar",
            r"[A-Z][0-9]{2}\.[0-9]{0,2}$",
            condition=None,
            relative_tolerance=0,
            aggregated=True,
            n_counterexamples=n_counterexamples,
        )
    else:
        req.add_varchar_regex_constraint(
            "col_varchar",
            r"[A-Z][0-9]{2}\.[0-9]{0,2}$",
            condition=None,
            relative_tolerance=0,
            aggregated=True,
            n_counterexamples=n_counterexamples,
        )
    test_result = req[0].test(engine)
    failure_message = test_result.failure_message
    # If no counterexample are given, this marker should not be present in the
    # failure message.
    marker = "Some counterexamples consist of the following: "
    location = failure_message.find(marker)
    if n_received_counterexamples == 0:
        assert location == -1
    else:
        assert location != -1
        # In the example of this very fixture, we know that no commas are used
        # in values. We can therefore assume that commas indicate separation
        # between counterexamples.
        assert (
            len(failure_message[location + len(marker) :].split(","))
            == n_received_counterexamples
        )


@pytest.mark.parametrize("computation_in_db", [True, False])
@pytest.mark.parametrize("n_counterexamples", [-2, -100])
def test_varchar_regex_counterexample_invalid(
    engine, varchar_table_real, computation_in_db, n_counterexamples
):
    req = requirements.WithinRequirement.from_table(*varchar_table_real)
    if computation_in_db:
        # TODO: This feature is available in snowflake-sqlalchemy 1.4.0.
        # Once we remove or update the pinned version, we can enable this test
        # for snowflake.
        # The feature is not supported in sqlalchemy-bigquery 1.4.4
        if is_mssql(engine) or is_snowflake(engine) or is_bigquery(engine):
            pytest.skip("Functionality not supported by given dialect.")
        req.add_varchar_regex_constraint_db(
            "col_varchar",
            r"[A-Z][0-9]{2}\.[0-9]{0,2}$",
            condition=None,
            relative_tolerance=0,
            aggregated=True,
            n_counterexamples=n_counterexamples,
        )
    else:
        req.add_varchar_regex_constraint(
            "col_varchar",
            r"[A-Z][0-9]{2}\.[0-9]{0,2}$",
            condition=None,
            relative_tolerance=0,
            aggregated=True,
            n_counterexamples=n_counterexamples,
        )
    with pytest.raises(ValueError):
        req[0].test(engine)


def test_backend_dependent_condition(engine, mix_table1):
    if is_mssql(engine):
        condition = Condition(raw_string="DATALENGTH(col_varchar) = 3")
    elif (
        is_postgresql(engine)
        or is_snowflake(engine)
        or is_bigquery(engine)
        or is_impala(engine)
        or is_db2(engine)
    ):
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
        (identity, "mix_table1", "mix_table2", "col_varchar", "col_varchar"),
        (identity, "mix_table1", "mix_table2", "col_int", "col_int"),
        (identity, "mix_table1", "mix_table2", "col_date", "col_date"),
        (negation, "mix_table1", "mix_table2", "col_varchar", "col_int"),
    ],
)
def test_column_type_between(engine, get_fixture, data):
    (operation, table_name1, table_name2, column1, column2) = data
    table1 = get_fixture(table_name1)
    table2 = get_fixture(table_name2)
    req = requirements.BetweenRequirement.from_tables(*table1, *table2)
    req.add_column_type_constraint(column1=column1, column2=column2)
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
    if is_bigquery(engine):
        pytest.skip("No primary key concept in BigQuery")
    if is_impala(engine):
        pytest.skip("Currently not implemented for impala.")

    (operation, columns) = data
    req = requirements.WithinRequirement.from_table(*pk_table)
    req.add_primary_key_definition_constraint(columns)
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


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
    # is not correctly compiled when dealing with snowflake or bigquery.
    # Use the mod function instead
    if (
        (is_snowflake(engine) or is_bigquery(engine) or is_impala(engine))
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
    [
        (identity, ["col_int"], None),
        (identity, ["col_int"], []),
        (identity, ["col_int"], ["col_date"]),
    ],
)
def test_uniqueness_within_infer_pk(engine, data, mix_table2_pk):
    if is_impala(engine):
        pytest.skip("Primary key retrieval currently not implemented for impala.")
    if is_bigquery(engine):
        pytest.skip("No primary key concept in BigQuery")
    # We purposefully select a non-unique column ["col_date"] to validate
    # that the reference columns are overwritten.
    operation, target_columns, selection_columns = data
    req = requirements.WithinRequirement.from_table(*mix_table2_pk)
    req.add_uniqueness_constraint(columns=selection_columns, infer_pk_columns=True)
    test_result = req[0].test(engine)
    # additional test: the PK columns are inferred during test time, i.e. we can check here if they were inferred correctly
    assert (
        req[0].ref.columns == target_columns
    ), f"Incorrect columns were retrieved from table. {req[0].ref.columns} != {target_columns}"
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
        (identity, 2 / 62),
        (negation, 2 / 63),
    ],
)
def test_max_null_fraction_within(engine, unique_table1, data):
    (operation, max_null_fraction) = data
    req = requirements.WithinRequirement.from_table(*unique_table1)
    req.add_max_null_fraction_constraint(
        column="col_int", max_null_fraction=max_null_fraction
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, "col_int", "col_int", 0),
        (identity, "col_varchar", "col_int", 0),
        (identity, "col_int", "col_varchar", 1),
        (negation, "col_int", "col_varchar", 0.99),
    ],
)
def test_max_null_fraction_between(engine, unique_table1, data):
    (operation, column1, column2, max_relative_deviation) = data
    req = requirements.BetweenRequirement.from_tables(
        *unique_table1,
        *unique_table1,
    )
    req.add_max_null_fraction_constraint(
        column1=column1,
        column2=column2,
        max_relative_deviation=max_relative_deviation,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, "col_varchar", "VARCHAR"),
        (identity, "col_int", "INTEGER"),
        (negation, "col_varchar", "INTEGER"),
        (identity, "col_varchar", sa.types.String()),
        (negation, "col_varchar", sa.types.Numeric()),
    ],
)
def test_column_type_within(engine, mix_table1, data):
    (operation, col_name, type_name) = data
    if is_impala(engine) and isinstance(type_name, str):
        type_name = {"VARCHAR": "string", "INTEGER": "int"}[type_name]
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
    if is_impala(engine):
        pytest.skip("Currently not implemented for Impala. EXCEPT throws syntax error.")
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
    if is_impala(engine):
        pytest.skip("Currently not implemented for Impala. EXCEPT throws syntax error.")
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
    if is_impala(engine):
        pytest.skip("Currently not implemented for Impala. EXCEPT throws syntax error.")
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
    # TODO: Not sure why this doesn't work
    if is_db2(engine):
        pytest.skip()
    if is_impala(engine):
        pytest.skip("Currently not implemented for Impala. EXCEPT throws syntax error.")
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
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize("key", [("some_id",), ("some_id", "extra_id")])
def test_groupby_aggregation_within(engine, groupby_aggregation_table_correct, key):
    skip_if_mssql(engine)
    # TODO: This should actually work for db2
    if is_db2(engine):
        pytest.skip()
    if is_impala(engine):
        pytest.skip("array_agg does not exist for Impala.")
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
    if is_db2(engine):
        pytest.skip()
    if is_impala(engine):
        pytest.skip("array_agg does not exist for Impala.")
    req = requirements.WithinRequirement.from_table(
        *groupby_aggregation_table_incorrect
    )
    req.add_groupby_aggregation_constraint(key, "value", 1, tolerance)
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [
        (identity, "col_int", "col_int", None, None, 1.0),
        (
            identity,
            "col_int",
            "col_int",
            Condition(raw_string="col_int >= 3"),
            Condition(raw_string="col_int >= 3"),
            1.0,
        ),
    ],
)
def test_ks_2sample_constraint_perfect_between(engine, int_table1, data):
    """
    Test Kolmogorov-Smirnov for the same column -> p-value should be perfect 1.0.
    """
    if is_db2(engine):
        pytest.skip()
    (operation, col_1, col_2, condition1, condition2, significance_level) = data
    req = requirements.BetweenRequirement.from_tables(*int_table1, *int_table1)
    req.add_ks_2sample_constraint(
        column1=col_1,
        column2=col_2,
        condition1=condition1,
        condition2=condition2,
        significance_level=significance_level,
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "condition1, condition2",
    [
        (
            None,
            Condition(raw_string="col_int >= 10"),
        ),
        (
            Condition(raw_string="col_int >= 10"),
            None,
        ),
        (
            Condition(raw_string="col_int >= 10"),
            Condition(raw_string="col_int >= 3"),
        ),
    ],
)
def test_ks_2sample_constraint_perfect_between_different_conditions(
    engine, int_table1, condition1, condition2
):
    """
    Test Kolmogorov-Smirnov for the same column but different conditions.
    As a consequence, since the data is distinct, the tests are expected
    to fail for a very high significance level.
    """
    # TODO: Figure out why this is necessary.
    if is_db2(engine):
        pytest.skip()
    req = requirements.BetweenRequirement.from_tables(*int_table1, *int_table1)
    req.add_ks_2sample_constraint(
        column1="col_int",
        column2="col_int",
        condition1=condition1,
        condition2=condition2,
        significance_level=1.0,
    )
    test_result = req[0].test(engine)
    assert negation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "data",
    [(negation, "col_int", "col_int", 0.05)],
)
def test_ks_2sample_constraint_wrong_between(
    engine, int_table1, int_square_table, data
):
    """
    Test kolmogorov smirnov test for table and square of table -> significance level should be less than default 0.05
    """
    # TODO: Figure out why this is necessary.
    if is_db2(engine):
        pytest.skip()
    (operation, col_1, col_2, min_p_value) = data
    req = requirements.BetweenRequirement.from_tables(*int_table1, *int_square_table)
    req.add_ks_2sample_constraint(
        column1=col_1, column2=col_2, significance_level=min_p_value
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message


@pytest.mark.parametrize(
    "configuration",
    [
        (identity, "value_0_1", "value_0_1", 0.8),  # p-value should be very high
        (
            negation,
            "value_0_1",
            "value_02_1",
            0.05,
        ),  # p-value should be very low, tables are not similar enough
        (
            negation,
            "value_0_1",
            "value_1_1",
            1e-50,
        ),  # test should fail even for very small values
    ],
)
def test_ks_2sample_random(engine, random_normal_table, configuration):
    if is_bigquery(engine) or is_impala(engine) or is_db2(engine):
        pytest.skip("It takes too long to insert the table.")

    (operation, col_1, col_2, min_p_value) = configuration
    req = requirements.BetweenRequirement.from_tables(
        *random_normal_table, *random_normal_table
    )
    req.add_ks_2sample_constraint(
        column1=col_1, column2=col_2, significance_level=min_p_value
    )
    test_result = req[0].test(engine)
    assert operation(test_result.outcome), test_result.failure_message
