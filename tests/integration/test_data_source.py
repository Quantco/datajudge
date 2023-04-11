import pytest
import sqlalchemy as sa

from datajudge import BetweenRequirement, Condition
from datajudge.db_access import (
    DataReference,
    ExpressionDataSource,
    RawQueryDataSource,
    get_column_names,
    is_bigquery,
)


def identity(boolean_value):
    return boolean_value


def negation(boolean_value):
    return not boolean_value


def test_custom_data_source_from_query(engine, int_table1, int_table2):
    _, schema_name1, table_name1 = int_table1
    _, schema_name2, table_name2 = int_table2
    union = "UNION"
    if is_bigquery(engine):
        # Bigquery only knows about UNION DISTINCT and UNION ALL
        # The UNION statement in postgres is equivalent to UNION DISTINCT
        union = "UNION DISTINCT"

    query = (
        f"SELECT * FROM {schema_name1}.{table_name1} {union} "
        f"SELECT * FROM {schema_name2}.{table_name2}"
    )
    data_source = RawQueryDataSource(query, "string query")
    derived_clause = data_source.get_clause(engine)
    derived_selection = sa.select(derived_clause)
    with engine.connect() as connection:
        rows = connection.execute(derived_selection).scalars().fetchall()
    assert set(rows) == set(range(1, 20))


def test_custom_data_source_from_expression(engine, metadata, int_table1, int_table2):
    _, schema_name1, table_name1 = int_table1
    _, schema_name2, table_name2 = int_table2
    table1 = sa.Table(table_name1, metadata, schema=schema_name1, autoload_with=engine)
    table2 = sa.Table(table_name2, metadata, schema=schema_name2, autoload_with=engine)
    subquery = sa.union(
        sa.select(table1).where(table1.c["col_int"] < 10),
        sa.select(table2).where(table2.c["col_int"] > 10),
    ).subquery()
    selection = sa.select(subquery)
    data_source = ExpressionDataSource(selection, "expression")
    derived_clause = data_source.get_clause(engine)
    derived_selection = sa.select(derived_clause)
    with engine.connect() as connection:
        rows = connection.execute(derived_selection).scalars().fetchall()

    assert set(rows) == (set(range(1, 10)) | set(range(11, 20)))


@pytest.mark.parametrize(
    "expression_getter",
    [
        lambda table_name, metadata, **kwargs: sa.Table(
            table_name,
            metadata,
            **kwargs,
        ),
        lambda table_name, metadata, **kwargs: sa.Table(
            table_name,
            metadata,
            **kwargs,
        ).alias(),
        lambda table_name, metadata, **kwargs: sa.select(
            sa.Table(
                table_name,
                metadata,
                **kwargs,
            )
        ).where(sa.text("col_int > 3")),
    ],
)
def test_get_column_names_expression(engine, metadata, int_table1, expression_getter):
    _, schema_name, table_name = int_table1
    expression = expression_getter(
        table_name, metadata, schema=schema_name, autoload_with=engine
    )
    data_source = ExpressionDataSource(expression, "expression")
    data_ref = DataReference(data_source)
    factual_column_names, _ = get_column_names(engine, data_ref)
    assert set(factual_column_names) == {"col_int"}


def test_get_column_names_raw_query(engine, int_table1):
    _, schema_name, table_name = int_table1
    query = f"SELECT col_int FROM {schema_name}.{table_name}"
    data_source = RawQueryDataSource(query, "query", ["col_int"])
    data_ref = DataReference(data_source)
    factual_column_names, _ = get_column_names(engine, data_ref)
    assert set(factual_column_names) == {"col_int"}


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
def test_uniques_subset_between_expression(
    engine, metadata, unique_table1, unique_table2, data
):
    (
        operation,
        columns1,
        columns2,
        max_relative_violations,
        map_func,
        condition1,
        condition2,
    ) = data
    _, schema_name1, table_name1 = unique_table1
    _, schema_name2, table_name2 = unique_table2
    table1 = sa.Table(table_name1, metadata, schema=schema_name1, autoload_with=engine)
    table2 = sa.Table(table_name2, metadata, schema=schema_name2, autoload_with=engine)

    req = BetweenRequirement.from_expressions(
        table1,
        table2,
        "expression1",
        "expression2",
    )
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
def test_uniques_subset_between_raw_query(engine, unique_table1, unique_table2, data):
    (
        operation,
        columns1,
        columns2,
        max_relative_violations,
        map_func,
        condition1,
        condition2,
    ) = data
    _, schema_name1, table_name1 = unique_table1
    _, schema_name2, table_name2 = unique_table2
    query1 = f"SELECT col_int FROM {schema_name1}.{table_name1}"
    query2 = f"SELECT col_int FROM {schema_name2}.{table_name2}"
    req = BetweenRequirement.from_raw_queries(
        query1, query2, "query1", "query2", columns1=["col_int"], columns2=["col_int"]
    )

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
