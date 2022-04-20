import pytest

from datajudge import Condition, WithinRequirement
from datajudge.db_access import is_mssql, is_postgresql

# These tests


@pytest.mark.parametrize("use_uppercase_column", [True, False])
@pytest.mark.parametrize("use_uppercase_query", [True, False])
def test_column_existence(
    engine, capitalization_table, use_uppercase_column, use_uppercase_query
):
    if is_mssql(engine) and use_uppercase_column != use_uppercase_query:
        pytest.skip("Mssql interface expects exact capitalization.")
    if is_postgresql(engine) and use_uppercase_query:
        pytest.skip("Postgres interface always expects lower-cased columns.")
    (
        db_name,
        schema_name,
        table_name,
        uppercase_column,
        lowercase_column,
    ) = capitalization_table
    column = uppercase_column if use_uppercase_column else lowercase_column
    column = column.upper() if use_uppercase_query else column.lower()
    req = WithinRequirement.from_table(db_name, schema_name, table_name)
    req.add_column_existence_constraint([column])
    test_result = req[0].test(engine)
    assert test_result.outcome, test_result.failure_message


@pytest.mark.parametrize("use_uppercase_column", [True, False])
@pytest.mark.parametrize("use_uppercase_query", [True, False])
def test_column_condition(
    engine, capitalization_table, use_uppercase_column, use_uppercase_query
):
    if is_mssql(engine) and use_uppercase_column != use_uppercase_query:
        pytest.skip("Mssql interface expects exact capitalization.")
    if is_postgresql(engine) and use_uppercase_query:
        pytest.skip("Postgres interface always expects lower-cased columns.")
    (
        db_name,
        schema_name,
        table_name,
        uppercase_column,
        lowercase_column,
    ) = capitalization_table
    column = uppercase_column if use_uppercase_column else lowercase_column
    column = column.upper() if use_uppercase_query else column.lower()
    req = WithinRequirement.from_table(db_name, schema_name, table_name)
    if use_uppercase_column:
        condition = Condition(raw_string=f"{column} != 'QuantCo'")
    else:
        condition = Condition(raw_string=f"{column} != 100")
    req.add_n_rows_max_constraint(0, condition=condition)
    test_result = req[0].test(engine)
    assert test_result.outcome, test_result.failure_message
