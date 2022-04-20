import pytest
import sqlalchemy as sa

from datajudge.db_access import ExpressionDataSource, TableDataSource


@pytest.mark.parametrize(
    "input, expected",
    [(("db", "table", "schema"), "db.schema.table"), (("db", "table"), "table")],
)
def test_table_data_source_string(input, expected):
    ds = TableDataSource(*input)
    assert str(ds) == expected


@pytest.mark.parametrize(
    "expression, name, expected",
    [
        (
            sa.Table("table", sa.MetaData(), schema="schema"),
            "custom_source",
            "custom_source",
        ),
    ],
)
def test_expression_data_source_string(expression, name, expected):
    ds = ExpressionDataSource(expression, name)
    assert str(ds) == expected
