import os

import pytest
import sqlalchemy as sa

from datajudge import BetweenRequirement, Condition, WithinRequirement
from datajudge.pytest_integration import collect_data_tests


@pytest.fixture(scope="module")
def datajudge_engine():
    address = os.environ.get("DB_ADDR", "localhost")
    connection_string = f"postgresql://datajudge:datajudge@{address}:5432/datajudge"
    return sa.create_engine(connection_string)


# Postgres' default database.
db_name = "tempdb"
# Postgres' default schema.
schema_name = "public"


# 1. Sanity check on new version based on domain knowledge.
within_requirement = WithinRequirement.from_table(
    table_name="twitch_v2",
    schema_name=schema_name,
    db_name=db_name,
)

within_requirement.add_varchar_regex_constraint(
    column="language",
    regex="^[a-zA-Z]+$",
)


# 2. Sanity check between old version and new version of the data.
between_requirement_version = BetweenRequirement.from_tables(
    db_name1=db_name,
    db_name2=db_name,
    schema_name1=schema_name,
    schema_name2=schema_name,
    table_name1="twitch_v1",
    table_name2="twitch_v2",
)

between_requirement_version.add_column_subset_constraint()
between_requirement_version.add_column_superset_constraint()
columns = ["channel", "partnered", "mature"]
between_requirement_version.add_row_subset_constraint(
    columns1=columns, columns2=columns, constant_max_missing_fraction=0
)
between_requirement_version.add_row_matching_equality_constraint(
    matching_columns1=["channel"],
    matching_columns2=["channel"],
    comparison_columns1=["language"],
    comparison_columns2=["language"],
    max_missing_fraction=0,
)
between_requirement_version.add_ks_2sample_constraint(
    column1="average_viewers",
    column2="average_viewers",
    significance_level=0.05,
)
between_requirement_version.add_uniques_equality_constraint(
    columns1=["language"],
    columns2=["language"],
)


# 3. Sanity check between different columns of the new version.
between_requirement_columns = BetweenRequirement.from_tables(
    db_name1=db_name,
    db_name2=db_name,
    schema_name1=schema_name,
    schema_name2=schema_name,
    table_name1="twitch_v2",
    table_name2="twitch_v2",
)

between_requirement_columns.add_numeric_mean_constraint(
    column1="average_viewers",
    column2="average_viewers",
    condition1=None,
    condition2=Condition(raw_string="mature IS TRUE"),
    max_absolute_deviation=0.1,
)


# 4. Collect all requirements and make them discoverable by pytest.

requirements = [
    within_requirement,
    between_requirement_version,
    between_requirement_columns,
]
test_func = collect_data_tests(requirements)
