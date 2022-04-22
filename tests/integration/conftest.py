import datetime
import itertools
import os
import urllib.parse

import pytest
import sqlalchemy as sa

from datajudge.db_access import is_mssql

TEST_DB_NAME = "tempdb"
SCHEMA = "dbo"  # 'dbo' is the standard schema in mssql


def get_engine(backend) -> sa.engine.Engine:
    address = os.environ.get("DB_ADDR", "localhost")
    if backend == "postgres":
        connection_string = f"postgresql://datajudge:datajudge@{address}:5432/datajudge"
    elif "mssql" in backend:
        connection_string = (
            f"mssql+pyodbc://sa:datajudge-123@{address}:1433/{TEST_DB_NAME}"
        )
        if backend == "mssql-freetds":
            connection_string += "?driver=libtdsodbc.so&tds_version=7.4"
        else:
            msodbc_driver_name = urllib.parse.quote_plus(
                "ODBC Driver 17 for SQL Server"
            )
            connection_string += f"?driver={msodbc_driver_name}"
    elif "snowflake" in backend:
        user = os.environ.get("SNOWFLAKE_USER", "datajudge")
        password = os.environ.get("SNOWFLAKE_PASSWORD")
        account = os.environ.get("SNOWFLAKE_ACCOUNT", "")
        connection_string = f"snowflake://{user}:{password}@{account}/datajudge/DBO?warehouse=datajudge&role=accountadmin"

    return sa.create_engine(connection_string)


@pytest.fixture(scope="module")
def engine(backend):
    engine = get_engine(backend)
    with engine.connect() as conn:
        if engine.name == "postgresql":
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    return engine


@pytest.fixture(scope="module")
def metadata():
    return sa.MetaData()


def _handle_table(engine, metadata, table_name, columns, data):
    engine.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{table_name}")
    with engine.connect() as conn:
        table = sa.Table(table_name, metadata, *columns, schema=SCHEMA)
        table.create(conn)
        conn.execute(table.insert().values(), data)


@pytest.fixture(scope="module")
def int_table1(engine, metadata):
    table_name = "int_table1"
    columns = [sa.Column("col_int", sa.Integer())]
    data = [{"col_int": i} for i in range(1, 20)]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def int_table2(engine, metadata):
    table_name = "int_table2"
    engine.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{table_name}")
    columns = [sa.Column("col_int", sa.Integer())]
    data = [{"col_int": i} for i in range(2, 20)]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def mix_table1(engine, metadata):
    table_name = "mix_table1"
    columns = [
        sa.Column("col_int", sa.Integer()),
        sa.Column("col_varchar", sa.String()),
        sa.Column("col_date", sa.DateTime()),
    ]
    data = [
        {
            "col_int": i,
            "col_varchar": f"hi{i}",
            "col_date": datetime.datetime(2016, 1, i),
        }
        for i in range(1, 20)
    ]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def mix_table2(engine, metadata):
    table_name = "mix_table2"
    columns = [
        sa.Column("col_int", sa.Integer()),
        sa.Column("col_varchar", sa.String()),
        sa.Column("col_date", sa.DateTime()),
    ]
    data = [
        {
            "col_int": i,
            "col_varchar": f"hi{i}",
            "col_date": datetime.datetime(2016, 1, i // 2),
        }
        for i in range(2, 20)
    ]
    data[5]["col_varchar"] = "ho"
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def date_table1(engine, metadata):
    table_name = "date_table1"
    columns = [
        sa.Column("col_date", sa.DateTime()),
    ]
    data = [{"col_date": datetime.datetime(2016, 1, i)} for i in range(1, 20)]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def date_table2(engine, metadata):
    table_name = "date_table2"
    columns = [
        sa.Column("col_date", sa.DateTime()),
    ]
    data = [{"col_date": datetime.datetime(2016, 1, i)} for i in range(2, 20)]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def date_table_overlap(engine, metadata):
    table_name = "date_table_overlap"
    columns = [
        sa.Column("id1", sa.Integer()),
        sa.Column("date_start", sa.DateTime()),
        sa.Column("date_end", sa.DateTime()),
    ]
    data = []
    # Trivial case: single entry.
    data += [
        {
            "id1": 1,
            "date_start": datetime.datetime(2016, 1, 1),
            "date_end": datetime.datetime(2016, 1, 10),
        }
    ]
    # 'Normal case': Multiple entries without overlap.
    data += [
        {
            "id1": 2,
            "date_start": datetime.datetime(2016, 1, i * 2),
            "date_end": datetime.datetime(2016, 1, i * 2 + 1),
        }
        for i in range(1, 5)
    ]
    # Multiple entries with non-singleton overlap.
    data += [
        {
            "id1": 3,
            "date_start": datetime.datetime(2016, 1, 1),
            "date_end": datetime.datetime(2016, 1, 10),
        },
        {
            "id1": 3,
            "date_start": datetime.datetime(2016, 1, 7),
            "date_end": datetime.datetime(2016, 1, 15),
        },
    ]
    # Multiple entries with singleton overlap.
    data += [
        {
            "id1": 4,
            "date_start": datetime.datetime(2016, 1, 1),
            "date_end": datetime.datetime(2016, 1, 10),
        },
        {
            "id1": 4,
            "date_start": datetime.datetime(2016, 1, 10),
            "date_end": datetime.datetime(2016, 1, 15),
        },
    ]
    # Multiple entries with subset relation.
    data += [
        {
            "id1": 5,
            "date_start": datetime.datetime(2016, 1, 1),
            "date_end": datetime.datetime(2016, 1, 10),
        },
        {
            "id1": 5,
            "date_start": datetime.datetime(2016, 1, 4),
            "date_end": datetime.datetime(2016, 1, 8),
        },
    ]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def date_table_overlap_2d(engine, metadata):
    table_name = "date_table_overlap_2d"
    columns = [
        sa.Column("id1", sa.Integer()),
        sa.Column("date_start1", sa.DateTime()),
        sa.Column("date_end1", sa.DateTime()),
        sa.Column("date_start2", sa.DateTime()),
        sa.Column("date_end2", sa.DateTime()),
    ]
    data = []
    # Single entry: should never cause a problem.
    data += [
        {
            "id1": 1,
            "date_start1": datetime.datetime(2016, 1, 1),
            "date_end1": datetime.datetime(2016, 1, 10),
            "date_start2": datetime.datetime(2016, 1, 1),
            "date_end2": datetime.datetime(2016, 1, 10),
        }
    ]
    # No overlaps.
    data += [
        {
            "id1": 2,
            "date_start1": datetime.datetime(2016, 1, i * 2),
            "date_end1": datetime.datetime(2016, 1, i * 2 + 1),
            "date_start2": datetime.datetime(2016, 1, i * 2),
            "date_end2": datetime.datetime(2016, 1, i * 2 + 1),
        }
        for i in range(1, 5)
    ]
    # No overlap, but actually just 1d.
    data += [
        {
            "id1": 3,
            "date_start1": datetime.datetime(2016, 1, i * 2),
            "date_end1": datetime.datetime(2016, 1, i * 2 + 1),
            "date_start2": datetime.datetime(2016, 1, 1),
            "date_end2": datetime.datetime(2016, 1, 1),
        }
        for i in range(1, 5)
    ]
    # No overlap but overlap in 1d.
    data += [
        {
            "id1": 4,
            "date_start1": datetime.datetime(2016, 1, i),
            "date_end1": datetime.datetime(2016, 1, i + 5),
            "date_start2": datetime.datetime(2016, 1, i * 2),
            "date_end2": datetime.datetime(2016, 1, i * 2 + 1),
        }
        for i in range(1, 5)
    ]
    # Non-singleton overlap between first and second row.
    data += [
        {
            "id1": 5,
            "date_start1": datetime.datetime(2015, 12, 31),
            "date_end1": datetime.datetime(2016, 1, 5),
            "date_start2": datetime.datetime(2016, 12, 31),
            "date_end2": datetime.datetime(2017, 1, 5),
        },
        {
            "id1": 5,
            "date_start1": datetime.datetime(2016, 1, 1),
            "date_end1": datetime.datetime(2016, 1, 10),
            "date_start2": datetime.datetime(2017, 1, 1),
            "date_end2": datetime.datetime(2017, 1, 10),
        },
        {
            "id1": 5,
            "date_start1": datetime.datetime(2016, 1, 11),
            "date_end1": datetime.datetime(2016, 1, 20),
            "date_start2": datetime.datetime(2017, 1, 11),
            "date_end2": datetime.datetime(2017, 1, 20),
        },
    ]

    # Singleton overlap.
    data += [
        {
            "id1": 6,
            "date_start1": datetime.datetime(2016, 1, 1),
            "date_end1": datetime.datetime(2016, 1, 10),
            "date_start2": datetime.datetime(2017, 1, 1),
            "date_end2": datetime.datetime(2017, 1, 10),
        },
        {
            "id1": 6,
            "date_start1": datetime.datetime(2016, 1, 10),
            "date_end1": datetime.datetime(2016, 1, 15),
            "date_start2": datetime.datetime(2017, 1, 10),
            "date_end2": datetime.datetime(2017, 1, 15),
        },
    ]
    # Subset overlap.
    data += [
        {
            "id1": 7,
            "date_start1": datetime.datetime(2016, 1, 1),
            "date_end1": datetime.datetime(2016, 1, 10),
            "date_start2": datetime.datetime(2017, 1, 1),
            "date_end2": datetime.datetime(2017, 1, 10),
        },
        {
            "id1": 7,
            "date_start1": datetime.datetime(2016, 1, 5),
            "date_end1": datetime.datetime(2016, 1, 7),
            "date_start2": datetime.datetime(2017, 1, 5),
            "date_end2": datetime.datetime(2017, 1, 7),
        },
    ]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def date_table_gap(engine, metadata):
    table_name = "date_table_gap"
    columns = [
        sa.Column("id1", sa.Integer()),
        sa.Column("date_start", sa.DateTime()),
        sa.Column("date_end", sa.DateTime()),
    ]
    data = []
    # Single entry should not be considered a gap.
    data += [
        {
            "id1": 1,
            "date_start": datetime.datetime(2016, 1, 1),
            "date_end": datetime.datetime(2016, 1, 10),
        }
    ]
    # Multiple entries without gap.
    data += [
        {
            "id1": 2,
            "date_start": datetime.datetime(2016, 1, i * 2),
            "date_end": datetime.datetime(2016, 1, i * 2 + 1),
        }
        for i in range(1, 5)
    ]
    # Multiple entries with overlap.
    data += [
        {
            "id1": 3,
            "date_start": datetime.datetime(2016, 1, 1),
            "date_end": datetime.datetime(2016, 1, 10),
        },
        {
            "id1": 3,
            "date_start": datetime.datetime(2016, 1, 7),
            "date_end": datetime.datetime(2016, 1, 15),
        },
    ]
    # Multiple entries with gap.
    data += [
        {
            "id1": 4,
            "date_start": datetime.datetime(2016, 1, 1),
            "date_end": datetime.datetime(2016, 1, 10),
        },
        {
            "id1": 4,
            "date_start": datetime.datetime(2016, 1, 12),
            "date_end": datetime.datetime(2016, 1, 15),
        },
    ]
    # Multiple entries on the threshold
    data += [
        {
            "id1": 5,
            "date_start": datetime.datetime(2016, 1, 1),
            "date_end": datetime.datetime(2016, 1, 10),
        },
        {
            "id1": 5,
            "date_start": datetime.datetime(2016, 1, 11),
            "date_end": datetime.datetime(2016, 1, 15),
        },
    ]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def date_table_keys(engine, metadata):
    table_name = "date_table_keys"
    columns = [
        sa.Column("id1", sa.Integer()),
        sa.Column("id2", sa.Integer()),
        sa.Column("date_start1", sa.DateTime()),
        sa.Column("date_end1", sa.DateTime()),
        sa.Column("date_start2", sa.DateTime()),
        sa.Column("date_end2", sa.DateTime()),
    ]
    data = []
    for id1, id2 in itertools.product([1, 2], repeat=2):
        data += [
            {
                "id1": id1,
                "id2": id2,
                "date_start1": datetime.datetime(2016, 1, i * 2),
                "date_end1": datetime.datetime(2016, 1, i * 2 + 1),
                "date_start2": datetime.datetime(2016, 1, i * 2),
                "date_end2": datetime.datetime(2016, 1, i * 2 + 1),
            }
            for i in range(1, 5)
        ]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def unique_table1(engine, metadata):
    table_name = "unique_table1"
    columns = [
        sa.Column("col_int", sa.Integer()),
        sa.Column("col_varchar", sa.String()),
    ]
    data = [{"col_int": i // 2, "col_varchar": f"hi{i // 3}"} for i in range(60)]
    data += [
        {"col_int": None, "col_varchar": None},
        {"col_int": None, "col_varchar": "hi"},
    ]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def unique_table2(engine, metadata):
    table_name = "unique_table2"
    columns = [
        sa.Column("col_int", sa.Integer()),
        sa.Column("col_varchar", sa.String()),
    ]
    data = [{"col_int": i // 2, "col_varchar": f"hi{i // 3}"} for i in range(40)]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def nested_table(engine, metadata):
    table_name = "nested_table"
    columns = [sa.Column("nested_varchar", sa.String())]
    data = [
        {"nested_varchar": "ABC#1,"},
        {"nested_varchar": "ABC#1,DEF#2,"},
        {"nested_varchar": "GHI#3,JKL#4,"},
    ]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def varchar_table1(engine, metadata):
    table_name = "varchar_table1"
    columns = [
        sa.Column("col_varchar", sa.String()),
    ]
    data = [{"col_varchar": "qq" * i} for i in range(1, 10)]
    data.append({"col_varchar": None})
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def varchar_table2(engine, metadata):
    table_name = "varchar_table2"
    columns = [
        sa.Column("col_varchar", sa.String()),
    ]
    data = [{"col_varchar": "qq" * i} for i in range(2, 11)]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def varchar_table_real(engine, metadata):
    table_name = "varchar_table_real"
    columns = [
        sa.Column("col_varchar", sa.String()),
    ]
    data = [
        {"col_varchar": val}
        for val in [
            "C72.80",
            "R34.45",
            "R34.45",
            "R34.45",
            "R06.0",
            "R06.0",
            "X70.0",
            "N07.9",
            "F12.7",
            "S26.06",
            "G01.6",
            "Z10.54",
            "I71.00",
            "X64.1",
            "M36.17",
            "U38.09",
            "V73.7Y",
            "V73.7Y",
            "V73.7Y",
            "L58.2X",
        ]
    ]
    _handle_table(engine, metadata, table_name, columns, data)

    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def pk_table(engine, metadata):
    table_name = "pk_table"
    columns = [
        sa.Column("col_int1", sa.Integer(), primary_key=True),
        sa.Column("col_int2", sa.Integer(), primary_key=True),
    ]
    data = [{"col_int1": i, "col_int2": i + 1} for i in range(1, 20)]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def row_match_table1(engine, metadata):
    table_name = "row_match_table"
    columns = [
        sa.Column("col_match1", sa.Integer()),
        sa.Column("col_match2", sa.Integer()),
        sa.Column("col_compare1", sa.Integer()),
        sa.Column("col_compare2", sa.Integer()),
    ]
    col_match1_data = range(0, 9)
    col_match2_data = range(1, 10)
    col_compare1_data = [0, 1, 2, 3, None, None, 7, 1, 1]
    col_compare2_data = [1, 2, 3, 4, 1, None, 8, 1, 1]
    data = [
        {"col_match1": a, "col_match2": b, "col_compare1": c, "col_compare2": d}
        for (a, b, c, d) in zip(
            col_match1_data,
            col_match2_data,
            col_compare1_data,
            col_compare2_data,
        )
    ]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def row_match_table2(engine, metadata):
    table_name = "row_match_table2"
    columns = [
        sa.Column("col_match1", sa.Integer()),
        sa.Column("col_match2", sa.Integer()),
        sa.Column("col_compare1", sa.Integer()),
        sa.Column("col_compare2", sa.Integer()),
    ]
    col_match1_data = list(range(0, 8)) + [None]
    col_match2_data = list(range(1, 8)) + [7, 9]
    col_compare1_data = [0, 1, 2, 4, 1, None, 7, 1, 1]
    col_compare2_data = [1, 2, 2, 4, None, None, 8, 1, 1]
    data = [
        {"col_match1": a, "col_match2": b, "col_compare1": c, "col_compare2": d}
        for (a, b, c, d) in zip(
            col_match1_data,
            col_match2_data,
            col_compare1_data,
            col_compare2_data,
        )
    ]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def min_gain_table1(engine, metadata):
    table_name = "min_gain_table1"
    columns = [sa.Column("col_int", sa.Integer())]
    data = [{"col_int": i} for i in range(1, 20)]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def min_gain_table2(engine, metadata):
    table_name = "min_gain_table2"
    columns = [sa.Column("col_int", sa.Integer())]
    data = [{"col_int": i} for i in range(2, 20)]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def groupby_aggregation_table_correct(engine, metadata):
    table_name = "groupby_aggregation_table"
    columns = [
        sa.Column("some_id", sa.Integer()),
        sa.Column("extra_id", sa.Integer()),
        sa.Column("value", sa.Integer()),
    ]
    data_source = [
        [34807101, 8, {1}],
        [42760071, 3, {7, 3, 4, 5, 6, 1, 2}],
        [42760071, 7, {1}],
        [44093821, 10, {2, 8, 3, 6, 7, 4, 1, 5}],
    ]
    data = [
        {"some_id": id, "extra_id": e, "value": v}
        for id, e, values in data_source
        for v in values
    ]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def groupby_aggregation_table_incorrect(engine, metadata):
    table_name = "groupby_aggregation_table_incorrect"
    columns = [
        sa.Column("some_id", sa.Integer()),
        sa.Column("extra_id", sa.Integer()),
        sa.Column("value", sa.Integer()),
    ]
    data_source = [
        [34807101, 8, {1}],
        [42760071, 3, {7, 3, 4, 5, 6, 1, 2}],
        [42760071, 7, {1}],
        [44093821, 7, {22, 19, 16, 24, 23, 21, 20, 18, 17}],
    ]
    data = [
        {"some_id": id, "extra_id": e, "value": v}
        for id, e, values in data_source
        for v in values
    ]
    _handle_table(engine, metadata, table_name, columns, data)
    return TEST_DB_NAME, SCHEMA, table_name


@pytest.fixture(scope="module")
def capitalization_table(engine, metadata):
    table_name = "capitalization"
    uppercase_column = "NAME"
    lowercase_column = "num_employees"
    # Create and populate table with raw strings as to ensure
    # the columns are actually created with capitalization.
    str_datatype = "VARCHAR(20)" if is_mssql(engine) else "TEXT"
    with engine.connect() as connection:
        connection.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{table_name}")
        connection.execute(
            f"CREATE TABLE {SCHEMA}.{table_name} "
            "(id INTEGER PRIMARY KEY, "
            f"{uppercase_column} {str_datatype}, {lowercase_column} INTEGER)"
        )
        connection.execute(
            f"INSERT INTO {SCHEMA}.{table_name} "
            f"(id, {uppercase_column}, {lowercase_column}) VALUES (1, 'QuantCo', 100)"
        )
    return TEST_DB_NAME, SCHEMA, table_name, uppercase_column, lowercase_column


def pytest_addoption(parser):
    parser.addoption(
        "--backend",
        choices=(("mssql", "mssql-freetds", "postgres", "snowflake")),
        help="which database backend to use to run the integration tests",
    )


def pytest_generate_tests(metafunc):
    if "backend" in metafunc.fixturenames:
        metafunc.parametrize(
            "backend", [metafunc.config.getoption("backend")], scope="module"
        )


# See https://github.com/pytest-dev/pytest/issues/349#issuecomment-471400399
# for context.
@pytest.fixture
def get_fixture(request):
    def _get_fixture(name):
        return request.getfixturevalue(name)

    return _get_fixture
