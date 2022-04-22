# datajudge

[![CI](https://github.com/Quantco/datajudge/actions/workflows/ci.yaml/badge.svg)](https://github.com/Quantco/datajudge/actions/workflows/ci.yaml)

Express and test specifications against data from database.

# Why not `great expectations`?

The major selling point is to be able to conveniently express expectations _between_ different `DataSource`s. `great expectations`, in contrast, focuses on expectations against single `DataSource`s.

Over time, we have found some people find the following aspects of datajudge useful
* Lots of the 'query writing' is off-loaded to the library by having `Constraint`s tailored to our needs
* Easier/faster onboarding than with `great expectations`
* Assertion messages with counterexamples and other context information, speeding up the data debugging process

# Installation instructions

This package is available on QuantCo's conda channel and installing it via the conda channel is strongly encouraged. Once you've added the QuantCo channel to your list of channels in use, you an simply run `conda install datajudge` from the environment of your choice. You will likely want to use `datajudge` in conjuction with other packages - in particular `pytest` and database drivers relevant to your database. You might want to install `snowflake-sqlalchemy` when using snowflake, `pyscopg` when using postgres and platform-specific drivers ([Windows](https://docs.microsoft.com/en-us/sql/connect/odbc/windows/microsoft-odbc-driver-for-sql-server-on-windows?view=sql-server-ver15), [Linux](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15), [macOS](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver15)) when using mssql.

If for some reason, you prefer not to install the package via the conda channel, you'll need to do as follows:

```bash
git clone https://github.com/Quantco/datajudge
cd datajudge
conda activate your_client_environment
pip install --no-build-isolation --disable-pip-version-check .
```

For development _on_ `datajudge` - in contrast to merely using it - you can get started as follows:
```bash
git clone https://github.com/Quantco/datajudge
cd datajudge
mamba env create
conda activate datajudge
pip install --no-build-isolation --disable-pip-version-check -e .
```

# Example

To get started, we will create a sample database using sqlite that contains a list of companies.

```python
import sqlalchemy as sa

eng = sa.create_engine('sqlite:///example.db')

with eng.connect() as con:
    con.execute("CREATE TABLE companies (id INTEGER PRIMARY KEY, name TEXT, num_employees INTEGER)")
    con.execute("INSERT INTO companies (name, num_employees) VALUES ('QuantCo', 100), ('Google', 150000), ('BMW', 120000), ('Apple', 145000)")
    con.execute("CREATE TABLE companies_archive (id INTEGER PRIMARY KEY, name TEXT, num_employees INTEGER)")
    con.execute("INSERT INTO companies_archive (name, num_employees) VALUES ('QuantCo', 90), ('Google', 140000), ('BMW', 110000)")
```

The table "companies_archive" contains three entries:

| id  | name    | num_employees |
| --- | ------- | ------------- |
| 1   | QuantCo | 90            |
| 2   | Google  | 140,000       |
| 3   | BMW     | 110,000       |

While "companies" contains an additional entry:

| id  | name    | num_employees |
| --- | ------- | ------------- |
| 1   | QuantCo | 100           |
| 2   | Google  | 150,000       |
| 3   | BMW     | 120,000       |
| 4   | Apple   | 145,000       |

As an example, we will run 4 tests on this table:

- Does the table "companies" contain a column named "name"?
- Does the table "companies" contain at least 1 entry with the name "QuantCo"?
- Does the column "num_employees" of the "companies" table have all positive values?
- Does the column "name" of the table "companies" contain at least all the values of
  the corresponding column in "companies_archive"?

```python
import pytest
import sqlalchemy as sa

from datajudge import (
    Condition,
    WithinRequirement,
    BetweenRequirement,
)
from datajudge.pytest_integration import collect_data_tests


# We create a Requirement (within table). This object will contain
# all the constraints we want to test on the specified table.
# To test another table or test the same table against another table,
# we would create another Requirement object.
companies_req = WithinRequirement.from_table(
    db_name="example", schema_name=None, table_name="companies"
)

# Adding a constraint: column "name" should exist in our table
companies_req.add_column_existence_constraint(columns=["name"])

# Adding a constraint with a condiction
condition = Condition(raw_string="name = 'QuantCo'")
companies_req.add_n_rows_min_constraint(n_rows_min=1, condition=condition)

companies_req.add_numeric_min_constraint(column="num_employees", min_value=1)

# Creating a requirement (set of constraints) to test constraint between
# the table "companies" and the table "companies_archive".
companies_between_req = BetweenRequirement.from_tables(
    db_name1="example",
    schema_name1=None,
    table_name1="companies",
    db_name2="example",
    schema_name2=None,
    table_name2="companies_archive",
)

companies_between_req.add_row_superset_constraint(
    columns1=['name'], columns2=['name'], constant_max_missing_fraction=0
)


@pytest.fixture(scope="module")
def datajudge_engine():
    return sa.create_engine("sqlite:///example.db")


requirements = [companies_req, companies_between_req]
# "collect_data_tests" expects a pytest fixture with the name "datajudge_engine" that is a SQLAlchemy engine.
test_constraint = collect_data_tests(requirements)
```

Saving this file as ``specification.py`` and calling ``pytest specification.py -v``
will verify that all constaints are satisfied. The output you see in the terminal
should be similar to this:

```bash
=================================== test session starts ===================================
platform darwin -- Python 3.9.7, pytest-6.2.5, py-1.11.0, pluggy-1.0.0 -- /Users/marc_qco/miniconda3/envs/datajudge/bin/python
cachedir: .pytest_cache
metadata: {'Python': '3.9.7', 'Platform': 'macOS-11.6-x86_64-i386-64bit', 'Packages': {'pytest': '6.2.5', 'py': '1.11.0', 'pluggy': '1.0.0'}, 'Plugins': {'html': '3.1.1', 'metadata': '1.11.0', 'cov': '3.0.0'}}
rootdir: /Users/marc_qco/repos/datajudge
plugins: html-3.1.1, metadata-1.11.0, cov-3.0.0
collected 4 items

specification.py::test_constraint[ColumnExistence::companies] PASSED                [ 25%]
specification.py::test_constraint[NRowsMin::companies] PASSED                       [ 50%]
specification.py::test_constraint[NumericMin::companies] PASSED                     [ 75%]
specification.py::test_constraint[RowSuperset::companies|companies_archive] PASSED  [100%]

==================================== 4 passed in 0.31s ====================================
```

You can also use a formatted html report using the ``--html=report.html`` flag.


# Nomenclature

`Constraint` < `Requirement` < `Specification`.

A `Constraint` captures a comparison between either two `DataSource`s or a single `DataSource`
and a reference value.

A `Requirement` captures all `Constraint`s between two given `DataSource`s or all
`Constraint`s within a `DataSource`.

A specification captures all requirements against a database.


# Usage instructions

## Creating a specification

In order to get going, you might want to copy `skeleton.py` to the location of
your choice. The skeleton represents a specification. A specification contains
`Requirement`s, each expressing all expectations against either a single data source
with `WithinRequirement` or all expectations against an ordered pair of data sources
with `BetweenRequirement`.
Requirements are composed of `Constraint`s. See the section on `Constraint`s below.

## Testing a specification

Run `pytest test_$placeholder.py --html=report.html`.

You can inspect the test results either directly in your terminal or via
the generated html report `report.html`.

As this relies on [pytest](https://docs.pytest.org/en/latest/), all of pytest's feature can be leveraged


## Specifying constraints

In order to discover possible `Constraint`s, please investigate the `_add_*_constraint` methods
in [`requirements.py`](https://github.com/Quantco/datajudge/blob/master/src/datajudge/requirements.py).
The latter are meant to be mostly self-documenting through usage of expressive
parameters.

Note that most `Constraint`s will allow for at least one `Condition`. A `Condition`
can be thought of as a conditional elevent in probability theory or filter/clause in a datbase
query. Please consult the doc string of `Condition` for greater detail. For examples, please
see `tests/unit/test_condition.py`.

Many `Constraint`s have optional `columns` parameters. If no argument is given, all
columns available from a table will be used.

`Constraint`s on `BetweenRequirement`s featuring a gain or loss limitation offer
both a constant maximal reference value as well a comparison of date
ranges. The latter aproach allows for some further manual adjustment. In case one
intends to use a date range baseline, the date column must be passed during
the instantiation of the `Requirement` and `date_range_*` must be passed
in the respective `add_*_constraint` method. In said case, the `constant_max_*`
argument can safely be ignored. E.g.:

```#rows_1 < max_relative_gain * #rows_0```

or

```#rows_1 < ((max_date_1 - min_date_1)/(max_date_0 - min_date_0) - 1 + date_range_gain_deviation) #rows_0```


## Alternative `DataSource`s
The 'Example' section above creates a `Requirement`. Such a `Requirement` object
is instantiated with fixed `DataSource`s. A `DataSource` expresses how the data to be tested
is obtained from database. In the example above, each source is a plain and simple database
table. Yet, at times, one might want to check expectations and impose
constraints on objects which are derived from primitive tables of a database.

For that purpose, the `WithinRequirement` class supports two further constructors: `from_expression` and
`from_raw_query`. Analogously, `BetweenRequirement` supports `from_expressions` and `from_raw_queries`.

These constructors allow to create a `Requirement` object based on a sqlalchemy expression or
a select query in raw string format respectively. For greater detail, please refer to the respective `Requirement` class.

Note that in principle, several tables _can_ be combined to make up for a single data source. Yet, most of
the time when trying to compare two tables, it is more convenient to create a `BetweenRequirement` and use
the `from_tables` constructor.


## Column capitalization

Different database management systems handle capitalization of entities such as column names differently.
For the time being, we define the following behavior:
* Mssql: The user should provide the column name capitalization as is seen in database, either lower or uppercase.
* Postgres: The user should always provide lower case column names.
* Snowflake: Whatever capitalization the user provides, it'll be translated to lower case capitalization.

The snowflake behavior is due to an upstream [bug](https://github.com/snowflakedb/snowflake-sqlalchemy/issues/157)
in `snowflake-sqlalchemy`.

This behavior is subject to change.

## Logging

The `TestResult` class comes with a `logging_message` property. The latter comprises information about
the test failure, the constraint at hand as well as the underlying database queries.

## Database management system support

While designed as dbms-agnostically as possible, `datajudge` likely doesn't work in its entirety for every dbms.
We run integration tests against various versions of postgres, mssql and snowflake.
