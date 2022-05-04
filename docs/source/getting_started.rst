Getting Started
============


Glossary
--------

- A ``DataSource`` represents a way to retrieve data from database. Typically, this corresponds to a table in the database. Yet, it could also be a more elaborate object. See the section on 'Alternative ``DataSource`` s' for more detail.

- A ``Constraint`` captures a concrete expectation between either two ``DataSource`` s or a single ``DataSource`` and a reference value.

- A ``Requirement`` captures all ``Constraint`` s between two given ``DataSource`` s or all ``Constraint`` s within a single ``DataSource``. If a ``Requirement`` refers links to two ``DataSource`` s, it is a ``BetweenRequirement``. If a ``Requirement`` merely refers to a single ``DataSource``, it is a ``WithinRequirement``.

- Conceptually, a 'specification' captures all ``Requirement`` s against a database. In practice that means it is usually a separate python file which:
  
  - gathers all relevant ``Requirement`` s
  - turns these ``Requirement`` s' ``Constraint`` s into individual tests
  - can be 'tested' by pytest


Creating a specification
------------------------

In order to get going, you might want to use the following snippet in a new python file.
This file will represent a specification.

.. code-block:: python

    import pytest
    import sqlalchemy as sa
    from datajudge.pytest_integration import collect_data_tests


    @pytest.fixture(scope="module")
    def datajudge_engine():
        # TODO: Adapt connection string to database at hand.
        return sa.create_engine("your_connection_string")


    # TODO: Insert Requirement objects to list.
    test = []

    requirements_constraint = collect_data_tests(requirements)

This file will eventually lead as an input to pytest. More on that in the section 'Testing a specification'.

In case you haven't worked with sqlalchemy engines before, you might need to install drivers to connect to your database. You might want to install snowflake-sqlalchemy when using Snowflake, pyscopg when using Postgres and platform-specific drivers (`Windows <https://docs.microsoft.com/en-us/sql/connect/odbc/windows/microsoft-odbc-driver-for-sql-server-on-windows?view=sql-server-ver15>`_, `Linux <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15>`_, `macOS <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver15>`_) when using MSSQL.


Specifying Constraints
----------------------

In order to discover possible ``Constraint`` s, please investigate the ``_add_*_constraint`` methods
for `BetweenRequirement <https://datajugde.readthedocs.io/en/latest/api/datajudge.requirements.html#datajudge.requirements.BetweenRequirement>`_
and `WithinRequirement <https://datajugde.readthedocs.io/en/latest/api/datajudge.requirements.html#datajudge.requirements.WithinRequirement>`_
respectively.

These methods are meant to be mostly self-documenting through the usage of expressive parameters.

Note that most ``Constraint`` s will allow for at least one ``Condition``. A ``Condition``
can be thought of as a conditional event in probability theory or a filter/clause in a database
query. Please consult the doc string of ``Condition`` for greater detail. For examples, please
see ``tests/unit/test_condition.py``.

Many ``Constraint`` s have optional ``columns`` parameters. If no argument is given, all
available columns will be used.


Defining limitations of change
------------------------------

``BetweenRequirement`` s allow for ``Constraint`` s expressing the limitation of a loss or gain. For example, the ``NRowsMinGain`` ``Constraint``
expresses by how much the number of rows must at least grow from the first ``DataSource`` to the second. In the example of ``NRowsMinGain`` ,
this growth limitation is expressed relative to the number of rows of the first ``DataSource``.

Generally, such relative limitations can be defined in two ways:
- manually, based on domain knowledge (e.g. 'at least 5% growth')
- automatically, based on date ranges

The former would translate to

::

    \#rows_table_2 > (1 + min_relative_gain) * #rows_table_1

while the latter would translate to

::

   date_growth := (max_date_table_2 - min_date_table_2) / (max_date_table_1 - min_date_table_1)
   \#rows_table_2 > (1 + date_growth) * \#rows_table_1


In the latter case a date column must be passed during the instantiation of the ``BetweenRequirement``. Moreover, the  and ``date_range_*`` must be passed
in the respective ``add_*_constraint`` method. When using date ranges as an indicator of change, the ``constant_max_*`` argument can safely be ignored. Additionally,
an additional buffer to the date growth can be added with help of the ``date_range_gain_deviation`` parameter:

::

   date_growth := (max_date_table_2 - min_date_table_2) / (max_date_table_1 - min_date_table_1)
   \#rows_table_2 > (1 + date_growth + date_range_gain_deviation) + * \#rows_table_1

This example revolving around ``NRowsMinGain`` generalizes to many ``Constraint`` s concerned with growth, gain, loss or shrinkage limitations.


Testing a specification
-----------------------

In order to test whether the ``Constraint`` s expressed in a specification hold true, you can simply run

::

    pytest your_specification.py

This will produce results directly in your terminal. If you prefer to additionally generate a report,
you can run

::

   pytest your_specification.py --html=your_report.html

As the testing relies on `pytest <https://docs.pytest.org/en/latest/>`_, all of `pytest`'s features can be
used, e.g. early stopping with `-x` or sub-selecting specific tests with `-k`.


Test information
----------------

When calling a ``Constraint``'s ``test`` method, a ``TestResult`` is returned. The latter comes with a
``logging_message`` field. This field comprises information about the test failure, the constraint at hand
as well as the underlying database queries.

Depending on the use case at hand, it might make sense to rely on this information for logging or data investigation
purposes.


Alternative DataSources
---------------------------

A ``Requirement`` is instantiated with either one or two fixed ``DataSource`` s.

While the most typical example of a ``DataSource`` would be a table in a database, ``datajudge`` allows
for other ``DataSource`` s as well. These are often derived from primitive tables of a database.

.. list-table:: DataSources
   :header-rows: 1

   * - ``DataSource``
     - explanation
     - ``WithinRequirement`` constructor
     - ``BetweenRequirement`` constructor
   * - ``TableDataSource``
     - represents a table in a database
     - ``WithinRequirement.from_table``
     - ``BetweenRequirement.from_tables``
   * - ``ExpressionDataSource``
     - represents the result of a ``sqlalchemy`` expression
     - ``WithinRequirement.from_table``
     - ``BetweenRequirement.from_tables``
   * - ``RawQueryDataSource``
     - represents the result of a sql query expressed via a string
     - ``WithinRequirement.from_raw_query``
     - ``BetweenRequirement.from_raw_queries``


Typically, a user does not need to instantiate a corresponding ``DataSource`` themselves. Rather, this is taken care
of by using the appropriate constructor for ``WithinRequirement`` or ``BetweenRequirement``.

Note that in principle, several tables can be combined to make up for a single ``DataSource``. Yet, most of
the time when trying to compare two tables, it is more convenient to create a ``BetweenRequirement`` and use
the ``from_tables`` constructor.


Column capitalization
---------------------

Different database management systems handle the capitalization of entities, such as column names, differently.
For the time being:

- Mssql: ``datajudge`` expects column name capitalization as is seen in database, either lowercase or uppercase.
- Postgres: ``datajudge`` expects lowercase column names.
- Snowflake: ``datajudge`` will lowercase independently of the capitalization provided.

The Snowflake behavior is due to an upstream `bug <https://github.com/snowflakedb/snowflake-sqlalchemy/issues/157>`_
in snowflake-sqlalchemy.

This behavior is subject to change.

