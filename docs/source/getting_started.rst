Getting Started
============


Glossary
--------

- A ``DataSource`` represents a way to retrieve data from database. Typically, this corresponds to a table in the database. Yet, it could also be a more elaborate object. See the section on 'Alternative ``DataSource`` s' for more detail.

- A ``Constraint`` captures a concrete expectation between either two ``DataSource`` s or a single ``DataSource`` and a reference value.

- A ``Requirement`` captures all ``Constraint`` s between two given ``DataSource`` s or all ``Constraint`` s within a single ``DataSource``. If a ``Requirement`` refers links to ``DataSource`` s, it is a ``BetweenRequirement``. If a ``Requirement`` merely refers to a single ``DataSource``, it is a ``WithinRequirement``.

- A specification captures all ``Requirement`` s against a database.


Creating a specification
------------------------

In order to get going, you might want to copy `` skeleton.py`` to the location of
your choice. The skeleton represents a specification.


Specifying Constraints
----------------------

In order to discover possible ``Constraint`` s, please investigate the ``_add_*_constraint`` methods
for `BetweenRequirement <https://datajugde.readthedocs.io/en/latest/api/datajudge.requirements.html#datajudge.requirements.BetweenRequirement>`_
and `WithinRequirement <https://datajugde.readthedocs.io/en/latest/api/datajudge.requirements.html#datajudge.requirements.WithinRequirement>`_
respectively.

These methods are meant to be mostly self-documenting through usage of expressive parameters.

Note that most ``Constraint`` s will allow for at least one ``Condition``. A ``Condition``
can be thought of as a conditional event in probability theory or a filter/clause in a datbase
query. Please consult the doc string of ``Condition`` for greater detail. For examples, please
see ``tests/unit/test_condition.py``.

Many ``Constraint`` s have optional ``columns`` parameters. If no argument is given, all
available columns  will be used.

``BetweenRequirement`` s allow for ``Constraint`` s expressing the limitation of a loss or gain (e.g. ``NRowsMinGain``).
These limitations can often be defined explicitly or be the result of a comparison of date ranges.
In the latter case the date column must be passed during the instantiation of the ``BetweenRequirement`` and ``date_range_*`` must be passed
in the respective ``add_*_constraint`` method. When using date ranges as an indicator of change, the ``constant_max_*``
argument can safely be ignored.


Testing a specification
-----------------------

In order to test whether the ``Constraint`` s expressed in a specification hold true, you can simply run

::
    pytest your_specification.py

This will produce results directly in your terminal. If you prefer to additionally generate a pdf report,
you can run

::
   pytest your_specification.py --html=your_report.html

As the testing relies on `pytest<https://docs.pytest.org/en/latest/>`__, all of `pytest`'s features can be
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

While the most typical example of a ``DataSource`` seems to be a table in a database, ``datajudge`` allows
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
     - represents the result of a `` sqlalchemy`` expression
     - ``WithinRequirement.from_table``
     - ``BetweenRequirement.from_tables``
   * - ``RawQueryDataSource``
     - represents the result of a sql query expressed via a string
     - ``WithinRequirement.from_raw_query``
     - ``BetweenRequirement.from_raw_queries``


Typically, a user does not need to instantiate a corresponding ``DataSource`` themselves. Rather, this is taken care
of by using the appropriate constructor for ``WithinRequirement`` or ``BetweenRequirement``.

Note that in principle, several tables _can_ be combined to make up for a single ``DataSource``. Yet, most of
the time when trying to compare two tables, it is more convenient to create a ``BetweenRequirement`` and use
the ``from_tables`` constructor.


Column capitalization
---------------------

Different database management systems handle capitalization of entities, such as column names, differently.
For the time being:
- Mssql: ``datajudge`` expects column name capitalization as is seen in database, either lowercase or uppercase.
- Postgres: ``datajudge`` expects lowercase column names.
- Snowflake: ``datajudge`` will lowercase independently of the capitalization provided.

The snowflake behavior is due to an upstream `bug <https://github.com/snowflakedb/snowflake-sqlalchemy/issues/157>`_
in `` snowflake-sqlalchemy``.

This behavior is subject to change.

