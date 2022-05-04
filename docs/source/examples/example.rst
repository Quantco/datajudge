Example
=======


To get started, we will create a sample database using sqlite that contains a list of companies.

The table "companies_archive" contains three entries:

.. list-table:: companies_archive
   :header-rows: 1

   * - id
     - name
     - num_employees
   * - 1
     - QuantCo
     - 90
   * - 2
     - Google
     - 140,000
   * - 3
     - BMW
     - 110,000

While "companies" contains an additional entry:

.. list-table:: companies
   :header-rows: 1

   * - id
     - name
     - num_employees
   * - 1
     - QuantCo
     - 100
   * - 2
     - Google
     - 150,000
   * - 3
     - BMW
     - 120,000
   * - 4
     - Apple
     - 145,000


.. code-block:: python

    import sqlalchemy as sa

    eng = sa.create_engine('sqlite:///example.db')

    with eng.connect() as con:
        con.execute("CREATE TABLE companies (id INTEGER PRIMARY KEY, name TEXT, num_employees INTEGER)")
        con.execute("INSERT INTO companies (name, num_employees) VALUES ('QuantCo', 100), ('Google', 150000), ('BMW', 120000), ('Apple', 145000)")
        con.execute("CREATE TABLE companies_archive (id INTEGER PRIMARY KEY, name TEXT, num_employees INTEGER)")
        con.execute("INSERT INTO companies_archive (name, num_employees) VALUES ('QuantCo', 90), ('Google', 140000), ('BMW', 110000)")


As an example, we will run 4 tests on this table:

1. Does the table "companies" contain a column named "name"?
2. Does the table "companies" contain at least 1 entry with the name "QuantCo"?
3. Does the column "num_employees" of the "companies" table have all positive values?
4. Does the column "name" of the table "companies" contain at least all the values of
   the corresponding column in "companies_archive"?

.. code-block:: python

    import pytest
    import sqlalchemy as sa

    from datajudge import (
        Condition,
        WithinRequirement,
        BetweenRequirement,
    )
    from datajudge.pytest_integration import collect_data_tests


    # We create a Requirement, within a table. This object will contain
    # all the constraints we want to test on the specified table.
    # To test another table or test the same table against another table,
    # we would create another Requirement object.
    companies_req = WithinRequirement.from_table(
        db_name="example", schema_name=None, table_name="companies"
    )

    # Constraint 1.: Does the table "companies" contain a column named "name"?
    companies_req.add_column_existence_constraint(columns=["name"])

    # Constraint 2.: Does the table "companies" contain at least 1 entry with the name "QuantCo"?
    condition = Condition(raw_string="name = 'QuantCo'")
    companies_req.add_n_rows_min_constraint(n_rows_min=1, condition=condition)

    # Constraint 3.: Does the column "num_employees" of the "companies" table have all positive values?
    companies_req.add_numeric_min_constraint(column="num_employees", min_value=1)

    # We create a new Requirement, this time between different tables.
    # Concretely, we intent to test constraints between the table "companies"
    # and the table "companies_archive".
    companies_between_req = BetweenRequirement.from_tables(
        db_name1="example",
        schema_name1=None,
        table_name1="companies",
        db_name2="example",
        schema_name2=None,
        table_name2="companies_archive",
    )

    # Constraint 4.: Does the column "name" of the table "companies" contain at least all the values of the corresponding column in "companies_archive"?
    companies_between_req.add_row_superset_constraint(
        columns1=['name'], columns2=['name'], constant_max_missing_fraction=0
    )

    # collect_data_tests expects a pytest fixture with the name
    # "datajudge_engine" that is a SQLAlchemy engine

    @pytest.fixture()
    def datajudge_engine():
        return sa.create_engine("sqlite:///example.db")

    # We gather our distinct Requirements in a list.
    requirements = [companies_req, companies_between_req]

    # "collect_data_tests" takes all requirements and turns their respective
    # Constraints into individual tests. pytest will be able to pick
    # up these tests.
    test_constraint = collect_data_tests(requirements)


Saving this file as ``specification.py`` and calling ``pytest specification.py -v``
will verify that all constaints are satisfied. The output you see in the terminal
should be similar to this:

.. code-block::

    =================================== test session starts ===================================
    ...
    collected 4 items

    specification.py::test_constraint[ColumnExistence::companies] PASSED                [ 25%]
    specification.py::test_constraint[NRowsMin::companies] PASSED                       [ 50%]
    specification.py::test_constraint[NumericMin::companies] PASSED                     [ 75%]
    specification.py::test_constraint[RowSuperset::companies|companies_archive] PASSED  [100%]

    ==================================== 4 passed in 0.31s ====================================

You can also use a formatted html report using the ``--html=report.html`` flag.
