Example: Dumps of Twitch data
=============================

This example is based on data capturing statistics and properties of popular Twitch channels.
The setup is such that we have two data sets 'of the same kind' but from different points in time.

In other words, a 'version' of the data set represents a temporal notion.
For example, version 1 might stem from end of March and version 2 from end of April.
Moreover, we will assume that the first, version 1, has been vetted and approved with the
help of manual investigation and domain knowledge. The second data set, version 2, has just been
made available. We would like to use it but can't be sure of its validity just yet. As a consequence
we would like to assess the quality of the data in version 2.

In order to have a database Postgres instance to begin with, it might be useful to use our
`script <https://github.com/Quantco/datajudge/blob/main/start_postgres.sh>`_, spinning up
a dockerized Postgres database:

.. code-block:: console

  $ ./start_postgres.sh


The original data set can be found on `kaggle <https://www.kaggle.com/datasets/aayushmishra1512/twitchdata>`_.
For the sake of this tutorial, we slightly process it and provide two versions of it.
One can either recreate this by executing this
`processing script <https://github.com/Quantco/datajudge/tree/main/docs/source/examples/twitch_process.py>`_
oneself on the original data or download our processed files (
`version 1 <https://github.com/Quantco/datajudge/tree/main/docs/source/examples/twitch_version1.csv>`_
and
`version 2 <https://github.com/Quantco/datajudge/tree/main/docs/source/examples/twitch_version2.csv>`_)
right away.

Once both version of the data exist, they can be uploaded to the tabase. We provide an
`uploading script <https://github.com/Quantco/datajudge/tree/main/docs/source/examples/twitch_upload.py>`_
creating and populating one table per version of the data in a Postgres database. It resembles the
following:

.. code-block:: python

    address = os.environ.get("DB_ADDR", "localhost")
    connection_string = f"postgresql://datajudge:datajudge@{address}:5432/datajudge"
    engine = sa.create_engine(connection_string)
    df_v2.to_sql("twitch_v2", engine, schema="public", if_exists="replace")
    df_v1.to_sql("twitch_v1", engine, schema="public", if_exists="replace")


Once the tables are stored in a database, we can actually write a ``datajudge``
specification against them. But first, we'll have a look at what the data roughly
looks like by investigating a random sample of four rows:

.. list-table:: A sample of the data
   :header-rows: 1

   * - channel
     - watch_time
     - stream_time
     - peak_viewers
     - average_viewers
     - followers
     - followers_gained
     - views_gained
     - partnered
     - mature
     - language
   * - xQcOW
     - 6196161750
     - 215250
     - 222720
     - 27716
     - 3246298
     - 1734810
     - 93036735
     - True
     - False
     - English
   * - summit1g
     - 6091677300
     - 211845
     - 310998
     - 25610
     - 5310163
     - 1374810
     - 89705964
     - True
     - False
     - English
   * - Gaules
     - 5644590915
     - 515280
     - 387315
     - 10976
     - 1767635
     - 1023779
     - 102611607
     - True
     - True
     - Portuguese
   * - ESL_CSGO
     - 3970318140
     - 517740
     - 300575
     - 7714
     - 3944850
     - 703986
     - 106546942
     - True
     - False
     - English

Note that we expect both version 1 and version 2 to follow this structure. Due to them
being assembled at different points in time, merely their rows shows differ.


Now let's write an actual specification, expressing our expectations against the data.
First, we need to make sure a connection to the database can be established at test execution
time. How this is done exactly depends on how you set up your database. When using our
default setup with running, this would look as follows:

.. code-block:: python

    import os
    import pytest
    import sqlalchemy as sa


    @pytest.fixture(scope="module")
    def datajudge_engine():
        address = os.environ.get("DB_ADDR", "localhost")
        connection_string = f"postgresql://datajudge:datajudge@{address}:5432/datajudge"
        return sa.create_engine(connection_string)

Once a way to connect to the database is defined, we want to declare our data sources and
express expectations against them. In this example, we have two tables in the same database -
one table per version of the Twitch data.


Yet, let's start with a straightforward example only using version 2. We want to use our
domain knowledge that constrains the values of the ``language`` column only to contain letters
and have a length strictly larger than 0.


.. code-block:: python

    from datajudge import WithinRequirement


    # Postgres' default database.
    db_name = "tempdb"
    # Postgres' default schema.
    schema_name = "public"

    within_requirement = WithinRequirement.from_table(
        table_name="twitch_v2",
        schema_name=schema_name,
	db_name=db_name,
    )
    within_requirement.add_varchar_regex_constraint(
	column="language",
	regex="^[a-zA-Z]+$",
    )


Done! Now onto comparisons between the table representing the approved version 1 of the
data and the to be assessed version 2 of the data.

.. code-block:: python

    from datajudge import BetweenRequirement, Condition

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


Now having compared the 'same kind of data' between version 1 and version 2,
we may as well compare 'different kind of data' within version 2, as a means of
a sanity check. This sanity check consists of checking whether the mean
``average_viewer`` value of mature channels should deviate at most 10% from
the overall mean.

.. code-block:: python

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


Lastly, we need to collect all of our requirements in a list and make sure
``pytest`` can find them by calling ``collect_data_tests``.


.. code-block:: python

    from datajudge.pytest_integration import collect_data_tests
    requirements = [
	within_requirement,
	between_requirement_version,
	between_requirement_columns,
    ]
    test_func = collect_data_tests(requirements)

If we then test these expectations against the data by running
``$ pytest specification.py`` -- where ``specification.py``
contains all of the code outlined before (you can find it
`here <https://github.com/Quantco/datajudge/tree/main/docs/source/examples/twitch_specification.py>`_ )
-- we see that the new version of the data is
not quite on par with what we'd expect:

.. code-block:: console

    $ pytest twitch_specification.py
    ================================== test session starts ===================================
    platform darwin -- Python 3.10.5, pytest-7.1.2, pluggy-1.0.0
    rootdir: /Users/kevin/Code/datajudge/docs/source/examples
    plugins: html-3.1.1, cov-3.0.0, metadata-2.0.2
    collected 8 items

    twitch_specification.py F.....FF                                                   [100%]

    ======================================== FAILURES ========================================
    ____________________ test_func[VarCharRegex::tempdb.public.twitch_v2] ____________________

    constraint = <datajudge.constraints.varchar.VarCharRegex object at 0x10855da20>
    datajudge_engine = Engine(postgresql://datajudge:***@localhost:5432/datajudge)

	@pytest.mark.parametrize(
	    "constraint", all_constraints, ids=Constraint.get_description
	)
	def test_constraint(constraint, datajudge_engine):
	    test_result = constraint.test(datajudge_engine)
    >       assert test_result.outcome, test_result.failure_message
    E       AssertionError: tempdb.public.twitch_v2's column(s) 'language' breaks regex
            '^[a-zA-Z]+$' in 0.045454545454545456 > 0.0 of the cases. In absolute terms, 1
	    of the 22 samples violated the regex. Some counterexamples consist of the
	    following: ['Sw3d1zh'].

    ../../../src/datajudge/pytest_integration.py:25: AssertionError
    ____________ test_func[UniquesEquality::public.twitch_v1 | public.twitch_v2] _____________

    constraint = <datajudge.constraints.uniques.UniquesEquality object at 0x10855d270>
    datajudge_engine = Engine(postgresql://datajudge:***@localhost:5432/datajudge)

	@pytest.mark.parametrize(
	    "constraint", all_constraints, ids=Constraint.get_description
	)
	def test_constraint(constraint, datajudge_engine):
	    test_result = constraint.test(datajudge_engine)
    >       assert test_result.outcome, test_result.failure_message
    E       AssertionError: tempdb.public.twitch_v1's column(s) 'language' doesn't have
            the element(s) '{'Sw3d1zh'}' when compared with the reference values.

    ../../../src/datajudge/pytest_integration.py:25: AssertionError
    ______________ test_func[NumericMean::public.twitch_v2 | public.twitch_v2] _______________

    constraint = <datajudge.constraints.numeric.NumericMean object at 0x1084e1810>
    datajudge_engine = Engine(postgresql://datajudge:***@localhost:5432/datajudge)

	@pytest.mark.parametrize(
	    "constraint", all_constraints, ids=Constraint.get_description
	)
	def test_constraint(constraint, datajudge_engine):
	    test_result = constraint.test(datajudge_engine)
    >       assert test_result.outcome, test_result.failure_message
    E       AssertionError: tempdb.public.twitch_v2's column(s) 'average_viewers' has
            mean 4734.9780000000000000, deviating more than 0.1 from
	    tempdb.public.twitch_v2's column(s) 'average_viewers''s
	    3599.9826086956521739. Condition on second table: WHERE mature IS TRUE

    ../../../src/datajudge/pytest_integration.py:25: AssertionError
    ================================ short test summary info =================================
    FAILED twitch_specification.py::test_func[VarCharRegex::tempdb.public.twitch_v2] - Asse...
    FAILED twitch_specification.py::test_func[UniquesEquality::public.twitch_v1 | public.twitch_v2]
    FAILED twitch_specification.py::test_func[NumericMean::public.twitch_v2 | public.twitch_v2]
    ============================== 3 failed, 5 passed in 1.52s ===============================

Alternatively, you can also look at these test results in
`this html report <https://github.com/Quantco/datajudge/tree/main/docs/source/examples/twitch_report.html>`_
generated by
`pytest-html <https://github.com/pytest-dev/pytest-html>`_.

Hence we see that we might not want to blindly trust version 2 of the data as is. Rather, we might need
to investigate what is wrong with the data, what this has been caused by and how to fix it.

Concretely, what exactly do we learn from the error messages?

* The column ``language`` now has a row with value ``'Sw3d1zh'``. This break two of our
  constraints. The ``VarCharRegex`` constraint compared the columns' values to a regular
  expression. The ``UniquesEquality`` constraint expected the unique values of the
  ``language`` column to not have changed between version 1 and version 2.
* The mean value of ``average_viewers`` of ``mature`` channels is substantially - more
  than our 10% tolerance - lower than the global mean.
