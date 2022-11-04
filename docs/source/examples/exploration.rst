Example: Exploration
====================


While datajudge seeks to tackle the use case of expressing and evaluating tests against
data, its fairly generic inner workings allow for using it in a rather explorative
workflow as well.

Let's first clarify terminology by exemplifying both scenarios. A person wishing to test
data might ask the question

    Has the number of rows not grown too much from version 1 of the table to version 2
    of the table?

whereas a person wishing to explore the data might ask the question

    By how much has the number of rows grown from version 1 to version 2 of the table?

Put differently, a test typically revolves around a binary outcome while an exploration
usually doesn't.

In the following we will attempt to illustrate possible usages of datajudge for
exploration by walking through two simple examples.

These examples rely on some insight about how most datajudge ``Constraint`` s work under
the hood. Importantly, ``Constraint`` s typically come with

* a ``retrieve`` method: this method fetches relevant data from database, given a
  ``DataReference``
* a ``get_factual_value`` method: this is typically a wrapper around ``retrieve`` for the
  first ``DataReference`` of the given ``Requirement`` / ``Constraint``
* a ``get_target_value`` method: this is either a wrapper around ``retrieve`` for the second
  ``DataRefence`` in the case
  of a ``BetweenRequirement`` or an echoing of the ``Constraint`` s key reference value in
  the case of a ``WithinRequirement``

Moreover, as is the case when using datajudge for testing purposes, these approaches rely on
a sqlalchemy engine. The latter is the gateway to the database at hand.

Example 1: Comparing numbers of rows
------------------------------------

Assume we have two tables in the same database called ``table1`` and ``table2``. Now we would
like to compare their numbers of rows. As a first step, we would like to retrieve the
respective numbers of rows. For that matter we create a ``BetweenTableRequirement`` including
both tables and add a ``NRowsEquality`` ``Constraint`` onto it.


.. code-block:: python

    import sqlalchemy as sa
    from datajudge import requirements, Condition

    engine = sa.create_engine(your_connection_string)
    req = requirements.BetweenRequirement.from_tables(
        db_name,
        schema_name,
        "table1",
        db_name,
        schema_name,
        "table2",
    )
    req.add_n_rows_equality_constraint()
    n_rows1 = req.get_factual_value(engine)
    n_rows2 = req.get_target_value(engine)


Once retrieved, we can compare them as we wish by e.g. computing the absolute and relative
growth (or loss) from ``table1`` to ``table2``:

.. code-block:: python

    absolute_change = abs(n_rows2 - n_rows1)
    relative_change = (absolute_change) / n_rows1 if n_rows1 != 0 else None


Importantly, many datajudge staples, such as ``Condition`` s can be used, too. We shall see
this in our next example.

Example 2: Investigating unique values
--------------------------------------

In this example we will suppose that there is a table called "table" which has several
columns. Two of its columns are called ``col_int`` and ``col_varchar``. We are now interested
in the unique values in these two columns combined. Put differently, we are wondering:

    Which pairs of values in ``col_int`` and ``col_varchar`` have we encountered?

To add to the mix, we will moreover only be interested in tuples in which ``col_int`` has a
value of larger than 10.

As before, we will start off by creating a ``Requirement``. Since we are only dealing with
a single table this time, we will create a ``WithinRequirement``.


.. code-block:: python

    import sqlalchemy as sa
    from datajudge import requirements, Condition

    engine = sa.create_engine(your_connection_string)

    req = requirements.WithinRequirement.from_table(
        db_name,
	schema_name,
	"table",
    )
    condition = Condition(raw_string="col_int >= 10")
    req.add_uniques_equality_constraint(
        columns=["col_int", "col_varchar"],
	uniques=["hello world"], # This is really just a placeholder.
        condition=condition,
    )
    constraint = req[0]
    uniques = constraint.get_factual_value(engine)


If one was to investigate this ``uniques`` variable further, one could, e.g. see the following:


.. code-block:: python

    ([(10, 'hi10'), (11, 'hi11'), (12, 'hi12'), (13, 'hi13'), (14, 'hi14'), (15, 'hi15'), (16, 'hi16'), (17, 'hi17'), (18, 'hi18'), (19, 'hi19')], [1, 100, 12, 1, 7, 8, 1, 1, 1337, 1])


This makes more sense when we investigate the underlying ``retrieve`` method of the
``UniquesEquality`` ``Constraint``: the first value of our tuple corresponds to the list
of unique pairs in columns ``col_int`` and ``col_varchar``. The second value of our tuple
are the respective counts thereof.

If now we were curious and would like to use the SQL queries under the hood to manually
customize the query, we could do that, too. In order to do so, we can use the fact that
``retrieve`` methods typically both return an actual result or value as well as the
sqlalchemy selections that led to it. We can use this selection and compile it to a
standard, textual SQL query.


.. code-block:: python

    values, selections = constraint.retrieve(engine, constraint.ref)
    print(str(selections[0].compile(engine, compile_kwargs={"literal_binds": True}))


In the case from above, this would return the following query:


.. code-block:: sql

    SELECT
        anon_1.col_int,
	anon_1.col_varchar,
	count(*) AS count_1
    FROM
        (SELECT
	    tempdb.dbo.table.col_int AS col_int,
	    tempdb.dbo.table.col_varchar AS col_varchar
        FROM
	    tempdb.dbo.table WITH (NOLOCK)
        WHERE col_int >= 10) AS anon_1
    GROUP BY anon_1.col_int, anon_1.col_varchar
