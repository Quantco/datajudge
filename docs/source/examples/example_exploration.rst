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
exploration by looking at three simple examples.

These examples rely on some insight about how most datajudge ``Constraint`` s work under
the hood. Importantly, ``Constraint`` s typically come with

* a ``retrieve`` method: this method fetches relevant data from database, given a
  ``DataReference``
* a ``get_factual_value`` method: this is typically a wrapper around ``retrieve`` for the
  first ``DataReference`` of the given ``Requirement`` / ``Constraint``
* a ``get_target_value`` method: this is either a wrapper around ``retrieve`` for the
  second ``DataReference`` in the case of a ``BetweenRequirement`` or an echoing of the
  ``Constraint`` s key reference value in the case of a ``WithinRequirement``

Moreover, as is the case when using datajudge for testing purposes, these approaches rely
on a `sqlalchemy engine <https://docs.sqlalchemy.org/en/14/core/connections.html>`_. The
latter is the gateway to the database at hand.

Example 1: Comparing numbers of rows
------------------------------------

Assume we have two tables in the same database called ``table1`` and ``table2``. Now we
would like to compare their numbers of rows. Naturally, we would like to retrieve
the respective numbers of rows before we can compare them. For this purpose we create
a ``BetweenTableRequirement`` referring to both tables and add a ``NRowsEquality``
``Constraint`` onto it.


.. code-block:: python

    import sqlalchemy as sa
    from datajudge import BetweenRequirement

    engine = sa.create_engine(your_connection_string)
    req = BetweenRequirement.from_tables(
        db_name,
        schema_name,
        "table1",
        db_name,
        schema_name,
        "table2",
    )
    req.add_n_rows_equality_constraint()
    n_rows1 = req[0].get_factual_value(engine)
    n_rows2 = req[0].get_target_value(engine)


Note that here, we access the first (and only) ``Constraint`` that has been added to the
``BetweenRequirement`` by writing ``req[0]``. ``Requirements`` are are sequences of
``Constraint`` s, after all.

Once the numbers of rows are retrieved, we can compare them as we wish. For instance, we
could compute the absolute and relative growth (or loss) of numbers of rows from
``table1`` to ``table2``:

.. code-block:: python

    absolute_change = abs(n_rows2 - n_rows1)
    relative_change = (absolute_change) / n_rows1 if n_rows1 != 0 else None


Importantly, many datajudge staples, such as ``Condition`` s can be used, too. We shall see
this in our next example.

Example 2: Investigating unique values
--------------------------------------

In this example we will suppose that there is a table called ``table`` consisting of
several columns. Two of its columns are supposed to be called ``col_int`` and
``col_varchar``. We are now interested in the unique values in these two columns combined.
Put differently, we are wondering:

    Which unique pairs of values in ``col_int`` and ``col_varchar`` have we encountered?

To add to the mix, we will moreover only be interested in tuples in which ``col_int`` has a
value of larger than 10.

As before, we will start off by creating a ``Requirement``. Since we are only dealing with
a single table this time, we will create a ``WithinRequirement``.


.. code-block:: python

    import sqlalchemy as sa
    from datajudge import WithinRequirement, Condition

    engine = sa.create_engine(your_connection_string)

    req = requirements.WithinRequirement.from_table(
        db_name,
	schema_name,
	"table",
    )

    condition = Condition(raw_string="col_int >= 10")

    req.add_uniques_equality_constraint(
        columns=["col_int", "col_varchar"],
	uniques=[], # This is really just a placeholder.
        condition=condition,
    )
    uniques = req[0].get_factual_value(engine)


If one was to investigate this ``uniques`` variable further, one could, e.g. see the
following:


.. code-block:: python

    ([(10, 'hi10'), (11, 'hi11'), (12, 'hi12'), (13, 'hi13'), (14, 'hi14'), (15, 'hi15'), (16, 'hi16'), (17, 'hi17'), (18, 'hi18'), (19, 'hi19')], [1, 100, 12, 1, 7, 8, 1, 1, 1337, 1])


This becomes easier to parse when inspecting the underlying ``retrieve`` method of the
``UniquesEquality`` ``Constraint``: the first value of the tuple corresponds to the list
of unique pairs in columns ``col_int`` and ``col_varchar``. The second value of the tuple
are the respective counts thereof.

Moreoever, one could manually customize the underlying SQL query. In order to do so, one
can use the fact that ``retrieve`` methods typically return an actual result or value
as well as the sqlalchemy selections that led to said result or value. We can use these
selections and compile them to a standard, textual SQL query:


.. code-block:: python

    values, selections = req[0].retrieve(engine, constraint.ref)
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


Example 3: Comparing column structure
-------------------------------------

While we often care about value tuples of given columns, i.e. rows, it can also provide
meaningful insights to compare the column structure of two tables. In particular, we
might want to compare whether columns of one table are a subset or superset of another
table. Moreover, for columns present in both tables, we'd like to learn about their
respective types.

In order to illustrate such an example, we will again assume that there are two tables
called ``table1`` and ``table2``, irrespective of prior examples.

We can now create a ``BetweenRequirement`` for these two tables and use the
``ColumnSubset`` ``Constraint``. As before, we will rely on the ``get_factual_value``
method to retrieve the values of interest for the first table passed to the
``BetweenRequirement`` and the ``get_target_value`` method for the second table passed
to the ``BetweenRequirement``.

.. code-block:: python

    import sqlalchemy as sa
    from datajudge import BetweenRequirement

    engine = sa.create_engine(your_connection_string)

    req = BetweenRequirement.from_tables(
        db_name,
        schema_name,
        "table1",
        db_name,
        schema_name,
        "table2",
    )

    req.add_column_subset_constraint()

    columns1 = req[0].get_factual_value(engine)
    columns2 = req[0].get_target_value(engine)

    print(f"Columns present in both: {set(columns1) & set(columns2)}")
    print(f"Columns present in only table1: {set(columns1) - set(columns2)}")
    print(f"Columns present in only table2: {set(columns2) - set(columns1)}")


This could, for instance result in the following printout:

.. code-block::

    Columns present in both: {'col_varchar', 'col_int'}
    Columns present in only table1: set()
    Columns present in only table2: {'col_date'}


Now, we can investigate the types of the columns present in both tables:


.. code-block:: python

    for column in set(columns1) & set(columns2):
        req.add_column_type_constraint(column1=column, column2=column)
	type1 = req[0].get_factual_value(engine)
	type2 = req[0].get_target_value(engine)
	print(f"Column '{column}' has type '{type1}' in table1 and type '{type2}' in table2.")


Depending on the underlying database management system and data, the output of this
could for instance be:


.. code-block::

   Column 'col_varchar' has type 'varchar' in table1 and type 'varchar' in table2.
   Column 'col_int' has type 'integer' in table1 and type 'integer' in table2.
