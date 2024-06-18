Example: Dates
==============

This example concerns itself with expressing ``Constraint``\s against data revolving
around dates. While date ``Constraint``\s between tables exist, we will only illustrate
``Constraint``\s on a single table and reference values here. As a consequence, we will
only use ``WithinRequirement``, as opposed to ``BetweenRequirement``.

Concretely, we will assume a table containing prices for a given product of id 1.
Importantly, these prices are valid for a certain date range only. More precisely,
we assume that the price for a product - identified via the ``preduct_id`` column
- is indicated in the ``price`` column, the date from which it is valid - the date
itself included - in ``date_from`` and the the until when it is valid - the date
itself included - in the ``date_to`` column.

Such a table might look as follows:

.. list-table:: prices
   :header-rows: 1

   * - product_id
     - price
     - date_from
     - date_to
   * - 1
     - 13.99
     - 22/01/01
     - 22/01/10
   * - 1
     - 14.5
     - 22/01/11
     - 22/01/17
   * - 1
     - 13.37
     - 22/01/16
     - 22/01/31

Given this table, we would like to ensure - for the sake of illustrational purposes -
that 6 constraints are satisfied:

1. All values from column ``date_from`` should be in January 2022.
2. All values from column ``date_to`` should be in January 2022.
3. The minimum value in column ``date_from`` should be the first of January 2022.
4. The maximum value in column ``date_to`` should be the 31st of January 2022.
5. There is no gap between ``date_from`` and ``date_to``. In other words, every date
   of January has to be assigned to at least one row for a given product.
6. There is no overlap between ``date_from`` and ``date_to``. In other words, every
   date of January has to be assigned to at most one row for a given product.


Assuming that such a table exists in database, we can write a specification against it.

.. code-block:: python

    import pytest
    import sqlalchemy as sa

    from datajudge import WithinRequirement
    from datajudge.pytest_integration import collect_data_tests

    # We create a Requirement, within a table. This object will contain
    # all the constraints we want to test on the specified table.
    # To test another table or test the same table against another table,
    # we would create another Requirement object.
    prices_req = WithinRequirement.from_table(
        db_name="example", schema_name=None, table_name="prices"
    )

    # Constraint 1:
    # All values from column date_from should be in January 2022.
    prices_req.add_date_between_constraint(
        column="date_from",
	lower_bound="'20220101'",
	upper_bound="'20220131'",
	# We don't tolerate any violations of the constraint:
	min_fraction=1,
    )

    # Constraint 2:
    # All values from column date_to should be in January 2022.
    prices_req.add_date_between_constraint(
        column="date_to",
	lower_bound="'20220101'",
	upper_bound="'20220131'",
	# We don't tolerate any violations of the constraint:
	min_fraction=1,
    )

    # Constraint 3:
    # The minimum value in column date_from should be the first of January 2022.

    # Ensure that the minimum is smaller or equal the reference value min_value.
    prices_req.add_date_min_constraint(column="date_from", min_value="'20220101'")
    # Ensure that the minimum is greater or equal the reference value min_value.
    prices_req.add_date_min_constraint(
        column="date_from",
	min_value="'20220101'",
	use_upper_bound_reference=True,
    )

    # Constraint 4:
    # The maximum value in column date_to should be the 31st of January 2022.

    # Ensure that the maximum is greater or equal the reference value max_value.
    prices_req.add_date_max_constraint(column="date_to", max_value="'20220131'")
    # Ensure that the maximum is smaller or equal the reference value max_value.
    prices_req.add_date_max_constraint(
        column="date_to",
	max_value="'20220131'",
	use_upper_bound_reference=True,
    )

    # Constraint 5:
    # There is no gap between date_from and date_to. In other words, every date
    # of January has to be assigned to at least one row for a given product.
    prices_req.add_date_no_gap_constraint(
        start_column="date_from",
	end_column="date_to",
	# We don't want a gap of price date ranges for a given product.
	# For different products, we allow arbitrary date gaps.
	key_columns=["product_id"],
	# As indicated in prose, date_from and date_to are included in ranges.
	end_included=True,
	# Again, we don't expect any violations of our constraint.
	max_relative_violations=0,
    )

    # Constraint 6:
    # There is no overlap between date_from and date_to. In other words, every
    # of January has to be assigned to at most one row for a given product.
    princes_req.add_date_no_overlap_constraint(
        start_column="date_from",
	end_column="date_to",
	# We want no overlap of price date ranges for a given product.
	# For different products, we allow arbitrary date overlaps.
	key_columns=["product_id"],
	# As indicated in prose, date_from and date_to are included in ranges.
	end_included=True,
	# Again, we don't expect any violations of our constraint.
	max_relative_violations=0,
    )

    @pytest.fixture()
    def datajudge_engine():
	# TODO: Insert actual connection string
        return sa.create_engine("your_db://")

    # We gather our single Requirement in a list.
    requirements = [prices_req]

    # "collect_data_tests" takes all requirements and turns their respective
    # Constraints into individual tests. pytest will be able to pick
    # up these tests.
    test_constraint = collect_data_tests(requirements)

Please note that the ``DateNoOverlap`` and ``DateNoGap`` constraints also exist
in a slightly different form: ``DateNoOverlap2d`` and ``DateNoGap2d``.
As the names suggest, these can operate in 'two date dimensions'.

For example, let's assume a table with four date columns, representing two
ranges in distinct dimensions, respectively:

* ``date_from``: Date from when a price is valid
* ``date_to``: Date until when a price is valid
* ``date_definition_from``: Date when a price definition was inserted
* ``date_definition_to``: Date until when a price definition was used

Analogously to the unidimensional scenario illustrated here, one might care
for certain constraints in two dimensions.
