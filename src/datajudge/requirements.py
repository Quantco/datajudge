from abc import ABC
from collections.abc import MutableSequence
from typing import Callable, Collection, List, Optional, Sequence, TypeVar

import sqlalchemy as sa

from .constraints import column as column_constraints
from .constraints import date as date_constraints
from .constraints import groupby as groupby_constraints
from .constraints import miscs as miscs_constraints
from .constraints import nrows as nrows_constraints
from .constraints import numeric as numeric_constraints
from .constraints import row as row_constraints
from .constraints import uniques as uniques_constraints
from .constraints import varchar as varchar_constraints
from .constraints.base import Constraint, TestResult
from .db_access import (
    Condition,
    DataReference,
    DataSource,
    ExpressionDataSource,
    RawQueryDataSource,
    TableDataSource,
    get_date_growth_rate,
)

T = TypeVar("T")


class TableQualifier:
    def __init__(self, db_name: str, schema_name: str, table_name: str):
        self.db_name = db_name
        self.schema_name = schema_name
        self.table_name = table_name

    def get_within_requirement(self):
        return WithinRequirement.from_table(
            db_name=self.db_name,
            schema_name=self.schema_name,
            table_name=self.table_name,
        )

    def get_between_requirement(self, table_qualifier):
        return BetweenRequirement.from_tables(
            db_name1=self.db_name,
            schema_name1=self.schema_name,
            table_name1=self.table_name,
            db_name2=table_qualifier.db_name,
            schema_name2=table_qualifier.schema_name,
            table_name2=table_qualifier.table_name,
        )


class Requirement(ABC, MutableSequence):
    def __init__(self):
        self._constraints: List[Constraint] = []
        self.data_source: DataSource

    def insert(self, index: int, value: Constraint) -> None:
        self._constraints.insert(index, value)

    def __getitem__(self, i):
        return self._constraints[i]

    def __setitem__(self, i, o) -> None:
        self._constraints[i] = o

    def __delitem__(self, i) -> None:
        del self._constraints[i]

    def __len__(self) -> int:
        return len(self._constraints)

    def test(self, engine) -> List[TestResult]:
        return [constraint.test(engine) for constraint in self]


class WithinRequirement(Requirement):
    def __init__(self, data_source: DataSource):
        self.data_source = data_source
        super().__init__()

    @classmethod
    def from_table(cls, db_name: str, schema_name: str, table_name: str):
        return cls(
            data_source=TableDataSource(
                db_name=db_name, schema_name=schema_name, table_name=table_name
            )
        )

    @classmethod
    def from_raw_query(cls, query: str, name: str, columns: List[str] = None):
        """Create a `WithinRequirement` based on a raw query string.

        The `query` parameter can be passed any query string returning rows, e.g.
        `"SELECT * FROM myschema.mytable LIMIT 1337"` or
        `"SELECT id, name FROM table1 UNION SELECT id, name FROM table2"`.

        The `name` will be used to represent this query in error messages.

        If constraints rely on specific columns, these should be provided here via
        `columns`, e.g. `["id", "name"]`.
        """
        return cls(data_source=RawQueryDataSource(query, name, columns=columns))

    @classmethod
    def from_expression(cls, expression: sa.sql.expression.FromClause, name: str):
        """Create a `WithinRequirement` based on a sqlalchemy expression.

        Any sqlalchemy object implementing the `alias` method can be passed as an
        argument for the `expression` parameter. This could, e.g. be an
        `sqlalchemy.Table` object or the result of a `sqlalchemy.select` call.

        The `name` will be used to represent this expression in error messages.
        """
        return cls(data_source=ExpressionDataSource(expression, name))

    def add_column_existence_constraint(self, columns: List[str]):
        # Note that columns are not meant to be part of the reference.
        ref = DataReference(self.data_source)
        self._constraints.append(column_constraints.ColumnExistence(ref, columns))

    def add_primary_key_definition_constraint(self, primary_keys: List[str]):
        """Primary keys of exactly equal to given column names in the database."""
        ref = DataReference(self.data_source)
        self._constraints.append(
            miscs_constraints.PrimaryKeyDefinition(ref, primary_keys)
        )

    def add_uniqueness_constraint(
        self,
        columns: List[str],
        max_duplicate_fraction: float = 0,
        condition: Condition = None,
        max_absolute_n_duplicates: int = 0,
    ):
        """Columns should uniquely identify row.

        Given a set of columns, satisfy conditions of a primary key, i.e.
        uniqueness of tuples from said columns. This constraint has a tolerance
        for inconsistencies, expressed via max_duplicate_fraction. The latter
        suggests that the number of uniques from said colums is larger or equal
        to (1 - max_duplicate_fraction) the number of rows.

        """
        ref = DataReference(self.data_source, columns, condition)
        self._constraints.append(
            miscs_constraints.Uniqueness(
                ref,
                max_duplicate_fraction=max_duplicate_fraction,
                max_absolute_n_duplicates=max_absolute_n_duplicates,
            )
        )

    def add_column_type_constraint(self, column: str, column_type: str):
        ref = DataReference(self.data_source, [column])
        self._constraints.append(column_constraints.ColumnType(ref, column_type))

    def add_null_absence_constraint(self, column: str, condition: Condition = None):
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(miscs_constraints.NullAbsence(ref))

    def add_n_rows_equality_constraint(self, n_rows: int, condition: Condition = None):
        ref = DataReference(self.data_source, None, condition)
        self._constraints.append(nrows_constraints.NRowsEquality(ref, n_rows=n_rows))

    def add_n_rows_min_constraint(self, n_rows_min: int, condition: Condition = None):
        ref = DataReference(self.data_source, None, condition)
        self._constraints.append(nrows_constraints.NRowsMin(ref, n_rows=n_rows_min))

    def add_n_rows_max_constraint(self, n_rows_max: int, condition: Condition = None):
        ref = DataReference(self.data_source, None, condition)
        self._constraints.append(nrows_constraints.NRowsMax(ref, n_rows=n_rows_max))

    def add_uniques_equality_constraint(
        self,
        columns: List[str],
        uniques: Collection[T],
        map_func: Callable[[T], T] = None,
        reduce_func: Callable[[Collection], Collection] = None,
        condition: Condition = None,
    ):
        """Check if the data's unique values are equal to a given set of values.

        The `UniquesEquality` constraint asserts if the values contained in a column
        of a `DataSource` are strictly the ones of a reference set of expected values,
        specified via the `uniques` parameter.

        See the `Uniques` class for further parameter details on `map_func` and
        `reduce_func`.
        """

        ref = DataReference(self.data_source, columns, condition)
        self._constraints.append(
            uniques_constraints.UniquesEquality(
                ref, uniques=uniques, map_func=map_func, reduce_func=reduce_func
            )
        )

    def add_uniques_superset_constraint(
        self,
        columns: List[str],
        uniques: Collection[T],
        max_relative_violations: float = 0,
        map_func: Callable[[T], T] = None,
        reduce_func: Callable[[Collection], Collection] = None,
        condition: Condition = None,
    ):
        """Check if unique calues of columns are contained in the reference data.

        The `UniquesSuperset` constraint asserts that reference set of expected values,
        specified via `uniques`, is contained in given columns of a `DataSource`.

        Null values in the column are ignored. To assert the non-existence of them use
        the `NullAbsence` constraint via the `add_null_absence_constraint` helper method
        for `WithinRequirement`.

        `max_relative_violations` indicates what fraction of unique values of the given
        `DataSource` are not represented in the reference set of unique values. Please
        note that `UniquesSubset` and `UniquesSuperset` are not symmetrical in this regard.

        One use of this constraint is to test for consistency in columns with expected
        categorical values.

        See `Uniques` for further details on `map_func` and `reduce_func`.
        """

        ref = DataReference(self.data_source, columns, condition)
        self._constraints.append(
            uniques_constraints.UniquesSuperset(
                ref,
                uniques=uniques,
                max_relative_violations=max_relative_violations,
                map_func=map_func,
                reduce_func=reduce_func,
            )
        )

    def add_uniques_subset_constraint(
        self,
        columns: List[str],
        uniques: Collection[T],
        max_relative_violations: float = 0,
        map_func: Callable[[T], T] = None,
        reduce_func: Callable[[Collection], Collection] = None,
        condition: Condition = None,
    ):
        """Check if the data's unique values are contained in a given set of values.

        The `UniquesSubset` constraint asserts if the values contained in a column of
        a `DataSource` are part of a reference set of expected values, specified via
        `uniques`.

        Null values in the column are ignored. To assert the non-existence of them use
        the `NullAbsence` constraint via the `add_null_absence_constraint` helper method
        for `WithinRequirement`.

        `max_relative_violations` indicates what fraction of rows of the given table
        may have values not included in the reference set of unique values. Please note
        that `UniquesSubset` and `UniquesSuperset` are not symmetrical in this regard.

        See `Uniques` for further details on `map_func` and `reduce_func`.
        """

        ref = DataReference(self.data_source, columns, condition)
        self._constraints.append(
            uniques_constraints.UniquesSubset(
                ref,
                uniques=uniques,
                max_relative_violations=max_relative_violations,
                map_func=map_func,
                reduce_func=reduce_func,
            )
        )

    def add_n_uniques_equality_constraint(
        self,
        columns: Optional[List[str]],
        n_uniques: int,
        condition: Condition = None,
    ):
        ref = DataReference(self.data_source, columns, condition)
        self._constraints.append(
            uniques_constraints.NUniquesEquality(ref, n_uniques=n_uniques)
        )

    def add_numeric_min_constraint(
        self, column: str, min_value: float, condition: Condition = None
    ):
        """All values in column are greater or equal min_value."""
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            numeric_constraints.NumericMin(ref, min_value=min_value)
        )

    def add_numeric_max_constraint(
        self, column: str, max_value: float, condition: Condition = None
    ):
        """All values in column are less or equal max_value."""
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            numeric_constraints.NumericMax(ref, max_value=max_value)
        )

    def add_numeric_between_constraint(
        self,
        column: str,
        lower_bound: float,
        upper_bound: float,
        min_fraction: float,
        condition: Condition = None,
    ):
        """At least min_fraction of column's values are >= lower_bound and <= upper_bound."""
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            numeric_constraints.NumericBetween(
                ref, min_fraction, lower_bound, upper_bound
            )
        )

    def add_numeric_mean_constraint(
        self,
        column: str,
        mean_value: float,
        max_absolute_deviation: float,
        condition: Condition = None,
    ):
        """Assert the mean of the column deviates at most max_deviation from mean_value."""
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            numeric_constraints.NumericMean(
                ref, max_absolute_deviation, mean_value=mean_value
            )
        )

    def add_date_min_constraint(
        self,
        column: str,
        min_value: str,
        use_lower_bound_reference: bool = True,
        column_type: str = "date",
        condition: Condition = None,
    ):
        """Ensure all dates to be superior than min_value.

        Use string format: min_value="'20121230'".

        For valid column_type values, see get_format_from_column_type in constraints/base.py..

        If `use_lower_bound_reference`, the min of the first table has to be
        greater or equal to `min_value`.
        If not `use_upper_bound_reference`, the min of the first table has to
        be smaller or equal to `min_value`.
        """
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            date_constraints.DateMin(
                ref,
                min_value=min_value,
                use_lower_bound_reference=use_lower_bound_reference,
                column_type=column_type,
            )
        )

    def add_date_max_constraint(
        self,
        column: str,
        max_value: str,
        use_upper_bound_reference: bool = True,
        column_type: str = "date",
        condition: Condition = None,
    ):
        """Ensure all dates to be superior than max_value.

        Use string format: max_value="'20121230'".

        For valid column_type values, see get_format_from_column_type in constraints/base.py..

        If `use_upper_bound_reference`, the max of the first table has to be
        smaller or equal to `max_value`.
        If not `use_upper_bound_reference`, the max of the first table has to
        be greater or equal to `max_value`.
        """
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            date_constraints.DateMax(
                ref,
                max_value=max_value,
                use_upper_bound_reference=use_upper_bound_reference,
                column_type=column_type,
            )
        )

    def add_date_between_constraint(
        self,
        column: str,
        lower_bound: str,
        upper_bound: str,
        min_fraction: float,
        condition: Condition = None,
    ):
        """Use string format: lower_bound="'20121230'"."""
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            date_constraints.DateBetween(ref, min_fraction, lower_bound, upper_bound)
        )

    def add_date_no_overlap_constraint(
        self,
        start_column: str,
        end_column: str,
        key_columns: Optional[List[str]] = None,
        end_included: bool = True,
        max_relative_n_violations: float = 0,
        condition: Condition = None,
    ):
        """Constraint expressing that several date range rows may not overlap.

        The `DataSource` under inspection must consist of at least one but up
        to many `key_columns`, identifying an entity, a `start_column` and an
        `end_column`.

        For a given row in this `DataSource`, `start_column` and `end_column` indicate a
        date range. Neither of those columns should contain NULL values. Also, it
        should hold that for a given row, the value of `end_column` is strictly greater
        than the value of `start_column`.

        Note that the value of `start_column` is expected to be included in each date
        range. By default, the value of `end_column` is expected to be included as well -
        this can however be changed by setting `end_included` to `False`.

        A 'key' is a fixed set of values in `key_columns` and represents an entity of
        interest. A priori, a key is not a primary key, i.e., a key can have and often
        has several rows. Thereby, a key will often come with several date ranges.

        Often, you might want the date ranges for a given key not to overlap.

        If `key_columns` is `None` or `[]`, all columns of the table will be considered
        as composing the key.

        In order to express a tolerance for some violations of this non-overlapping
        property, use the `max_relative_n_violations` parameter. The latter expresses for
        what fraction of all key values, at least one overlap may exist.

        For illustrative examples of this constraint, please refer to its test cases.
        """

        relevant_columns = [start_column, end_column] + (
            key_columns if key_columns else []
        )
        ref = DataReference(self.data_source, relevant_columns, condition)
        self._constraints.append(
            date_constraints.DateNoOverlap(
                ref,
                key_columns=key_columns,
                start_columns=[start_column],
                end_columns=[end_column],
                end_included=end_included,
                max_relative_n_violations=max_relative_n_violations,
            )
        )

    def add_date_no_overlap_2d_constraint(
        self,
        start_column1: str,
        end_column1: str,
        start_column2: str,
        end_column2: str,
        key_columns: Optional[List[str]] = None,
        end_included: bool = True,
        max_relative_n_violations: float = 0,
        condition: Condition = None,
    ):
        """Express that several date range rows do not overlap in two date dimensions.

        The table under inspection must consist of at least one but up to many key columns,
        identifying an entity. Per date dimension, a start_column and an end_column should
        be provided.

        For a given row in this table, start_column1 and end_column1 indicate a date range.
        Moreoever, for that same row, start_column2 and end_column2 indicate a date range.
        These date ranges are expected to represent different date 'dimensions'. Example:
        A row indicates a forecasted value used in production. start_column1 and end_column1
        represent the timespan that was forecasted, e.g. the weather from next Saturday to
        next Sunday. end_column1 and end_column2 might indicate the timespan when this
        forceast was used, e.g. from the previous Monday to Wednesday.

        Neither of those columns should contain NULL values. Also it should hold that for
        a given row, the value of end_column is strictly greater than the value of
        start_column.

        Note that the values of `start_column1` and `start_column2` are expected to be
        includedin each date range. By default, the values of `end_column1` and
        `end_column2` are expected to be included as well - this can however be changed
        by setting `end_included` to `False`.

        A 'key' is a fixed set of values in key_columns and represents an entity of
        interest. A priori, a key is not a primary key, i.e., a key can have and often has
        several rows. Thereby, a key will often come with several date ranges.

        Often, you might want the date ranges for a given key not to overlap.

        If key_columns is `None` or `[]`, all columns of the table will be considered as
        composing the key.

        In order to express a tolerance for some violations of this non-overlapping property,
        use the `max_relative_n_violations` parameter. The latter expresses for what fraction
        of all key_values, at least one overlap may exist.

        For illustrative examples of this constraint, please refer to its test cases.
        """
        relevant_columns = (
            [start_column1, end_column1, start_column2, end_column2] + key_columns
            if key_columns
            else []
        )
        ref = DataReference(
            self.data_source,
            relevant_columns,
            condition,
        )
        self._constraints.append(
            date_constraints.DateNoOverlap2d(
                ref,
                key_columns=key_columns,
                start_columns=[start_column1, start_column2],
                end_columns=[end_column1, end_column2],
                end_included=end_included,
                max_relative_n_violations=max_relative_n_violations,
            )
        )

    def add_date_no_gap_constraint(
        self,
        start_column: str,
        end_column: str,
        key_columns: Optional[List[str]] = None,
        end_included: bool = True,
        max_relative_n_violations: float = 0,
        condition: Condition = None,
    ):
        """
        Express that date range rows have no gap in-between them.

        The table under inspection must consist of at least one but up to many key columns,
        identifying an entity. Additionally, a start_column and an end_column, indicating
        start and end dates, should be provided.

        Neither of those columns should contain NULL values. Also, it should hold that for
        a given row, the value of end_column is strictly greater than the value of
        start_column.

        Note that the value of `start_column` is expected to be included in each date range.
        By default, the value of `end_column` is expected to be included as well - this can
        however be changed by setting `end_included` to `False`.

        A 'key' is a fixed set of values in key_columns and represents an entity of
        interest. A priori, a key is not a primary key, i.e., a key can have and often has
        several rows. Thereby, a key will often come with several date ranges.

        If key_columns is `None` or `[]`, all columns of the table will be considered as
        composing the key.

        In order to express a tolerance for some violations of this gap property, use the
        `max_relative_n_violations` parameter. The latter expresses for what fraction
        of all key_values, at least one gap may exist.

        For illustrative examples of this constraint, please refer to its test cases.
        """
        relevant_columns = (
            ([start_column, end_column] + key_columns) if key_columns else []
        )
        ref = DataReference(self.data_source, relevant_columns, condition)
        self._constraints.append(
            date_constraints.DateNoGap(
                ref,
                key_columns=key_columns,
                start_columns=[start_column],
                end_columns=[end_column],
                max_relative_n_violations=max_relative_n_violations,
                end_included=end_included,
            )
        )

    def add_varchar_regex_constraint(
        self,
        column: str,
        regex: str,
        condition: Condition = None,
        allow_none: bool = False,
        relative_tolerance: float = 0.0,
        aggregated: bool = True,
    ):
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            varchar_constraints.VarCharRegex(
                ref,
                regex,
                allow_none=allow_none,
                relative_tolerance=relative_tolerance,
                aggregated=aggregated,
            )
        )

    def add_varchar_min_length_constraint(
        self, column: str, min_length: int, condition: Condition = None
    ):
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            varchar_constraints.VarCharMinLength(ref, min_length=min_length)
        )

    def add_varchar_max_length_constraint(
        self, column: str, max_length: int, condition: Condition = None
    ):
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            varchar_constraints.VarCharMaxLength(ref, max_length=max_length)
        )

    def add_groupby_aggregation_constraint(
        self,
        columns: Sequence[str],
        aggregation_column: str,
        start_value: int,
        tolerance: float = 0,
        condition: Condition = None,
    ):
        """Chek whether array aggregate corresponds to an integer range.

        The `DataSource` is grouped by `columns`. Sql's `array_agg` function is then
        applied to the `aggregate_column`.

        Since we expect `aggregate_column` to be a numeric column, this leads to
        a multiset of aggregated values. These values should correspond to the integers
        ranging from `start_value` to the cardinality of the multiset.

        In order to allow for slight deviations from this pattern, `tolerance` expresses
        the fraction of all grouped-by rows, which may be incomplete ranges.
        """

        ref = DataReference(self.data_source, list(columns), condition)
        self._constraints.append(
            groupby_constraints.AggregateNumericRangeEquality(
                ref,
                aggregation_column=aggregation_column,
                tolerance=tolerance,
                start_value=start_value,
            )
        )


class BetweenRequirement(Requirement):
    def __init__(
        self,
        data_source: DataSource,
        data_source2: DataSource,
        date_column: Optional[str] = None,
        date_column2: Optional[str] = None,
    ):
        self.data_source = data_source
        self.data_source2 = data_source2
        self.ref = DataReference(self.data_source)
        self.ref2 = DataReference(self.data_source2)
        self.date_column = date_column
        self.date_column2 = date_column2
        super().__init__()

    @classmethod
    def from_tables(
        cls,
        db_name1: str,
        schema_name1: str,
        table_name1: str,
        db_name2: str,
        schema_name2: str,
        table_name2: str,
        date_column: Optional[str] = None,
        date_column2: Optional[str] = None,
    ):
        return cls(
            data_source=TableDataSource(
                db_name=db_name1,
                schema_name=schema_name1,
                table_name=table_name1,
            ),
            data_source2=TableDataSource(
                db_name=db_name2,
                schema_name=schema_name2,
                table_name=table_name2,
            ),
            date_column=date_column,
            date_column2=date_column2,
        )

    @classmethod
    def from_raw_queries(
        cls,
        query1: str,
        query2: str,
        name1: str,
        name2: str,
        columns1: List[str] = None,
        columns2: List[str] = None,
        date_column: Optional[str] = None,
        date_column2: Optional[str] = None,
    ):
        """Create a `BetweenRequirement` based on raw query strings.

        The `query1` and `query2` parameters can be passed any query string returning
        rows, e.g. `"SELECT * FROM myschema.mytable LIMIT 1337"` or
        `"SELECT id, name FROM table1 UNION SELECT id, name FROM table2"`.

        `name1` and `name2` will be used to represent the queries in error messages,
        respectively.

        If constraints rely on specific columns, these should be provided here via
        `columns1` and `columns2` respectively.
        """
        return cls(
            data_source=RawQueryDataSource(query1, name1, columns=columns1),
            data_source2=RawQueryDataSource(query2, name2, columns=columns2),
            date_column=date_column,
            date_column2=date_column2,
        )

    @classmethod
    def from_expressions(
        cls,
        expression1,
        expression2,
        name1: str,
        name2: str,
        date_column: Optional[str] = None,
        date_column2: Optional[str] = None,
    ):
        """Create a `BetwenTableRequirement` based on sqlalchemy expressions.

        Any sqlalchemy object implementing the `alias` method can be passed as an
        argument for the `expression1` and `expression2` parameters. This could,
        e.g. be a `sqlalchemy.Table` object or the result of a `sqlalchemy.select`
        invokation.

        `name1` and `name2` will be used to represent the expressions in error messages,
        respectively.
        """
        return cls(
            data_source=ExpressionDataSource(expression1, name1),
            data_source2=ExpressionDataSource(expression2, name2),
            date_column=date_column,
            date_column2=date_column2,
        )

    def get_date_growth_rate(self, engine) -> float:
        if self.date_column is None or self.date_column2 is None:
            raise ValueError("Date growth can't be computed without date column.")
        date_growth_rate, _ = get_date_growth_rate(
            engine, self.ref, self.ref2, self.date_column, self.date_column2
        )
        return date_growth_rate

    def get_deviation_getter(
        self, fix_value: Optional[float], deviation: Optional[float]
    ):
        if fix_value is None and deviation is None:
            return ValueError("No valid gain/loss/deviation given.")
        if deviation is None:
            return lambda engine: fix_value
        if fix_value is None:
            return lambda engine: self.get_date_growth_rate(engine) + deviation
        return lambda engine: max(
            fix_value, self.get_date_growth_rate(engine) + deviation
        )

    def add_n_rows_equality_constraint(
        self, condition1: Condition = None, condition2: Condition = None
    ):
        ref = DataReference(self.data_source, condition=condition1)
        ref2 = DataReference(self.data_source2, condition=condition2)
        self._constraints.append(nrows_constraints.NRowsEquality(ref, ref2=ref2))

    def add_n_rows_max_gain_constraint(
        self,
        constant_max_relative_gain: Optional[float] = None,
        date_range_gain_deviation: Optional[float] = None,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        """#rows from first table <= #rows from second table * (1 + max_growth).

        See readme for more information on max_growth.
        """
        max_relative_gain_getter = self.get_deviation_getter(
            constant_max_relative_gain, date_range_gain_deviation
        )
        ref = DataReference(self.data_source, condition=condition1)
        ref2 = DataReference(self.data_source2, condition=condition2)
        self._constraints.append(
            nrows_constraints.NRowsMaxGain(ref, ref2, max_relative_gain_getter)
        )

    def add_n_rows_min_gain_constraint(
        self,
        constant_min_relative_gain: Optional[float] = None,
        date_range_gain_deviation: Optional[float] = None,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        """#rows from first table  >= #rows from second table * (1 + min_growth).

        See readme for more information on min_growth.
        """
        min_relative_gain_getter = self.get_deviation_getter(
            constant_min_relative_gain, date_range_gain_deviation
        )
        ref = DataReference(self.data_source, condition=condition1)
        ref2 = DataReference(self.data_source2, condition=condition2)
        self._constraints.append(
            nrows_constraints.NRowsMinGain(ref, ref2, min_relative_gain_getter)
        )

    def add_n_rows_max_loss_constraint(
        self,
        constant_max_relative_loss: Optional[float] = None,
        date_range_loss_deviation: Optional[float] = None,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        """#rows from first table >= #rows from second table * (1 - max_loss).

        See readme for more information on max_loss.
        """
        max_relative_loss_getter = self.get_deviation_getter(
            constant_max_relative_loss, date_range_loss_deviation
        )
        ref = DataReference(self.data_source, condition=condition1)
        ref2 = DataReference(self.data_source2, condition=condition2)
        self._constraints.append(
            nrows_constraints.NRowsMaxLoss(ref, ref2, max_relative_loss_getter)
        )

    def add_n_uniques_equality_constraint(
        self,
        columns1: Optional[List[str]],
        columns2: Optional[List[str]],
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(uniques_constraints.NUniquesEquality(ref, ref2=ref2))

    def add_n_uniques_max_gain_constraint(
        self,
        columns1: Optional[List[str]],
        columns2: Optional[List[str]],
        constant_max_relative_gain: Optional[float] = None,
        date_range_gain_deviation: Optional[float] = None,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        """#uniques or first table <= #uniques of second table* (1 + max_growth).

        #uniques in first table are defined based on columns1, #uniques in second
        table are defined based on columns2.

        See readme for more information on max_growth.
        """
        max_relative_gain_getter = self.get_deviation_getter(
            constant_max_relative_gain, date_range_gain_deviation
        )
        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            uniques_constraints.NUniquesMaxGain(ref, ref2, max_relative_gain_getter)
        )

    def add_n_uniques_max_loss_constraint(
        self,
        columns1: Optional[List[str]],
        columns2: Optional[List[str]],
        constant_max_relative_loss: Optional[float] = None,
        date_range_loss_deviation: Optional[float] = None,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        """#uniques in first table <= #uniques in second table * (1 - max_loss).

        #uniques in first table are defined based on columns1, #uniques in second
        table are defined based on columns2.

        See readme for more information on max_loss.
        """
        max_relative_loss_getter = self.get_deviation_getter(
            constant_max_relative_loss, date_range_loss_deviation
        )
        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            uniques_constraints.NUniquesMaxLoss(ref, ref2, max_relative_loss_getter)
        )

    def add_numeric_min_constraint(
        self,
        column1: str,
        column2: str,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(numeric_constraints.NumericMin(ref, ref2=ref2))

    def add_uniques_equality_constraint(
        self,
        columns1: List[str],
        columns2: List[str],
        map_func: Callable[[T], T] = None,
        reduce_func: Callable[[Collection], Collection] = None,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        """Check if the data's unique values in given columns are equal.

        The `UniquesEquality` constraint asserts if the values contained in a column
        of a DataSource`'s columns, are strictly the ones of another `DataSource`'s
        columns.

        See the `Uniques` class for further parameter details on `map_func` and
        `reduce_func`.
        """

        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            uniques_constraints.UniquesEquality(
                ref, ref2=ref2, map_func=map_func, reduce_func=reduce_func
            )
        )

    def add_uniques_superset_constraint(
        self,
        columns1: List[str],
        columns2: List[str],
        max_relative_violations: float = 0,
        map_func: Callable[[T], T] = None,
        reduce_func: Callable[[Collection], Collection] = None,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        """Check if unique calues of columns are contained in the reference data.

        The `UniquesSuperset` constraint asserts that reference set of expected values,
        derived from the unique values in given columns of the reference `DataSource`,
        is contained in given columns of a `DataSource`.

        Null values in the column are ignored. To assert the non-existence of them use
        the `NullAbsence` constraint via the `add_null_absence_constraint` helper method
        for `WithinRequirement`.

        `max_relative_violations` indicates what fraction of unique values of the given
        `DataSource` are not represented in the reference set of unique values. Please
        note that `UniquesSubset` and `UniquesSuperset` are not symmetrical in this regard.

        One use of this constraint is to test for consistency in columns with expected
        categorical values.

        See `Uniques` for further details on `map_func` and `reduce_func`.
        """

        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            uniques_constraints.UniquesSuperset(
                ref,
                ref2=ref2,
                max_relative_violations=max_relative_violations,
                map_func=map_func,
                reduce_func=reduce_func,
            )
        )

    def add_uniques_subset_constraint(
        self,
        columns1: List[str],
        columns2: List[str],
        max_relative_violations: float = 0,
        map_func: Callable[[T], T] = None,
        reduce_func: Callable[[Collection], Collection] = None,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        """Check if the given columns's unique values in are contained in reference data.

        The `UniquesSubset` constraint asserts if the values contained in given column of
        a `DataSource` are part of the unique values of given columns of another
        `DataSource`.

        Null values in the column are ignored. To assert the non-existence of them use
        the `NullAbsence` constraint via the `add_null_absence_constraint` helper method
        for `WithinRequirement`.

        `max_relative_violations` indicates what fraction of rows of the given table
        may have values not included in the reference set of unique values. Please note
        that `UniquesSubset` and `UniquesSuperset` are not symmetrical in this regard.

        See `Uniques` for further details on `map_func` and `reduce_func`.
        """

        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            uniques_constraints.UniquesSubset(
                ref,
                ref2=ref2,
                max_relative_violations=max_relative_violations,
                map_func=map_func,
                reduce_func=reduce_func,
            )
        )

    def add_numeric_max_constraint(
        self,
        column1: str,
        column2: str,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(numeric_constraints.NumericMax(ref, ref2=ref2))

    def add_numeric_mean_constraint(
        self,
        column1: str,
        column2: str,
        max_absolute_deviation: float,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(
            numeric_constraints.NumericMean(
                ref,
                max_absolute_deviation,
                ref2=ref2,
            )
        )

    def add_date_min_constraint(
        self,
        column1: str,
        column2: str,
        use_lower_bound_reference: bool = True,
        column_type: str = "date",
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        """Ensure date min of first table is greater or equal date min of second table.

        The used columns of both tables need to be of the same type.

        For valid column_type values, see get_format_from_column_type in constraints/base.py..

        If `use_lower_bound_reference`, the min of the first table has to be
        greater or equal to the min of the second table.
        If not `use_upper_bound_reference`, the min of the first table has to
        be smaller or equal to the min of the second table.
        """
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(
            date_constraints.DateMin(
                ref,
                ref2=ref2,
                use_lower_bound_reference=use_lower_bound_reference,
                column_type=column_type,
            )
        )

    def add_date_max_constraint(
        self,
        column1: str,
        column2: str,
        use_upper_bound_reference: bool = True,
        column_type: str = "date",
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        """Compare date max of first table to date max of second table.

        The used columns of both tables need to be of the same type.

        For valid column_type values, see get_format_from_column_type in constraints/base.py.

        If `use_upper_bound_reference`, the max of the first table has to be
        smaller or equal to the max of the second table.
        If not `use_upper_bound_reference`, the max of the first table has to
        be greater or equal to the max of the second table.
        """
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(
            date_constraints.DateMax(
                ref,
                ref2=ref2,
                use_upper_bound_reference=use_upper_bound_reference,
                column_type=column_type,
            )
        )

    def add_varchar_min_length_constraint(
        self,
        column1: str,
        column2: str,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(varchar_constraints.VarCharMinLength(ref, ref2=ref2))

    def add_varchar_max_length_constraint(
        self,
        column1: str,
        column2: str,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(varchar_constraints.VarCharMaxLength(ref, ref2=ref2))

    def add_column_subset_constraint(self):
        """Columns of first table are subset of second table."""
        self._constraints.append(
            column_constraints.ColumnSubset(self.ref, ref2=self.ref2)
        )

    def add_column_superset_constraint(self):
        """Columns of first table are superset of columns of second table."""
        self._constraints.append(
            column_constraints.ColumnSuperset(self.ref, ref2=self.ref2)
        )

    def add_row_equality_constraint(
        self,
        columns1: Optional[List[str]],
        columns2: Optional[List[str]],
        max_missing_fraction: float,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        """At most max_missing_fraction of rows in T1 and T2 are absent in either.

        I.e. (|T1 - T2| + |T2 - T1|) / |T1 U T2| <= max_missing_fraction.
        Rows from T1 are indexed in columns1, rows from T2 are indexed in columns2.
        """
        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            row_constraints.RowEquality(ref, ref2, lambda engine: max_missing_fraction)
        )

    def add_row_subset_constraint(
        self,
        columns1: Optional[List[str]],
        columns2: Optional[List[str]],
        constant_max_missing_fraction: Optional[float],
        date_range_loss_fraction: Optional[float] = None,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        """At most max_missing_fraction of rows in T1 are not in T2.

        I.e. |T1-T2|/|T1| <= max_missing_fraction.
        Rows from T1 are indexed in columns1, rows from T2 are indexed in columns2.

        In particular, the operation |T1-T2| relies on a sql EXCEPT statement. In
        constrast to EXCEPT ALL, this should lead to a set subtraction instead of
        a multiset subtraction. In other words, duplicates in T1 are treated as
        single occurrences.
        """
        max_missing_fraction_getter = self.get_deviation_getter(
            constant_max_missing_fraction, date_range_loss_fraction
        )
        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            row_constraints.RowSubset(ref, ref2, max_missing_fraction_getter)
        )

    def add_row_superset_constraint(
        self,
        columns1: Optional[List[str]],
        columns2: Optional[List[str]],
        constant_max_missing_fraction: float,
        date_range_loss_fraction: Optional[float] = None,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        """At most max_missing_fraction of rows in T2 are not in T1.

        I.e. |T2-T1|/|T2| <= max_missing_fraction.
        Rows from T1 are indexed in columns1, rows from T2 are indexed in columns2.
        """
        max_missing_fraction_getter = self.get_deviation_getter(
            constant_max_missing_fraction, date_range_loss_fraction
        )
        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            row_constraints.RowSuperset(ref, ref2, max_missing_fraction_getter)
        )

    def add_row_matching_equality_constraint(
        self,
        matching_columns1: List[str],
        matching_columns2: List[str],
        comparison_columns1: List[str],
        comparison_columns2: List[str],
        max_missing_fraction: float,
        condition1: Condition = None,
        condition2: Condition = None,
    ):
        """Match tables in matching_columns, compare for equality in comparison_columns.

        This constraint is similar to the nature of the RowEquality
        constraint. Just as the latter, this constraint divides the
        cardinality of an intersection by the cardinality of a union.
        The difference lies in how the set are created. While RowEquality
        considers all rows of both tables, indexed in columns,
        RowMatchingEquality considers only rows in both tables having values
        in matching_columns present in both tables. At most max_missing_fraction
        of such rows can be missing in the intersection.

        Alternatively, this can be thought of as counting mismatches in
        comparison_columns after performing an inner join on matching_columns.
        """
        ref = DataReference(self.data_source, None, condition1)
        ref2 = DataReference(self.data_source2, None, condition2)
        self._constraints.append(
            row_constraints.RowMatchingEquality(
                ref,
                ref2,
                matching_columns1,
                matching_columns2,
                comparison_columns1,
                comparison_columns2,
                lambda engine: max_missing_fraction,
            )
        )
