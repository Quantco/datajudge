from __future__ import annotations

import functools
import json
import operator
from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass
from typing import Sequence, final, overload

import sqlalchemy as sa
from sqlalchemy.sql.expression import FromClause


def is_mssql(engine: sa.engine.Engine) -> bool:
    return engine.name == "mssql"


def is_postgresql(engine: sa.engine.Engine) -> bool:
    return engine.name == "postgresql"


def is_snowflake(engine: sa.engine.Engine) -> bool:
    return engine.name == "snowflake"


def get_table_columns(table, column_names):
    return [table.c[column_name] for column_name in column_names]


@overload
def lowercase_column_names(column_names: str) -> str:
    ...


@overload
def lowercase_column_names(column_names: list[str]) -> list[str]:
    ...


def lowercase_column_names(column_names: str | list[str]) -> str | list[str]:
    # This function is used due to capitalization problems with snowflake.
    # Once these issues are resolved in snowflake-sqlalchemy, the usages of this
    # function should be removed.
    # See https://github.com/snowflakedb/snowflake-sqlalchemy/issues/157.
    if isinstance(column_names, str):
        return column_names.lower()
    return [column_name.lower() for column_name in column_names]


@dataclass(frozen=True)
class Condition:
    """Condition allows for further narrowing down of a DataSource in a Constraint.

    A `Condition` can be thought of as a filter, the content of a sql 'where' clause
    or a condition as known from probability theory.

    While a `DataSource` is expressed more generally, one might be interested
    in testing properties of a specific part of said `DataSource` in light
    of a particular constraint. Hence using `Condition`s allows for the reusage
    of a `DataSource, in lieu of creating a new custom `DataSource` with
    the `Condition` implicitly built in.

    A `Condition` can either be 'atomic', i.e. not further reducible to sub-conditions
    or 'composite', i.e. combining multiple subconditions. In the former case, it can
    be instantiated with help of the `raw_string` parameter, e.g. `"col1 > 0"`. In the
    latter case, it can be instantiated with help of the `conditions` and
    `reduction_operator` parameters. `reduction_operator` allows for two values: `"and"` (logical
    conjunction) and `"or"` (logical disjunction). Note that composition of `Condition`s
    supports arbitrary degrees of nesting.
    """

    raw_string: str | None = None
    conditions: Sequence[Condition] | None = None
    reduction_operator: str | None = None

    def __post_init__(self):
        if self._is_atomic() and self.conditions is not None:
            raise ValueError(
                "Condition can either be instantiated atomically, with "
                "the raw_query parameter, or in a composite fashion, with "
                "the conditions parameter. "
                "Exactly one of them needs to be provided, yet both are."
            )
        if not self._is_atomic() and (
            self.conditions is None or len(self.conditions) == 0
        ):
            raise ValueError(
                "Condition can either be instantiated atomically, with "
                "the raw_query parameter, or in a composite fashion, with "
                "the conditions parameter. "
                "Exactly one of them needs to be provided, yet none is."
            )
        if not self._is_atomic() and self.reduction_operator not in ["and", "or"]:
            raise ValueError(
                "reuction_operator has to be either 'and' or 'or' but "
                f"obtained {self.reduction_operator}."
            )

    def _is_atomic(self):
        return self.raw_string is not None

    def __str__(self):
        if self._is_atomic():
            return self.raw_string
        return f" {self.reduction_operator} ".join(
            f"({condition})" for condition in self.conditions
        )

    def snowflake_str(self):
        # Temporary method - should be removed as soon as snowflake-sqlalchemy
        # bug is fixed.
        return str(self)


@dataclass
class MatchAndCompare:
    matching_columns1: Sequence[str]
    matching_columns2: Sequence[str]
    comparison_columns1: Sequence[str]
    comparison_columns2: Sequence[str]

    def _get_matching_columns(self):
        return zip(self.matching_columns1, self.matching_columns2)

    def _get_comparison_columns(self):
        return zip(self.comparison_columns1, self.comparison_columns2)

    def __str__(self):
        return (
            f"Matched on {self.matching_columns1} and "
            f"{self.matching_columns2}. Compared on "
            f"{self.comparison_columns1} and "
            f"{self.comparison_columns2}."
        )

    def get_matching_string(self, table_variable1, table_variable2):
        return " AND ".join(
            [
                f"{table_variable1}.{column1} = {table_variable2}.{column2}"
                for (column1, column2) in self._get_matching_columns()
            ]
        )

    def get_comparison_string(self, table_variable1, table_variable2):
        return " AND ".join(
            [
                (
                    f"({table_variable1}.{column1} = "
                    f"{table_variable2}.{column2} "
                    f"OR ({table_variable1}.{column1} IS NULL AND "
                    f"{table_variable2}.{column2} IS NULL))"
                )
                for (column1, column2) in self._get_comparison_columns()
            ]
        )


class DataSource(ABC):
    @abstractmethod
    def __str__(self) -> str:
        pass

    @abstractmethod
    def get_clause(self, engine: sa.engine.Engine) -> FromClause:
        pass


@functools.lru_cache(maxsize=1)
def get_metadata():
    return sa.MetaData()


@final
class TableDataSource(DataSource):
    def __init__(
        self,
        db_name: str,
        table_name: str,
        schema_name: str | None = None,
    ):
        self.db_name = db_name
        self.table_name = table_name
        self.schema_name = schema_name

    def __str__(self) -> str:
        if self.schema_name:
            return f"{self.db_name}.{self.schema_name}.{self.table_name}"
        return self.table_name

    def get_clause(self, engine: sa.engine.Engine) -> FromClause:
        schema = self.schema_name
        if is_mssql(engine):
            schema = self.db_name + "." + self.schema_name  # type: ignore

        return sa.Table(
            self.table_name,
            get_metadata(),
            autoload_with=engine,
            schema=schema,
        )


@final
class ExpressionDataSource(DataSource):
    def __init__(self, expression: FromClause, name: str):
        self.expression = expression
        self.name = name

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(expression={self.expression!r}, name={self.name})"

    def get_clause(self, engine: sa.engine.Engine) -> FromClause:
        return self.expression.alias()


@final
class RawQueryDataSource(DataSource):
    def __init__(self, query_string: str, name: str, columns: list[str] = None):
        self.query_string = query_string
        self.name = name
        self.columns = columns
        wrapped_query = f"({query_string}) as t"
        if columns is not None and len(columns) > 0:
            subquery = (
                sa.text(query_string)
                .columns(*[sa.column(column_name) for column_name in columns])
                .subquery()
            )
            self.clause = subquery
        else:
            wrapped_query = f"({query_string}) as t"
            self.clause = sa.select(["*"]).select_from(sa.text(wrapped_query)).alias()

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(query_string={self.query_string}, name={self.name}, columns={self.columns})"

    def get_clause(self, engine: sa.engine.Engine) -> FromClause:
        return self.clause


class DataReference:
    def __init__(
        self,
        data_source: DataSource,
        columns: list[str] = None,
        condition: Condition = None,
    ):
        self.data_source = data_source
        self.columns = columns
        self.condition = condition

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(data_source={self.data_source!r}, columns={self.columns!r}, condition={self.condition!r})"

    def get_selection(self, engine: sa.engine.Engine):
        clause = self.data_source.get_clause(engine)
        if self.columns:
            selection = sa.select(
                [clause.c[column_name] for column_name in self.get_columns(engine)]
            )
        else:
            selection = sa.select([clause])
        if self.condition is not None:
            text = str(self.condition)
            if is_snowflake(engine):
                text = self.condition.snowflake_str()
            selection = selection.where(sa.text(text))
        if is_mssql(engine) and isinstance(self.data_source, TableDataSource):
            # Allow dirty reads when using MSSQL.
            # When using an ExpressionDataSource or StringDataSource, the user is
            # expected to specify this by themselves.
            # More on this:
            # https://docs.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-2016
            selection = selection.with_hint(clause, "WITH (NOLOCK)")
        return selection

    def get_column(self, engine):
        """Fetch the only relevant column of a DataReference."""
        if self.columns is None:
            raise ValueError(
                f"Trying to access column of DataReference "
                f"{self.get_string()} yet none is given."
            )
        return self.get_columns(engine)[0]

    def get_columns(self, engine):
        """Fetch all relevant columns of a DataReference."""
        if is_snowflake(engine):
            return lowercase_column_names(self.columns)
        return self.columns

    def get_columns_or_pk_columns(self, engine):
        return (
            self.columns
            if self.columns is not None
            else get_primary_keys(engine, self.data_source)
        )

    def get_column_selection_string(self):
        if self.columns is None:
            return " * "
        return ", ".join(self.columns)

    def get_clause_string(self, *, return_where=True):
        where_string = "WHERE " if return_where else ""
        return "" if self.condition is None else where_string + str(self.condition)

    def get_string(self):
        if self.columns is None:
            return str(self.data_source)
        return f"{self.data_source}'s columns " f" {self.get_column_selection_string()}"


def merge_conditions(condition1, condition2):
    if condition1 and condition2 is None:
        return None
    if condition1 is None:
        return condition2
    if condition2 is None:
        return condition1
    return Condition(conditions=[condition1, condition2], reduction_operator="and")


def get_date_span(engine, ref, date_column_name):
    if is_snowflake(engine):
        date_column_name = lowercase_column_names(date_column_name)
    subquery = ref.get_selection(engine).alias()
    column = subquery.c[date_column_name]
    if is_postgresql(engine):
        selection = sa.select(
            [
                sa.sql.extract(
                    "day",
                    (
                        sa.func.date_trunc(sa.literal("day"), sa.func.max(column))
                        - sa.func.date_trunc(sa.literal("day"), sa.func.min(column))
                    ),
                )
            ]
        )
    elif is_mssql(engine) or is_snowflake(engine):
        selection = sa.select(
            [
                sa.func.datediff(
                    sa.text("day"),
                    sa.func.min(column),
                    sa.func.max(column),
                )
            ]
        )
    else:
        raise NotImplementedError(
            "Date spans not yet implemented for this sql dialect."
        )

    date_span = engine.connect().execute(selection).scalar()
    if date_span < 0:
        raise ValueError(
            f"Date span has negative value: {date_span}. It must be positive."
        )
    # Note (ivergara): From postgres 13 to 14 the type returned by the selection changed from float to Decimal.
    # Now we're making sure that the returned type of this function is a float to comply with downstream expectations.
    # Since we're dealing with date spans, and most likely the level of precision doesn't require a Decimal
    # representation, we decided to enforce here the float type instead of using Decimal downstream.
    return float(date_span), [selection]


def get_date_growth_rate(engine, ref, ref2, date_column, date_column2):
    date_span, selections = get_date_span(engine, ref, date_column)
    date_span2, selections2 = get_date_span(engine, ref2, date_column2)
    if date_span2 == 0:
        raise ValueError("Reference date span is not allowed to be zero.")
    return date_span / date_span2 - 1, [*selections, *selections]


def get_date_overlaps_nd(
    engine: sa.engine.Engine,
    ref: DataReference,
    key_columns: list[str] | None,
    start_columns: list[str],
    end_columns: list[str],
    end_included: bool,
):
    if is_snowflake(engine):
        if key_columns:
            key_columns = lowercase_column_names(key_columns)
        start_columns = lowercase_column_names(start_columns)
        end_columns = lowercase_column_names(end_columns)
    if len(start_columns) != len(end_columns):
        raise ValueError(
            f"Expected same dimensionality for start_columns and end_columns. "
            f"Instead, start_columns has dimensionality {len(start_columns)} and "
            f"end_columns has dimensionality {len(end_columns)}."
        )
    dimensionality = len(start_columns)
    table1 = ref.get_selection(engine).alias()
    table2 = ref.get_selection(engine).alias()

    key_conditions = (
        [table1.c[key_column] == table2.c[key_column] for key_column in key_columns]
        if key_columns
        else [True]
    )
    table_key_columns = get_table_columns(table1, key_columns) if key_columns else []

    end_operator = operator.ge if end_included else operator.gt
    violation_condition = sa.and_(
        *[
            sa.and_(
                table1.c[start_columns[dimension]] < table2.c[start_columns[dimension]],
                end_operator(
                    table1.c[end_columns[dimension]], table2.c[start_columns[dimension]]
                ),
            )
            for dimension in range(dimensionality)
        ]
    )

    join_condition = sa.and_(*key_conditions, violation_condition)
    violation_selection = sa.select(
        table_key_columns
        + [
            table.c[start_column]
            for table in [table1, table2]
            for start_column in start_columns
        ]
        + [
            table.c[end_column]
            for table in [table1, table2]
            for end_column in end_columns
        ]
    ).select_from(table1.join(table2, join_condition))

    # Note, Kevin, 21/12/09
    # The following approach would likely be preferable to the approach used
    # subsequently. Sadly, it seems to not work for mssql. As of now, it is
    # unclear to me why.

    # distincter = (
    #     sa.func.distinct(sa.tuple_(*get_table_columns(violation_subquery, key_columns)))
    #     if key_columns
    #     else sa.func.distinct("*")
    # )

    # n_violations_selection = sa.select([sa.func.count(distincter)]).select_from(
    #     violation_subquery
    # )

    violation_subquery = violation_selection.subquery()

    keys = (
        get_table_columns(violation_subquery, key_columns)
        if key_columns
        else violation_subquery.columns
    )
    violation_subquery = sa.select(keys, group_by=keys).subquery()

    n_violations_selection = sa.select(sa.func.count()).select_from(violation_subquery)

    return violation_selection, n_violations_selection


def _not_in_interval_condition(
    main_table: sa.Table,
    helper_table: sa.Table,
    date_column: str,
    key_columns: list[str],
    start_column: str,
    end_column: str,
):
    return sa.not_(
        sa.exists(
            sa.select(helper_table).where(
                sa.and_(
                    *[
                        main_table.c[key_column] == helper_table.c[key_column]
                        for key_column in key_columns
                    ],
                    main_table.c[date_column] > helper_table.c[start_column],
                    main_table.c[date_column] < helper_table.c[end_column],
                )
            )
        )
    )


def get_date_gaps(
    engine: sa.engine.Engine,
    ref: DataReference,
    key_columns: list[str] | None,
    start_column: str,
    end_column: str,
    end_included: bool,
):
    if is_snowflake(engine):
        if key_columns:
            key_columns = lowercase_column_names(key_columns)
        start_column = lowercase_column_names(start_column)
        end_column = lowercase_column_names(end_column)

    # Inspired by
    # https://stackoverflow.com/questions/9604400/sql-query-to-show-gaps-between-multiple-date-ranges.

    helper_table = ref.get_selection(engine).alias()
    raw_start_table = ref.get_selection(engine).alias()
    raw_end_table = ref.get_selection(engine).alias()

    if key_columns is None or key_columns == []:
        key_columns = [
            column.name
            for column in helper_table.columns
            if column.name not in [start_column, end_column]
        ]

    start_not_in_other_interval_condition = _not_in_interval_condition(
        raw_start_table,
        helper_table,
        start_column,
        key_columns,
        start_column,
        end_column,
    )

    end_not_in_other_interval_condition = _not_in_interval_condition(
        raw_end_table, helper_table, end_column, key_columns, start_column, end_column
    )

    start_rank_column = (
        sa.func.row_number()
        .over(
            order_by=raw_start_table.c[start_column],
        )
        .label("start_rank")
    )

    end_rank_column = (
        sa.func.row_number()
        .over(
            order_by=raw_end_table.c[end_column],
        )
        .label("end_rank")
    )

    start_table = (
        sa.select([*raw_start_table.columns, start_rank_column])
        .where(start_not_in_other_interval_condition)
        .subquery()
    )

    end_table = (
        sa.select([*raw_end_table.columns, end_rank_column])
        .where(end_not_in_other_interval_condition)
        .subquery()
    )

    legitimate_gap_size = 1 if end_included else 0

    if is_mssql(engine) or is_snowflake(engine):
        gap_condition = (
            sa.func.datediff(
                sa.text("day"),
                end_table.c[end_column],
                start_table.c[start_column],
            )
            > legitimate_gap_size
        )
    elif is_postgresql(engine):
        gap_condition = (start_table.c[start_column] > end_table.c[end_column] + 1,)
        gap_condition = (
            sa.sql.extract(
                "day",
                (
                    sa.func.date_trunc(sa.literal("day"), start_table.c[start_column])
                    - sa.func.date_trunc(sa.literal("day"), end_table.c[end_column])
                ),
            )
            > legitimate_gap_size
        )
    else:
        raise NotImplementedError(f"Date gaps not yet implemented for {engine.name}.")

    join_condition = sa.and_(
        *[
            start_table.c[key_column] == end_table.c[key_column]
            for key_column in key_columns
        ],
        start_table.c["start_rank"] == end_table.c["end_rank"] + 1,
        gap_condition,
    )

    violation_selection = sa.select(
        [
            *get_table_columns(start_table, key_columns),
            start_table.c[start_column],
            end_table.c[end_column],
        ]
    ).select_from(start_table.join(end_table, join_condition))

    violation_subquery = violation_selection.subquery()

    keys = get_table_columns(violation_subquery, key_columns)

    grouped_violation_subquery = sa.select(keys, group_by=keys).subquery()

    n_violations_selection = sa.select([sa.func.count()]).select_from(
        grouped_violation_subquery
    )

    return violation_selection, n_violations_selection


def get_row_count(engine, ref, row_limit: int = None):
    """Return the number of rows for a `DataReference`.

    If `row_limit` is given, the number of rows is capped at the limit.
    """
    subquery = ref.get_selection(engine)
    if row_limit:
        subquery = subquery.limit(row_limit)
    subquery = subquery.alias()
    selection = sa.select([sa.cast(sa.func.count(), sa.BigInteger)]).select_from(
        subquery
    )
    result = engine.connect().execute(selection).scalar()
    return result, [selection]


def _column_aggregate(engine, ref, column_operator):
    subquery = ref.get_selection(engine).alias()
    column = subquery.c[ref.get_column(engine)]
    selection = sa.select([column_operator(column)])
    result = engine.connect().execute(selection).scalar()
    return result, [selection]


def get_min(engine, ref):
    column_operator = sa.func.min
    return _column_aggregate(engine, ref, column_operator)


def get_max(engine, ref):
    column_operator = sa.func.max
    return _column_aggregate(engine, ref, column_operator)


def get_mean(engine, ref):
    def column_operator(column):
        return sa.func.avg(sa.cast(column, sa.DECIMAL))

    return _column_aggregate(engine, ref, column_operator)


def get_min_length(engine, ref):
    def column_operator(column):
        return sa.func.min(sa.func.length(column))

    return _column_aggregate(engine, ref, column_operator)


def get_max_length(engine, ref):
    def column_operator(column):
        return sa.func.max(sa.func.length(column))

    return _column_aggregate(engine, ref, column_operator)


def get_fraction_between(engine, ref, lower_bound, upper_bound):
    column = ref.get_column(engine)
    new_condition = Condition(
        conditions=[
            Condition(raw_string=f"{column} >= {lower_bound}"),
            Condition(raw_string=f"{column} <= {upper_bound}"),
        ],
        reduction_operator="and",
    )
    overall_condition = merge_conditions(ref.condition, new_condition)
    new_ref = DataReference(
        ref.data_source, columns=ref.get_columns(engine), condition=overall_condition
    )
    n_all, selections_all = get_row_count(engine, ref)
    n_filtered, selections_filtered = get_row_count(engine, new_ref)
    selections = [*selections_all, *selections_filtered]
    return (n_filtered / n_all) if n_all > 0 else None, selections


def get_uniques(
    engine: sa.engine.Engine, ref: DataReference
) -> tuple[Counter, list[sa.select]]:
    if not ref.get_columns(engine):
        return Counter({}), []
    selection = ref.get_selection(engine).alias()
    columns = [selection.c[column_name] for column_name in ref.get_columns(engine)]
    selection = sa.select([*columns, sa.func.count()], group_by=columns)

    def _scalar_accessor(row):
        return row[0]

    def _tuple_accessor(row):
        return row[0 : len(columns)]

    unique_from_row = _tuple_accessor

    if len(columns) == 1:
        unique_from_row = _scalar_accessor

    result = Counter(
        {
            unique_from_row(row): row[-1]
            for row in engine.connect().execute(selection).fetchall()
        }
    )
    return result, [selection]


def get_unique_count(engine, ref):
    selection = ref.get_selection(engine)
    subquery = selection.distinct().alias()
    selection = sa.select([sa.func.count()]).select_from(subquery)
    result = engine.connect().execute(selection).scalar()
    return result, [selection]


def get_unique_count_union(engine, ref, ref2):
    selection1 = ref.get_selection(engine)
    selection2 = ref2.get_selection(engine)
    subquery = sa.sql.union(selection1, selection2).alias().select().distinct().alias()
    selection = sa.select([sa.func.count()]).select_from(subquery)
    result = engine.connect().execute(selection).scalar()
    return result, [selection]


def contains_null(engine, ref):
    selection = ref.get_selection(engine)
    subquery = selection.distinct().alias()
    selection = (
        sa.select([sa.func.count()])
        .select_from(subquery)
        .where(subquery.c[ref.get_column(engine)].is_(None))
    )
    n_rows = engine.connect().execute(selection).scalar()
    return n_rows > 0, [selection]


def get_column_names(engine, ref):
    table = ref.data_source.get_clause(engine)
    return [column.name for column in table.columns], None


def get_column_type(engine, ref):
    table = ref.get_selection(engine).alias()
    if is_snowflake(engine):
        column_type = [str(column.type) for column in table.columns][0]
        # Integer columns loaded from snowflake database may be referred to as decimal with
        # 0 scale. More here:
        # https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#decimal-numeric
        if column_type == "DECIMAL(38, 0)":
            column_type = "integer"
        return column_type, None
    column_type = [str(column.type).split(" ")[0] for column in table.columns][0]
    return column_type, None


def get_primary_keys(engine, ref):
    table = ref.data_source.get_clause(engine)
    return [column.name for column in table.primary_key.columns], None


def get_row_difference_sample(engine, ref, ref2):
    selection1 = ref.get_selection(engine)
    selection2 = ref2.get_selection(engine)
    selection = sa.sql.except_(selection1, selection2).alias().select()
    result = engine.connect().execute(selection).first()
    return result, [selection]


def get_row_difference_count(engine, ref, ref2):
    selection1 = ref.get_selection(engine)
    selection2 = ref2.get_selection(engine)
    subquery = (
        sa.sql.except_(selection1, selection2).alias().select().distinct().alias()
    )
    selection = sa.select([sa.func.count()]).select_from(subquery)
    result = engine.connect().execute(selection).scalar()
    return result, [selection]


def get_row_mismatch(engine, ref, ref2, match_and_compare):
    subselection1 = ref.get_selection(engine).alias()
    subselection2 = ref2.get_selection(engine).alias()

    matching_columns1 = get_table_columns(
        subselection1, match_and_compare.matching_columns1
    )
    matching_columns2 = get_table_columns(
        subselection2, match_and_compare.matching_columns2
    )
    comparing_columns1 = get_table_columns(
        subselection1, match_and_compare.comparison_columns1
    )
    comparing_columns2 = get_table_columns(
        subselection2, match_and_compare.comparison_columns2
    )

    match = sa.and_(
        *[
            column1 == column2
            for column1, column2 in zip(matching_columns1, matching_columns2)
        ]
    )
    compare = sa.and_(
        *[
            sa.or_(column1 == column2, sa.and_(column1.is_(None), column2.is_(None)))
            for column1, column2 in zip(comparing_columns1, comparing_columns2)
        ]
    )

    avg_match_column = sa.func.avg(sa.case([(compare, 0.0)], else_=1.0))

    selection = sa.select([avg_match_column]).select_from(
        subselection1.join(subselection2, match)
    )
    result = engine.connect().execute(selection).scalar()
    return result, [selection]


def get_duplicate_sample(engine, ref):
    initial_selection = ref.get_selection(engine).alias()
    aggregate_subquery = (
        sa.select([initial_selection, sa.func.count().label("n_copies")])
        .select_from(initial_selection)
        .group_by(*initial_selection.columns)
        .alias()
    )
    duplicate_selection = (
        sa.select(
            [
                column
                for column in aggregate_subquery.columns
                if column.key != "n_copies"
            ]
        )
        .select_from(aggregate_subquery)
        .where(aggregate_subquery.c.n_copies > 1)
    )
    result = engine.connect().execute(duplicate_selection).first()
    return result, [duplicate_selection]


def column_array_agg_query(
    engine: sa.engine.Engine, ref: DataReference, aggregation_column: str
):
    clause = ref.data_source.get_clause(engine)
    # NOTE: This was needed to appease mypy.
    if not ref.get_columns(engine):
        raise ValueError("There must be a column to group by")
    group_columns = [clause.c[column] for column in ref.get_columns(engine)]
    agg_column = clause.c[aggregation_column]
    selection = sa.select(
        [*group_columns, sa.func.array_agg(agg_column)], group_by=[*group_columns]
    )
    return [selection]


def snowflake_parse_variant_column(value: str):
    # Snowflake returns non-primitive columns such as arrays as JSON string,
    # but we want them in their deserialized form.
    return json.loads(value)


def get_column_array_agg(
    engine: sa.engine.Engine, ref: DataReference, aggregation_column: str
):
    selections = column_array_agg_query(engine, ref, aggregation_column)
    result = engine.connect().execute(selections[0]).fetchall()
    if is_snowflake(engine):
        result = [
            (*t[:-1], list(map(int, snowflake_parse_variant_column(t[-1]))))
            for t in result
        ]
    return result, selections
