from __future__ import annotations

import json
import operator
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any, overload

import sqlalchemy as sa
from sqlalchemy.sql import selectable

from ._engines import (
    is_bigquery,
    is_db2,
    is_duckdb,
    is_mssql,
    is_postgresql,
    is_snowflake,
)
from .condition import Condition
from .data_source import DataSource, TableDataSource


def get_table_columns(
    table: sa.Table | sa.Subquery, column_names: Sequence[str]
) -> list[sa.ColumnElement]:
    return [table.c[column_name] for column_name in column_names]


def apply_patches(engine: sa.engine.Engine) -> None:
    """Apply patches to e.g. specific dialect not implemented by sqlalchemy."""
    if is_bigquery(engine):
        # Patch for the EXCEPT operator (see BigQuery set operators
        # https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators)
        # This is implemented in the same way as for sqlalchemy-bigquery, see
        # https://github.com/googleapis/python-bigquery-sqlalchemy/blob/f1889443bd4d680550387b9bb14daeea8eb792d4/sqlalchemy_bigquery/base.py#L187
        compound_keywords_extensions = {
            selectable.CompoundSelect.EXCEPT: "EXCEPT DISTINCT",  # type: ignore[attr-defined]
            selectable.CompoundSelect.EXCEPT_ALL: "EXCEPT ALL",  # type: ignore[attr-defined]
        }
        engine.dialect.statement_compiler.compound_keywords.update(
            compound_keywords_extensions
        )

        # Patch for the INTERSECT operator (see BigQuery set operators
        # https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators)
        # This might cause some problems (see discussion in
        # https://github.com/googleapis/python-bigquery-sqlalchemy/issues/388) but doesn't seem
        # to be an issue here.
        compound_keywords_extensions = {
            selectable.CompoundSelect.INTERSECT: "INTERSECT DISTINCT",  # type: ignore[attr-defined]
            selectable.CompoundSelect.INTERSECT_ALL: "INTERSECT ALL",  # type: ignore[attr-defined]
        }
        engine.dialect.statement_compiler.compound_keywords.update(
            compound_keywords_extensions
        )


@overload
def lowercase_column_names(column_names: str) -> str:  # fmt: off
    ...


@overload
def lowercase_column_names(column_names: list[str]) -> list[str]:  # fmt: off
    ...


def lowercase_column_names(column_names: str | list[str]) -> str | list[str]:
    # This function is used due to capitalization problems with snowflake.
    # Once these issues are resolved in snowflake-sqlalchemy, the usages of this
    # function should be removed.
    # See https://github.com/snowflakedb/snowflake-sqlalchemy/issues/157.
    if isinstance(column_names, str):
        return column_names.lower()
    return [column_name.lower() for column_name in column_names]


@dataclass
class _MatchAndCompare:
    matching_columns1: Sequence[str]
    matching_columns2: Sequence[str]
    comparison_columns1: Sequence[str]
    comparison_columns2: Sequence[str]

    def _get_matching_columns(self) -> Iterator[tuple[str, str]]:
        return zip(self.matching_columns1, self.matching_columns2)

    def _get_comparison_columns(self) -> Iterator[tuple[str, str]]:
        return zip(self.comparison_columns1, self.comparison_columns2)

    def __str__(self) -> str:
        return (
            f"Matched on {self.matching_columns1} and "
            f"{self.matching_columns2}. Compared on "
            f"{self.comparison_columns1} and "
            f"{self.comparison_columns2}."
        )

    def get_matching_string(self, table_variable1: str, table_variable2: str) -> str:
        return " AND ".join(
            [
                f"{table_variable1}.{column1} = {table_variable2}.{column2}"
                for (column1, column2) in self._get_matching_columns()
            ]
        )

    def get_comparison_string(self, table_variable1: str, table_variable2: str) -> str:
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


class DataReference:
    def __init__(
        self,
        data_source: DataSource,
        columns: list[str] | None = None,
        condition: Condition | None = None,
    ):
        if columns is not None and not isinstance(columns, list):
            raise TypeError(f"columns must be a list, not {type(columns)}")

        self.data_source = data_source
        self.columns = columns
        self.condition = condition

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(data_source={self.data_source!r}, columns={self.columns!r}, condition={self.condition!r})"

    def get_selection(self, engine: sa.engine.Engine) -> sa.Select:
        clause = self.data_source._get_clause(engine)
        if self.columns:
            column_names = self.get_columns(engine)
            if column_names is None:
                raise ValueError("This shouldn't happen.")
            selection = sa.select(
                *[clause.c[column_name] for column_name in column_names]
            )
        else:
            selection = sa.select(clause)
        if self.condition is not None:
            text = str(self.condition)
            if is_snowflake(engine):
                text = self.condition._snowflake_str()
            selection = selection.where(sa.text(text))
        if is_mssql(engine) and isinstance(self.data_source, TableDataSource):
            # Allow dirty reads when using MSSQL.
            # When using an ExpressionDataSource or StringDataSource, the user is
            # expected to specify this by themselves.
            # More on this:
            # https://docs.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-2016
            selection = selection.with_hint(clause, "WITH (NOLOCK)")
        return selection

    def get_column(self, engine: sa.engine.Engine) -> str:
        """Fetch the only relevant column of a DataReference."""
        if self.columns is None:
            raise ValueError(
                f"Trying to access column of DataReference "
                f"{str(self)} yet none is given."
            )
        columns = self.get_columns(engine)
        if columns is None:
            raise ValueError("No columns defined.")
        if len(columns) > 1:
            raise ValueError(
                "DataReference was expected to only have a single column but had multiple: "
                f"{columns}"
            )
        return columns[0]

    def get_columns(self, engine: sa.engine.Engine) -> list[str] | None:
        """Fetch all relevant columns of a DataReference."""
        if self.columns is None:
            return None
        if is_snowflake(engine):
            return lowercase_column_names(self.columns)
        return self.columns

    def get_columns_or_pk_columns(self, engine: sa.engine.Engine) -> list[str] | None:
        return (
            self.columns
            if self.columns is not None
            else get_primary_keys(engine, self)[0]
        )

    def get_column_selection_string(self) -> str:
        if self.columns is None:
            return " * "
        return ", ".join(map(lambda x: f"'{x}'", self.columns))

    def _get_clause_string(self, *, return_where: bool = True) -> str:
        where_string = "WHERE " if return_where else ""
        return "" if self.condition is None else where_string + str(self.condition)

    def __str__(self) -> str:
        if self.columns is None:
            return str(self.data_source)
        return f"{self.data_source}'s column(s) {self.get_column_selection_string()}"


def merge_conditions(
    condition1: Condition | None, condition2: Condition | None
) -> Condition | None:
    if condition1 and condition2 is None:
        return None
    if condition1 is None:
        return condition2
    if condition2 is None:
        return condition1
    return Condition(conditions=[condition1, condition2], reduction_operator="and")


def get_date_span(
    engine: sa.engine.Engine, ref: DataReference, date_column_name: str
) -> tuple[float, list[sa.Select]]:
    if is_snowflake(engine):
        date_column_name = lowercase_column_names(date_column_name)
    subquery = ref.get_selection(engine).alias()
    column = subquery.c[date_column_name]
    if is_postgresql(engine):
        selection = sa.select(
            *[
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
            *[
                sa.func.datediff(
                    sa.text("day"),
                    sa.func.min(column),
                    sa.func.max(column),
                )
            ]
        )
    elif is_duckdb(engine):
        selection = sa.select(
            *[
                sa.func.datediff(
                    sa.literal("day"),
                    sa.func.min(column),
                    sa.func.max(column),
                )
            ]
        )
    elif is_bigquery(engine):
        selection = sa.select(
            *[
                sa.func.date_diff(
                    sa.func.max(column),
                    sa.func.min(column),
                    sa.literal_column("DAY"),
                )
            ]
        )
    elif is_db2(engine):
        selection = sa.select(
            *[
                sa.func.days_between(
                    sa.func.max(column),
                    sa.func.min(column),
                )
            ]
        )
    else:
        raise NotImplementedError(
            "Date spans not yet implemented for this sql dialect."
        )

    with engine.connect() as connection:
        date_span = connection.execute(selection).scalar()
    if date_span is None:
        raise ValueError("Date span could not be fetched.")
    if date_span < 0:
        raise ValueError(
            f"Date span has negative value: {date_span}. It must be positive."
        )
    # Note (ivergara): From postgres 13 to 14 the type returned by the selection changed from float to Decimal.
    # Now we're making sure that the returned type of this function is a float to comply with downstream expectations.
    # Since we're dealing with date spans, and most likely the level of precision doesn't require a Decimal
    # representation, we decided to enforce here the float type instead of using Decimal downstream.
    return float(date_span), [selection]


def _get_date_growth_rate(
    engine: sa.engine.Engine,
    ref: DataReference,
    ref2: DataReference,
    date_column: str,
    date_column2: str,
) -> tuple[float, list[sa.Select]]:
    date_span, selections = get_date_span(engine, ref, date_column)
    date_span2, selections2 = get_date_span(engine, ref2, date_column2)
    if date_span2 == 0:
        raise ValueError("Reference date span is not allowed to be zero.")
    return date_span / date_span2 - 1, [*selections, *selections2]


def get_interval_overlaps_nd(
    engine: sa.engine.Engine,
    ref: DataReference,
    key_columns: list[str] | None,
    start_columns: list[str],
    end_columns: list[str],
    end_included: bool,
) -> tuple[selectable.CompoundSelect, selectable.Select]:
    r"""Create selectables for interval overlaps in n dimensions.

    We define the presence of 'overlap' as presence of a non-empty intersection
    between two intervals.

    Given that we care about a single dimension and have two intervals :math:`t1` and :math:`t2`,
    we define an overlap follows:

     .. math::
        \\begin{align} \\text{overlap}(t_1, t_2) \\Leftrightarrow
            &(min(t_1) \\leq min(t_2) \\land max(t_1) \\geq min(t_2)) \\\\
            &\\lor \\\\
            &(min(t_2) \\leq min(t_1) \\land max(t_2) \\geq min(t_1))
        \\end{align}

    We can drop the second clause of the above disjunction if we define :math:`t_1` to be the 'leftmost'
    interval. We do so when building our query.

    Note that the above equations are representative of ``end_included=True`` and the second clause
    of the conjunction would use a strict inequality if ``end_included=False``.

    We define an overlap in several dimensions as the conjunction of overlaps in every single dimension.
    """
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
        else [sa.literal(True)]
    )
    table_key_columns = get_table_columns(table1, key_columns) if key_columns else []

    end_operator = operator.ge if end_included else operator.gt

    # We have a violation in two scenarios:
    # 1. At least two entries are exactly equal in key and interval columns
    # 2. Two entries are not exactly equal in key and interval_columns and fuilfill violation_condition

    # Scenario 1
    duplicate_selection = duplicates(table1)
    duplicate_subquery = duplicate_selection.subquery()

    # scenario 2
    naive_violation_condition = sa.and_(
        *[
            sa.and_(
                table1.c[start_columns[dimension]]
                <= table2.c[start_columns[dimension]],
                end_operator(
                    table1.c[end_columns[dimension]], table2.c[start_columns[dimension]]
                ),
            )
            for dimension in range(dimensionality)
        ]
    )

    interval_inequality_condition = sa.or_(
        *[
            sa.or_(
                table1.c[start_columns[dimension]]
                != table2.c[start_columns[dimension]],
                table2.c[end_columns[dimension]] != table2.c[end_columns[dimension]],
            )
            for dimension in range(dimensionality)
        ]
    )

    distinct_violation_condition = sa.and_(
        naive_violation_condition,
        interval_inequality_condition,
    )

    distinct_join_condition = sa.and_(*key_conditions, distinct_violation_condition)
    distinct_violation_selection = sa.select(
        *table_key_columns,
        *[
            table.c[start_column]
            for table in [table1, table2]
            for start_column in start_columns
        ],
        *[
            table.c[end_column]
            for table in [table1, table2]
            for end_column in end_columns
        ],
    ).select_from(table1.join(table2, distinct_join_condition))
    distinct_violation_subquery = distinct_violation_selection.subquery()

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

    # Merge scenarios 1 and 2.
    # We need to 'impute' the missing columns for the duplicate selection in order for the union between
    # both selections to work.
    duplicate_selection = sa.select(
        *(
            # Already existing columns
            [
                duplicate_subquery.c[column]
                for column in distinct_violation_subquery.columns.keys()
                if column in duplicate_subquery.columns.keys()
            ]
            # Fill all missing columns with NULLs.
            + [
                sa.null().label(column)
                for column in distinct_violation_subquery.columns.keys()
                if column not in duplicate_subquery.columns.keys()
            ]
        )
    )
    violation_selection = duplicate_selection.union(distinct_violation_selection)

    violation_subquery = violation_selection.subquery()

    keys = (
        get_table_columns(violation_subquery, key_columns)
        if key_columns
        else violation_subquery.columns
    )
    violation_subquery = sa.select(*keys).group_by(*keys).subquery()

    n_violations_selection = sa.select(sa.func.count()).select_from(violation_subquery)

    return violation_selection, n_violations_selection


def _not_in_interval_condition(
    main_table: sa.Table | sa.Subquery,
    helper_table: sa.Table | sa.Subquery,
    date_column: str,
    key_columns: list[str],
    start_column: str,
    end_column: str,
) -> sa.ColumnElement:
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


def _get_interval_gaps(
    engine: sa.engine.Engine,
    ref: DataReference,
    key_columns: list[str] | None,
    start_column: str,
    end_column: str,
    legitimate_gap_size: float,
    make_gap_condition: Callable[
        [sa.Engine, sa.Subquery, sa.Subquery, str, str, float], sa.ColumnElement[bool]
    ],
) -> tuple[sa.Select, sa.Select]:
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
        .over(order_by=[raw_start_table.c[col] for col in [start_column] + key_columns])
        .label("start_rank")
    )

    end_rank_column = (
        sa.func.row_number()
        .over(order_by=[raw_end_table.c[col] for col in [end_column] + key_columns])
        .label("end_rank")
    )

    start_table = (
        sa.select(*raw_start_table.columns, start_rank_column)
        .where(start_not_in_other_interval_condition)
        .subquery()
    )

    end_table = (
        sa.select(*raw_end_table.columns, end_rank_column)
        .where(end_not_in_other_interval_condition)
        .subquery()
    )

    gap_condition = make_gap_condition(
        engine, start_table, end_table, start_column, end_column, legitimate_gap_size
    )

    join_condition = sa.and_(
        *[
            start_table.c[key_column] == end_table.c[key_column]
            for key_column in key_columns
        ],
        start_table.c["start_rank"] == end_table.c["end_rank"] + 1,
        gap_condition,
    )

    violation_selection = sa.select(
        *get_table_columns(start_table, key_columns),
        start_table.c[start_column],
        end_table.c[end_column],
    ).select_from(start_table.join(end_table, join_condition))

    violation_subquery = violation_selection.subquery()

    keys = get_table_columns(violation_subquery, key_columns)

    grouped_violation_subquery = sa.select(*keys).group_by(*keys).subquery()

    n_violations_selection = sa.select(sa.func.count()).select_from(
        grouped_violation_subquery
    )

    return violation_selection, n_violations_selection


def _date_gap_condition(
    engine: sa.engine.Engine,
    start_table: sa.Subquery,
    end_table: sa.Subquery,
    start_column: str,
    end_column: str,
    legitimate_gap_size: float,
) -> sa.ColumnElement[bool]:
    if is_mssql(engine) or is_snowflake(engine):
        gap_condition = (
            sa.func.datediff(
                sa.text("day"),
                end_table.c[end_column],
                start_table.c[start_column],
            )
            > legitimate_gap_size
        )
    elif is_duckdb(engine):
        gap_condition = (
            sa.func.datediff(
                sa.literal("day"),
                end_table.c[end_column],
                start_table.c[start_column],
            )
            > legitimate_gap_size
        )
    elif is_bigquery(engine):
        # see https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_diff
        # Note that to have a gap (positive date_diff), the first date (start table)
        # in date_diff must be greater than the second date (end_table)
        gap_condition = (
            sa.func.date_diff(
                start_table.c[start_column], end_table.c[end_column], sa.text("DAY")
            )
            > legitimate_gap_size
        )
    elif is_postgresql(engine):
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
    elif is_db2(engine):
        gap_condition = (
            sa.func.days_between(
                start_table.c[start_column],
                end_table.c[end_column],
            )
            > legitimate_gap_size
        )
    else:
        raise NotImplementedError(f"Date gaps not yet implemented for {engine.name}.")
    return gap_condition


def get_date_gaps(
    engine: sa.engine.Engine,
    ref: DataReference,
    key_columns: list[str] | None,
    start_column: str,
    end_column: str,
    legitimate_gap_size: float,
) -> tuple[sa.Select, sa.Select]:
    return _get_interval_gaps(
        engine,
        ref,
        key_columns,
        start_column,
        end_column,
        legitimate_gap_size,
        _date_gap_condition,
    )


def _numeric_gap_condition(
    _engine: sa.engine.Engine,
    start_table: sa.Subquery,
    end_table: sa.Subquery,
    start_column: str,
    end_column: str,
    legitimate_gap_size: float,
) -> sa.ColumnElement[bool]:
    gap_condition = (
        start_table.c[start_column] - end_table.c[end_column]
    ) > legitimate_gap_size
    return gap_condition


def get_numeric_gaps(
    engine: sa.engine.Engine,
    ref: DataReference,
    key_columns: list[str] | None,
    start_column: str,
    end_column: str,
    legitimate_gap_size: float = 0,
) -> tuple[sa.Select, sa.Select]:
    return _get_interval_gaps(
        engine,
        ref,
        key_columns,
        start_column,
        end_column,
        legitimate_gap_size,
        _numeric_gap_condition,
    )


def get_functional_dependency_violations(
    engine: sa.engine.Engine,
    ref: DataReference,
    key_columns: list[str],
) -> tuple[Any, list[sa.Select]]:
    selection = ref.get_selection(engine)
    uniques = selection.distinct().cte()

    key_columns_sa = [uniques.c[key_column] for key_column in key_columns]
    violations_stmt = (
        sa.select(*key_columns_sa).group_by(*key_columns_sa).having(sa.func.count() > 1)
    ).cte()

    join_condition = sa.and_(
        *[
            uniques.c[key_column] == violations_stmt.c[key_column]
            for key_column in key_columns
        ]
    )

    violation_tuples = sa.select(uniques).select_from(
        uniques.join(violations_stmt, join_condition)
    )

    with engine.connect() as connection:
        result = connection.execute(violation_tuples).fetchall()
    return result, [violation_tuples]


def get_row_count(
    engine: sa.engine.Engine, ref: DataReference, row_limit: int | None = None
) -> tuple[int, list[sa.Select]]:
    """Return the number of rows for a `DataReference`.

    If `row_limit` is given, the number of rows is capped at the limit.
    """
    selection = ref.get_selection(engine)
    if row_limit:
        selection = selection.limit(row_limit)
    subquery = selection.alias()
    final_selection = sa.select(sa.cast(sa.func.count(), sa.BigInteger)).select_from(
        subquery
    )
    with engine.connect() as connection:
        result = int(str(connection.execute(final_selection).scalar()))
    return result, [final_selection]


def get_column(
    engine: sa.engine.Engine,
    ref: DataReference,
    *,
    aggregate_operator: Callable | None = None,
) -> tuple[Any, list[sa.Select]]:
    """
    Query the database for the values of the relevant column (as returned by `get_column(...)`).

    If an aggregation operation is passed, the results are aggregated accordingly
    and a single scalar value is returned.
    """
    subquery = ref.get_selection(engine).alias()
    column = subquery.c[ref.get_column(engine)]

    result: Any | None | Sequence[Any]

    if not aggregate_operator:
        selection = sa.select(column)
        with engine.connect() as connection:
            result = connection.execute(selection).scalars().all()

    else:
        selection = sa.select(aggregate_operator(column))
        with engine.connect() as connection:
            result = connection.execute(selection).scalar()

    return result, [selection]


def get_min(
    engine: sa.engine.Engine, ref: DataReference
) -> tuple[Any, list[sa.Select]]:
    column_operator = sa.func.min
    return get_column(engine, ref, aggregate_operator=column_operator)


def get_max(
    engine: sa.engine.Engine, ref: DataReference
) -> tuple[Any, list[sa.Select]]:
    column_operator = sa.func.max
    return get_column(engine, ref, aggregate_operator=column_operator)


def get_mean(
    engine: sa.engine.Engine, ref: DataReference
) -> tuple[Any, list[sa.Select]]:
    def column_operator(column):
        return sa.func.avg(sa.cast(column, sa.DECIMAL))

    return get_column(engine, ref, aggregate_operator=column_operator)


def get_percentile(
    engine: sa.engine.Engine, ref: DataReference, percentage: float
) -> tuple[float, list[sa.Select]]:
    row_count = "dj_row_count"
    row_num = "dj_row_num"
    column_name = ref.get_column(engine)
    base_selection = ref.get_selection(engine)
    column = base_selection.subquery().c[column_name]

    counting_selection = sa.select(
        column,
        sa.func.row_number().over(order_by=column).label(row_num),
        sa.func.count().over(partition_by=None).label(row_count),
    ).where(column.is_not(None))
    counting_subquery = counting_selection.subquery()

    inferior_selection = sa.select(*counting_subquery.columns).where(
        counting_subquery.c[row_num] * 100.0 / counting_subquery.c[row_count]
        < percentage
    )
    inferior_subquery = inferior_selection.subquery()

    argmin_selection = sa.select(
        sa.case(
            # Case 1: We we pick the next value.
            (
                sa.func.count(inferior_subquery.c[row_num]) > 0,
                sa.func.max(inferior_subquery.c[row_num]) + 1,
            ),
            # Case 2: We pick the first value since the inferior subquery
            # is empty.
            (sa.func.count(inferior_subquery.c[row_num]) == 0, 1),
            # Case 3: We received a reference without numerical values.
            else_=None,
        )
    )

    percentile_selection = sa.select(counting_subquery.c[column_name]).where(
        counting_subquery.c[row_num] == argmin_selection.scalar_subquery()
    )
    with engine.connect() as connection:
        intermediate_result = connection.execute(percentile_selection).scalar()
    if intermediate_result is None:
        raise ValueError("Percentile selection could not be fetched.")
    result = float(intermediate_result)
    return result, [percentile_selection]


def get_min_length(
    engine: sa.engine.Engine, ref: DataReference
) -> tuple[int, list[sa.Select]]:
    def column_operator(column):
        return sa.func.min(sa.func.length(column))

    return get_column(engine, ref, aggregate_operator=column_operator)


def get_max_length(
    engine: sa.engine.Engine, ref: DataReference
) -> tuple[int, list[sa.Select]]:
    def column_operator(column):
        return sa.func.max(sa.func.length(column))

    return get_column(engine, ref, aggregate_operator=column_operator)


def get_fraction_between(
    engine: sa.engine.Engine,
    ref: DataReference,
    lower_bound: str | float,
    upper_bound: str | float,
) -> tuple[float | None, list[sa.Select]]:
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
    if n_all is None or n_all == 0:
        return (None, selections)
    if n_filtered is None:
        return (0.0, selections)
    return n_filtered / n_all, selections


def get_uniques(
    engine: sa.engine.Engine, ref: DataReference
) -> tuple[Counter, list[sa.Select]]:
    if not ref.get_columns(engine):
        return Counter({}), []
    subquery = ref.get_selection(engine).alias()
    if (column_names := ref.get_columns(engine)) is None:
        raise ValueError("Need columns for get_uniques.")
    columns = [subquery.c[column_name] for column_name in column_names]
    selection = sa.select(*columns, sa.func.count()).group_by(*columns)

    def _scalar_accessor(row):
        return row[0]

    def _tuple_accessor(row):
        return row[0 : len(columns)]

    unique_from_row = _tuple_accessor

    if len(columns) == 1:
        unique_from_row = _scalar_accessor

    with engine.connect() as connection:
        result = Counter(
            {
                unique_from_row(row): row[-1]
                for row in connection.execute(selection).fetchall()
            }
        )
    return result, [selection]


def get_unique_count(
    engine: sa.engine.Engine, ref: DataReference
) -> tuple[int, list[sa.Select]]:
    selection = ref.get_selection(engine)
    subquery = selection.distinct().alias()
    selection = sa.select(sa.func.count()).select_from(subquery)
    with engine.connect() as connection:
        intermediate_result = connection.execute(selection).scalar()
    if intermediate_result is None:
        raise ValueError("Unique count could not be fetched.")
    result = int(intermediate_result)
    return result, [selection]


def get_unique_count_union(
    engine: sa.engine.Engine, ref: DataReference, ref2: DataReference
) -> tuple[int, list[sa.Select]]:
    selection1 = ref.get_selection(engine)
    selection2 = ref2.get_selection(engine)
    subquery = sa.sql.union(selection1, selection2).alias().select().distinct().alias()
    selection = sa.select(sa.func.count()).select_from(subquery)
    with engine.connect() as connection:
        intermediate_result = connection.execute(selection).scalar()
    if intermediate_result is None:
        raise ValueError("Unique count could not be fetched.")
    result = int(intermediate_result)
    return result, [selection]


def get_missing_fraction(
    engine: sa.engine.Engine, ref: DataReference
) -> tuple[float, list[sa.Select]]:
    selection = ref.get_selection(engine).subquery()
    n_rows_total_selection = sa.select(sa.func.count()).select_from(selection)
    n_rows_missing_selection = (
        sa.select(sa.func.count())
        .select_from(selection)
        .where(selection.c[ref.get_column(engine)].is_(None))
    )
    with engine.connect() as connection:
        n_rows_total = connection.execute(n_rows_total_selection).scalar()
        n_rows_missing = connection.execute(n_rows_missing_selection).scalar()

    if n_rows_total is None or n_rows_missing is None:
        return (0, [n_rows_total_selection, n_rows_missing_selection])
    return (
        n_rows_missing / n_rows_total,
        [n_rows_total_selection, n_rows_missing_selection],
    )


def get_column_names(
    engine: sa.engine.Engine, ref: DataReference
) -> tuple[list[str], None]:
    table = ref.data_source._get_clause(engine)
    return [column.name for column in table.columns], None


def get_column_type(engine: sa.engine.Engine, ref: DataReference) -> tuple[Any, None]:
    table = ref.get_selection(engine).alias()
    column_type = next(iter(table.columns)).type
    return column_type, None


def get_primary_keys(
    engine: sa.engine.Engine, ref: DataReference
) -> tuple[list[str], None]:
    data_source = ref.data_source

    if isinstance(data_source, TableDataSource):
        table = data_source._get_clause(engine)
        return [column.name for column in table.primary_key.columns], None

    raise NotImplementedError(
        f"Cannot determine primary keys of a data source of type {type(data_source)}."
    )


def get_row_difference_sample(
    engine: sa.engine.Engine, ref: DataReference, ref2: DataReference
) -> tuple[Any, list[sa.Select]]:
    selection1 = ref.get_selection(engine)
    selection2 = ref2.get_selection(engine)
    selection = sa.sql.except_(selection1, selection2).alias().select()
    with engine.connect() as connection:
        result = connection.execute(selection).first()
    return result, [selection]


def get_row_difference_count(
    engine: sa.engine.Engine, ref: DataReference, ref2: DataReference
) -> tuple[int, list[sa.Select]]:
    selection1 = ref.get_selection(engine)
    selection2 = ref2.get_selection(engine)
    subquery = (
        sa.sql.except_(selection1, selection2).alias().select().distinct().alias()
    )
    selection = sa.select(sa.func.count()).select_from(subquery)
    with engine.connect() as connection:
        result_intermediate = connection.execute(selection).scalar()
    if result_intermediate is None:
        raise ValueError("Could not get the row difference count.")
    result = int(result_intermediate)
    return result, [selection]


def get_row_mismatch(
    engine: sa.engine.Engine,
    ref: DataReference,
    ref2: DataReference,
    match_and_compare: _MatchAndCompare,
) -> tuple[float, int, list[sa.Select]]:
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

    avg_match_column = sa.func.avg(sa.case((compare, 0.0), else_=1.0))

    selection_difference = sa.select(avg_match_column).select_from(
        subselection1.join(subselection2, match)
    )
    selection_n_rows = sa.select(sa.func.count()).select_from(
        subselection1.join(subselection2, match)
    )
    with engine.connect() as connection:
        result_mismatch_intermediate = connection.execute(selection_difference).scalar()
        result_n_rows_intermediate = connection.execute(selection_n_rows).scalar()
    if result_mismatch_intermediate is None or result_n_rows_intermediate is None:
        raise ValueError("Could not fetch number of mismatches.")
    result_mismatch = float(result_mismatch_intermediate)
    result_n_rows = int(result_n_rows_intermediate)
    return result_mismatch, result_n_rows, [selection_difference, selection_n_rows]


def duplicates(subquery: sa.Subquery) -> sa.Select:
    aggregate_subquery = (
        sa.select(subquery, sa.func.count().label("n_copies"))
        .select_from(subquery)
        .group_by(*subquery.columns)
        .alias()
    )
    duplicate_selection = (
        sa.select(
            *[
                column
                for column in aggregate_subquery.columns
                if column.key != "n_copies"
            ]
        )
        .select_from(aggregate_subquery)
        .where(aggregate_subquery.c.n_copies > 1)
    )
    return duplicate_selection


def get_duplicate_sample(
    engine: sa.engine.Engine, ref: DataReference
) -> tuple[Any, list[sa.Select]]:
    initial_selection = ref.get_selection(engine).alias()
    duplicate_selection = duplicates(initial_selection)
    with engine.connect() as connection:
        result = connection.execute(duplicate_selection).first()
    return result, [duplicate_selection]


def column_array_agg_query(
    engine: sa.engine.Engine, ref: DataReference, aggregation_column: str
) -> list[sa.Select]:
    clause = ref.data_source._get_clause(engine)
    if not (column_names := ref.get_columns(engine)):
        raise ValueError("There must be a column to group by")
    group_columns = [clause.c[column] for column in column_names]
    agg_column = clause.c[aggregation_column]
    selection = sa.select(*group_columns, sa.func.array_agg(agg_column)).group_by(
        *group_columns
    )
    return [selection]


def snowflake_parse_variant_column(value: str) -> dict:
    # Snowflake returns non-primitive columns such as arrays as JSON string,
    # but we want them in their deserialized form.
    return json.loads(value)


def get_column_array_agg(
    engine: sa.engine.Engine, ref: DataReference, aggregation_column: str
) -> tuple[Any, list[sa.Select]]:
    selections = column_array_agg_query(engine, ref, aggregation_column)
    with engine.connect() as connection:
        result: Sequence[sa.engine.row.Row[Any]] | list[tuple[Any, ...]] = (
            connection.execute(selections[0]).fetchall()
        )
    if is_snowflake(engine):
        result = [
            (*t[:-1], list(map(int, snowflake_parse_variant_column(t[-1]))))
            for t in result
        ]
    return result, selections


def _cdf_selection(
    engine: sa.engine.Engine, ref: DataReference, cdf_label: str, value_label: str
) -> sa.Subquery:
    """Create an empirical cumulative distribution function values.

    Concretely, create a selection with values from ``value_label`` as well as
    the empirical cumulative didistribution function values, labeled as
    ``cdf_label``.
    """
    col = ref.get_column(engine)
    selection = ref.get_selection(engine).subquery()

    # Step 1: Calculate the CDF over the value column.
    cdf_selection = sa.select(
        selection.c[col].label(value_label),
        sa.func.cume_dist().over(order_by=col).label(cdf_label),
    ).subquery()

    # Step 2: Aggregate rows s.t. every value occurs only once.
    grouped_cdf_selection = (
        sa.select(
            cdf_selection.c[value_label],
            sa.func.max(cdf_selection.c[cdf_label]).label(cdf_label),
        )
        .group_by(cdf_selection.c[value_label])
        .subquery()
    )
    return grouped_cdf_selection


def _cross_cdf_selection(
    engine: sa.engine.Engine,
    ref1: DataReference,
    ref2: DataReference,
    cdf_label: str,
    value_label: str,
) -> tuple[sa.Select, str, str]:
    """Create a cross cumulative distribution function selection given two samples.

    Concretely, both ``DataReference``s are expected to have specified a single relevant column.
    This function will generate a selection with rows of the kind ``(value, cdf1(value), cdf2(value))``,
    where ``cdf1`` is the cumulative distribution function of ``ref1`` and ``cdf2`` of ``ref2``.

    E.g. if ``ref`` is a reference to a table's column with values ``[1, 1, 3, 2]``, and ``ref2`` is
    a reference to a table's column with values ``[2, 5, 4]``, executing the returned selection should
    yield a table of the following kind: ``[(1, .5, 0), (2, .75, 1/3), (3, 1 ,1/3), (4, 1, 2/3), (5, 1, 1)]``.
    """
    cdf_label1 = cdf_label + "1"
    cdf_label2 = cdf_label + "2"
    group_label1 = "_grp1"
    group_label2 = "_grp2"

    cdf_selection1 = _cdf_selection(engine, ref1, cdf_label, value_label)
    cdf_selection2 = _cdf_selection(engine, ref2, cdf_label, value_label)

    # Step 3: Combine the cdfs.
    cross_cdf = (
        sa.select(
            sa.func.coalesce(
                cdf_selection1.c[value_label], cdf_selection2.c[value_label]
            ).label(value_label),
            cdf_selection1.c[cdf_label].label(cdf_label1),
            cdf_selection2.c[cdf_label].label(cdf_label2),
        )
        .select_from(
            cdf_selection1.join(
                cdf_selection2,
                cdf_selection1.c[value_label] == cdf_selection2.c[value_label],
                isouter=True,
                full=True,
            )
        )
        .subquery()
    )

    def _cdf_index_column(table, value_label, cdf_label, group_label):
        return (
            sa.func.count(table.c[cdf_label])
            .over(order_by=table.c[value_label])
            .label(group_label)
        )

    # Step 4: Create a grouper id based on the value count; this is just a helper for forward-filling.
    # In other words, we point rows to their most recent present value - per sample. This is necessary
    # Due to the nature of the full outer join.
    indexed_cross_cdf = sa.select(
        cross_cdf.c[value_label],
        _cdf_index_column(cross_cdf, value_label, cdf_label1, group_label1),
        cross_cdf.c[cdf_label1],
        _cdf_index_column(cross_cdf, value_label, cdf_label2, group_label2),
        cross_cdf.c[cdf_label2],
    ).subquery()

    def _forward_filled_cdf_column(table, cdf_label, value_label, group_label):
        return (
            # Step 6: Replace NULL values at the beginning with 0 to enable computation of difference.
            sa.func.coalesce(
                (
                    # Step 5: Forward-Filling: Select first non-NULL value per group (defined in the prev. step).
                    sa.func.first_value(table.c[cdf_label]).over(
                        partition_by=table.c[group_label], order_by=table.c[value_label]
                    )
                ),
                0,
            ).label(cdf_label)
        )

    filled_cross_cdf = sa.select(
        indexed_cross_cdf.c[value_label],
        _forward_filled_cdf_column(
            indexed_cross_cdf, cdf_label1, value_label, group_label1
        ),
        _forward_filled_cdf_column(
            indexed_cross_cdf, cdf_label2, value_label, group_label2
        ),
    )
    return filled_cross_cdf, cdf_label1, cdf_label2


def get_ks_2sample(
    engine: sa.engine.Engine,
    ref1: DataReference,
    ref2: DataReference,
) -> tuple[float, list[sa.Select]]:
    """
    Run the query for the two-sample Kolmogorov-Smirnov test and return the test statistic d.

    For a raw-sql version of this query, please see this PR:
    https://github.com/Quantco/datajudge/pull/28/
    """
    cdf_label = "cdf"
    value_label = "val"
    filled_cross_cdf_selection, cdf_label1, cdf_label2 = _cross_cdf_selection(
        engine, ref1, ref2, cdf_label, value_label
    )

    filled_cross_cdf = filled_cross_cdf_selection.subquery()

    # Step 7: Calculate final statistic: maximal distance.
    final_selection = sa.select(
        sa.func.max(
            sa.func.abs(filled_cross_cdf.c[cdf_label1] - filled_cross_cdf.c[cdf_label2])
        )
    )

    with engine.connect() as connection:
        d_statistic = connection.execute(final_selection).scalar()

    if d_statistic is None:
        raise ValueError("Could not compute d statistic.")

    return d_statistic, [final_selection]


def get_regex_violations(
    engine: sa.engine.Engine,
    ref: DataReference,
    aggregated: bool,
    regex: str,
    n_counterexamples: int,
) -> tuple[tuple[int, Any], list[sa.Select]]:
    original_selection = ref.get_selection(engine)
    column = ref.get_column(engine)
    if aggregated:
        original_selection = original_selection.distinct()
    subquery = original_selection.subquery()

    violation_selection = sa.select(subquery.c[column]).where(
        sa.not_(subquery.c[column].regexp_match(regex))
    )
    n_violations_selection = sa.select(sa.func.count()).select_from(
        violation_selection.subquery()
    )

    selections = [n_violations_selection]

    if n_counterexamples == -1:
        counterexamples_selection = violation_selection
    elif n_counterexamples == 0:
        counterexamples_selection = None
    elif n_counterexamples > 0:
        counterexamples_selection = violation_selection.limit(n_counterexamples)
    else:
        raise ValueError(f"Unexpected number of counterexamples: {n_counterexamples}")

    if counterexamples_selection is not None:
        selections.append(counterexamples_selection)

    with engine.connect() as connection:
        n_violations_result = connection.execute(n_violations_selection).scalar()
        if n_violations_result is None:
            n_violations_result = 0
        if counterexamples_selection is None:
            counterexamples = []
        else:
            counterexamples_result = connection.execute(
                counterexamples_selection
            ).fetchall()
            counterexamples = [result[0] for result in counterexamples_result]
    return (n_violations_result, counterexamples), selections
