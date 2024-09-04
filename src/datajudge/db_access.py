from __future__ import annotations

import functools
import json
import operator
from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable, Sequence, final, overload

import sqlalchemy as sa
from sqlalchemy.sql import selectable
from sqlalchemy.sql.expression import FromClause


def is_mssql(engine: sa.engine.Engine) -> bool:
    return engine.name == "mssql"


def is_postgresql(engine: sa.engine.Engine) -> bool:
    return engine.name == "postgresql"


def is_snowflake(engine: sa.engine.Engine) -> bool:
    return engine.name == "snowflake"


def is_bigquery(engine: sa.engine.Engine) -> bool:
    return engine.name == "bigquery"


def is_impala(engine: sa.engine.Engine) -> bool:
    return engine.name == "impala"


def is_db2(engine: sa.engine.Engine) -> bool:
    return engine.name == "ibm_db_sa"


def get_table_columns(table, column_names):
    return [table.c[column_name] for column_name in column_names]


def apply_patches(engine: sa.engine.Engine):
    """
    Apply patches to e.g. specific dialect not implemented by sqlalchemy
    """

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


@dataclass(frozen=True)
class Condition:
    """Condition allows for further narrowing down of a DataSource in a Constraint.

    A ``Condition`` can be thought of as a filter, the content of a sql 'where' clause
    or a condition as known from probability theory.

    While a ``DataSource`` is expressed more generally, one might be interested
    in testing properties of a specific part of said ``DataSource`` in light
    of a particular constraint. Hence using ``Condition`` allows for the reusage
    of a ``DataSource``, in lieu of creating a new custom ``DataSource`` with
    the ``Condition`` implicitly built in.

    A ``Condition`` can either be 'atomic', i.e. not further reducible to sub-conditions
    or 'composite', i.e. combining multiple subconditions. In the former case, it can
    be instantiated with help of the ``raw_string`` parameter, e.g. ``"col1 > 0"``. In the
    latter case, it can be instantiated with help of the ``conditions`` and
    ``reduction_operator`` parameters. ``reduction_operator`` allows for two values: ``"and"`` (logical
    conjunction) and ``"or"`` (logical disjunction). Note that composition of ``Condition``
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
        if not self.conditions:
            raise ValueError("This should never happen thanks to __post__init.")
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
    def __init__(self, expression: FromClause | sa.Select, name: str):
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
    def __init__(self, query_string: str, name: str, columns: list[str] | None = None):
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
            self.clause = sa.select("*").select_from(sa.text(wrapped_query)).alias()

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

    def get_selection(self, engine: sa.engine.Engine):
        clause = self.data_source.get_clause(engine)
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

    def get_columns(self, engine) -> list[str] | None:
        """Fetch all relevant columns of a DataReference."""
        if self.columns is None:
            return None
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
        return ", ".join(map(lambda x: f"'{x}'", self.columns))

    def get_clause_string(self, *, return_where=True):
        where_string = "WHERE " if return_where else ""
        return "" if self.condition is None else where_string + str(self.condition)

    def __str__(self):
        if self.columns is None:
            return str(self.data_source)
        return f"{self.data_source}'s column(s) {self.get_column_selection_string()}"


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
    elif is_impala(engine):
        selection = sa.select(
            *[
                sa.func.datediff(
                    sa.func.to_date(sa.func.max(column)),
                    sa.func.to_date(sa.func.min(column)),
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
    return date_span / date_span2 - 1, [*selections, *selections2]


def get_interval_overlaps_nd(
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
        else [sa.literal(True)]
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
    violation_subquery = sa.select(*keys).group_by(*keys).subquery()

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
    elif is_impala(engine):
        gap_condition = (
            sa.func.datediff(
                sa.func.to_date(start_table.c[start_column]),
                sa.func.to_date(end_table.c[end_column]),
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
):
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
):
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
):
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

    result = engine.connect().execute(violation_tuples).fetchall()
    return result, [violation_tuples]


def get_row_count(
    engine, ref, row_limit: int | None = None
) -> tuple[int, list[sa.Select]]:
    """Return the number of rows for a `DataReference`.

    If `row_limit` is given, the number of rows is capped at the limit.
    """
    subquery = ref.get_selection(engine)
    if row_limit:
        subquery = subquery.limit(row_limit)
    subquery = subquery.alias()
    selection = sa.select(sa.cast(sa.func.count(), sa.BigInteger)).select_from(subquery)
    result = int(str(engine.connect().execute(selection).scalar()))
    return result, [selection]


def get_column(
    engine: sa.engine.Engine,
    ref: DataReference,
    *,
    aggregate_operator: Callable | None = None,
):
    """
    Queries the database for the values of the relevant column (as returned by `get_column(...)`).
    If an aggregation operation is passed, the results are aggregated accordingly
    and a single scalar value is returned.
    """
    subquery = ref.get_selection(engine).alias()
    column = subquery.c[ref.get_column(engine)]

    result: Any | None | Sequence[Any]

    if not aggregate_operator:
        selection = sa.select(column)
        result = engine.connect().execute(selection).scalars().all()

    else:
        selection = sa.select(aggregate_operator(column))
        result = engine.connect().execute(selection).scalar()

    return result, [selection]


def get_min(engine, ref):
    column_operator = sa.func.min
    return get_column(engine, ref, aggregate_operator=column_operator)


def get_max(engine, ref):
    column_operator = sa.func.max
    return get_column(engine, ref, aggregate_operator=column_operator)


def get_mean(engine, ref):
    def column_operator(column):
        if is_impala(engine):
            return sa.func.avg(column)
        return sa.func.avg(sa.cast(column, sa.DECIMAL))

    return get_column(engine, ref, aggregate_operator=column_operator)


def get_percentile(engine, ref, percentage):
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
        counting_subquery.c[row_num] == argmin_selection
    )
    result = engine.connect().execute(percentile_selection).scalar()
    return result, [percentile_selection]


def get_min_length(engine, ref):
    def column_operator(column):
        return sa.func.min(sa.func.length(column))

    return get_column(engine, ref, aggregate_operator=column_operator)


def get_max_length(engine, ref):
    def column_operator(column):
        return sa.func.max(sa.func.length(column))

    return get_column(engine, ref, aggregate_operator=column_operator)


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
) -> tuple[Counter, list[sa.Select]]:
    if not ref.get_columns(engine):
        return Counter({}), []
    selection = ref.get_selection(engine).alias()
    if (column_names := ref.get_columns(engine)) is None:
        raise ValueError("Need columns for get_uniques.")
    columns = [selection.c[column_name] for column_name in column_names]
    selection = sa.select(*columns, sa.func.count()).group_by(*columns)

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


def get_unique_count(engine, ref) -> tuple[int, list[sa.Select]]:
    selection = ref.get_selection(engine)
    subquery = selection.distinct().alias()
    selection = sa.select(sa.func.count()).select_from(subquery)
    result = int(engine.connect().execute(selection).scalar())
    return result, [selection]


def get_unique_count_union(engine, ref, ref2):
    selection1 = ref.get_selection(engine)
    selection2 = ref2.get_selection(engine)
    subquery = sa.sql.union(selection1, selection2).alias().select().distinct().alias()
    selection = sa.select(sa.func.count()).select_from(subquery)
    result = engine.connect().execute(selection).scalar()
    return result, [selection]


def get_missing_fraction(engine, ref):
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

    return (
        n_rows_missing / n_rows_total,
        [n_rows_total_selection, n_rows_missing_selection],
    )


def get_column_names(engine, ref):
    table = ref.data_source.get_clause(engine)
    return [column.name for column in table.columns], None


def get_column_type(engine, ref):
    table = ref.get_selection(engine).alias()
    column_type = next(iter(table.columns)).type
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
    selection = sa.select(sa.func.count()).select_from(subquery)
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

    avg_match_column = sa.func.avg(sa.case((compare, 0.0), else_=1.0))

    selection_difference = sa.select(avg_match_column).select_from(
        subselection1.join(subselection2, match)
    )
    selection_n_rows = sa.select(sa.func.count()).select_from(
        subselection1.join(subselection2, match)
    )
    result_mismatch = engine.connect().execute(selection_difference).scalar()
    result_n_rows = engine.connect().execute(selection_n_rows).scalar()
    return result_mismatch, result_n_rows, [selection_difference, selection_n_rows]


def get_duplicate_sample(engine, ref):
    initial_selection = ref.get_selection(engine).alias()
    aggregate_subquery = (
        sa.select(initial_selection, sa.func.count().label("n_copies"))
        .select_from(initial_selection)
        .group_by(*initial_selection.columns)
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
    result = engine.connect().execute(duplicate_selection).first()
    return result, [duplicate_selection]


def column_array_agg_query(
    engine: sa.engine.Engine, ref: DataReference, aggregation_column: str
):
    clause = ref.data_source.get_clause(engine)
    if not (column_names := ref.get_columns(engine)):
        raise ValueError("There must be a column to group by")
    group_columns = [clause.c[column] for column in column_names]
    agg_column = clause.c[aggregation_column]
    selection = sa.select(*group_columns, sa.func.array_agg(agg_column)).group_by(
        *group_columns
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
    result: Sequence[sa.engine.row.Row[Any]] | list[tuple[Any, ...]] = (
        engine.connect().execute(selections[0]).fetchall()
    )
    if is_snowflake(engine):
        result = [
            (*t[:-1], list(map(int, snowflake_parse_variant_column(t[-1]))))
            for t in result
        ]
    return result, selections


def _cdf_selection(engine, ref: DataReference, cdf_label: str, value_label: str):
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
    engine, ref1: DataReference, ref2: DataReference, cdf_label: str, value_label: str
):
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
):
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

    return d_statistic, [final_selection]


def get_regex_violations(engine, ref, aggregated, regex, n_counterexamples):
    subquery = ref.get_selection(engine)
    column = ref.get_column(engine)
    if aggregated:
        subquery = subquery.distinct()
    subquery = subquery.subquery()
    if is_impala(engine):
        violation_selection = sa.select(subquery.c[column]).where(
            sa.not_(sa.func.regexp_like(subquery.c[column], regex))
        )
    else:
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
        if counterexamples_selection is None:
            counterexamples = []
        else:
            counterexamples_result = connection.execute(
                counterexamples_selection
            ).fetchall()
            counterexamples = [result[0] for result in counterexamples_result]
    return (n_violations_result, counterexamples), selections
