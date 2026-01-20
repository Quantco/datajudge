import functools
from abc import ABC, abstractmethod
from typing import final

import sqlalchemy as sa
from sqlalchemy.sql import selectable

from ._engines import is_mssql


class DataSource(ABC):
    @abstractmethod
    def __str__(self) -> str: ...

    @abstractmethod
    def _get_clause(self, engine: sa.engine.Engine) -> selectable.FromClause: ...


@functools.lru_cache(maxsize=1)
def _get_metadata() -> sa.MetaData:
    return sa.MetaData()


@final
class TableDataSource(DataSource):
    """A ``DataSource`` based on a table."""

    def __init__(
        self,
        db_name: str,
        table_name: str,
        schema_name: str | None = None,
    ):
        self._db_name = db_name
        self._table_name = table_name
        self._schema_name = schema_name

    def __str__(self) -> str:
        if self._schema_name:
            return f"{self._db_name}.{self._schema_name}.{self._table_name}"
        return self._table_name

    def _get_clause(self, engine: sa.engine.Engine) -> sa.Table:
        schema = self._schema_name
        if is_mssql(engine) and self._schema_name:
            schema = self._db_name + "." + self._schema_name

        return sa.Table(
            self._table_name,
            _get_metadata(),
            autoload_with=engine,
            schema=schema,
        )


@final
class ExpressionDataSource(DataSource):
    """A ``DataSource`` based on a sqlalchemy expression."""

    def __init__(
        self,
        expression: selectable.FromClause | selectable.Select,
        name: str,
    ):
        self._expression = expression
        self.name = name

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(expression={self._expression!r}, name={self.name})"

    def _get_clause(self, engine: sa.engine.Engine) -> selectable.FromClause:
        return self._expression.alias()


@final
class RawQueryDataSource(DataSource):
    """A ``DataSource`` based on a SQL query as a string."""

    def __init__(self, query_string: str, name: str, columns: list[str] | None = None):
        self._query_string = query_string
        self.name = name
        self._columns = columns
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
        return f"{self.__class__.__name__}(query_string={self._query_string}, name={self.name}, columns={self._columns})"

    def _get_clause(self, engine: sa.engine.Engine) -> selectable.FromClause:
        return self.clause
