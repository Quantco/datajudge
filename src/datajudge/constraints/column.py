from __future__ import annotations

import abc

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference, is_snowflake, lowercase_column_names
from .base import Constraint, _OptionalSelections


class Column(Constraint, abc.ABC):
    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[list[str], _OptionalSelections]:
        # TODO: This does not 'belong' here. Rather, `retrieve` should be free of
        # side effects. This should be removed as soon as snowflake column capitalization
        # is fixed by snowflake-sqlalchemy.
        if is_snowflake(engine) and self._ref_value is not None:
            self._ref_value = lowercase_column_names(self._ref_value)
        return db_access.get_column_names(engine, ref)


class ColumnExistence(Column):
    def __init__(
        self,
        ref: DataReference,
        columns: list[str],
        name: str | None = None,
        cache_size=None,
    ):
        super().__init__(ref, ref_value=columns, name=name, cache_size=cache_size)

    def _compare(
        self, value_factual: list[str], value_target: list[str]
    ) -> tuple[bool, str]:
        excluded_columns = list(filter(lambda c: c not in value_factual, value_target))
        assertion_message = (
            f"{self._ref} doesn't have column(s) {', '.join(excluded_columns)}."
        )
        result = len(excluded_columns) == 0
        return result, assertion_message


class ColumnSubset(Column):
    def _compare(
        self, value_factual: list[str], value_target: list[str]
    ) -> tuple[bool, str]:
        missing_columns = list(filter(lambda c: c not in value_target, value_factual))
        assertion_message = (
            f"{self._ref2} doesn't have column(s) {', '.join(missing_columns)}. "
        )
        result = len(missing_columns) == 0
        return result, assertion_message


class ColumnSuperset(Column):
    def _compare(
        self, value_factual: list[str], value_target: list[str]
    ) -> tuple[bool, str]:
        missing_columns = list(filter(lambda c: c not in value_factual, value_target))
        assertion_message = (
            f"{self._ref} doesn't have column(s) {', '.join(missing_columns)}."
        )
        result = len(missing_columns) == 0
        return result, assertion_message


class ColumnType(Constraint):
    """A class used to represent a ColumnType constraint.

    This class enables flexible specification of column types either in string format or using SQLAlchemy's type hierarchy.
    It checks whether a column's type matches the specified type, allowing for checks against backend-specific types,
    SQLAlchemy's generic types, or string representations of backend-specific types.

    When using SQLAlchemy's generic types, the comparison is done using `isinstance`, which means that the actual type can also be a subclass of the target type.
    For more information, see https://docs.sqlalchemy.org/en/20/core/type_basics.html
    """

    def __init__(
        self,
        ref: DataReference,
        *,
        ref2: DataReference | None = None,
        column_type: str | sa.types.TypeEngine | None = None,
        name: str | None = None,
        cache_size=None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=column_type,
            name=name,
            cache_size=cache_size,
        )

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[sa.types.TypeEngine, _OptionalSelections]:
        result, selections = db_access.get_column_type(engine, ref)
        return result, selections

    def _compare(self, value_factual, value_target) -> tuple[bool, str]:
        assertion_message = f"{self._ref} is {value_factual} instead of {value_target}."

        if isinstance(value_target, sa.types.TypeEngine):
            result = isinstance(value_factual, type(value_target))
        else:
            column_type = str(value_factual).lower()
            # Integer columns loaded from snowflake database may be referred to as decimal with
            # 0 scale. More here:
            # https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#decimal-numeric
            if column_type == "decimal(38, 0)":
                column_type = "integer"
            result = column_type.startswith(value_target.lower())
        return result, assertion_message
