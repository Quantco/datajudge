import abc
from typing import List, Optional, Tuple, Union

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference, is_snowflake, lowercase_column_names
from .base import Constraint, OptionalSelections


class Column(Constraint, abc.ABC):
    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[List[str], OptionalSelections]:
        # TODO: This does not 'belong' here. Rather, `retrieve` should be free of
        # side effects. This should be removed as soon as snowflake column capitalization
        # is fixed by snowflake-sqlalchemy.
        if is_snowflake(engine) and self.ref_value is not None:
            self.ref_value = lowercase_column_names(self.ref_value)  # type: ignore
        return db_access.get_column_names(engine, ref)


class ColumnExistence(Column):
    def __init__(
        self,
        ref: DataReference,
        columns: List[str],
        name: Optional[str] = None,
        cache_size=None,
    ):
        super().__init__(ref, ref_value=columns, name=name, cache_size=cache_size)

    def compare(
        self, column_names_factual: List[str], column_names_target: List[str]
    ) -> Tuple[bool, str]:
        excluded_columns = list(
            filter(lambda c: c not in column_names_factual, column_names_target)
        )
        assertion_message = (
            f"{self.ref} doesn't have column(s) " f"{', '.join(excluded_columns)}."
        )
        result = len(excluded_columns) == 0
        return result, assertion_message


class ColumnSubset(Column):
    def compare(
        self, column_names_factual: List[str], column_names_target: List[str]
    ) -> Tuple[bool, str]:
        missing_columns = list(
            filter(lambda c: c not in column_names_target, column_names_factual)
        )
        assertion_message = (
            f"{self.ref2} doesn't have column(s) " f"{', '.join(missing_columns)}. "
        )
        result = len(missing_columns) == 0
        return result, assertion_message


class ColumnSuperset(Column):
    def compare(
        self, column_names_factual: List[str], column_names_target: List[str]
    ) -> Tuple[bool, str]:
        missing_columns = list(
            filter(lambda c: c not in column_names_factual, column_names_target)
        )
        assertion_message = (
            f"{self.ref} doesn't have column(s) " f"{', '.join(missing_columns)}."
        )
        result = len(missing_columns) == 0
        return result, assertion_message


class ColumnType(Constraint):
    """
    A class used to represent a ColumnType constraint.

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
        ref2: Optional[DataReference] = None,
        column_type: Optional[Union[str, sa.types.TypeEngine]] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=column_type,
            name=name,
            cache_size=cache_size,
        )
        self.column_type = column_type

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[sa.types.TypeEngine, OptionalSelections]:
        result, selections = db_access.get_column_type(engine, ref)
        return result, selections

    def compare(self, column_type_factual, column_type_target) -> Tuple[bool, str]:
        assertion_message = (
            f"{self.ref} is {column_type_factual} " f"instead of {column_type_target}."
        )

        if isinstance(column_type_target, sa.types.TypeEngine):
            result = isinstance(column_type_factual, type(column_type_target))
        else:
            column_type = str(column_type_factual).lower()
            # Integer columns loaded from snowflake database may be referred to as decimal with
            # 0 scale. More here:
            # https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#decimal-numeric
            if column_type == "decimal(38, 0)":
                column_type = "integer"
            result = column_type.startswith(column_type_target.lower())
        return result, assertion_message
