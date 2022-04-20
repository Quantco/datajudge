import abc
from typing import List, Tuple

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
    def __init__(self, ref: DataReference, columns: List[str]):
        super().__init__(ref, ref_value=columns)

    def compare(
        self, column_names_factual: List[str], column_names_target: List[str]
    ) -> Tuple[bool, str]:
        excluded_columns = list(
            filter(lambda c: c not in column_names_factual, column_names_target)
        )
        assertion_message = (
            f"{self.ref.get_string()} doesn't have column(s) "
            f"{', '.join(excluded_columns)}."
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
            f"{self.ref2.get_string()} doesn't have column(s) "
            f"{', '.join(missing_columns)}. "
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
            f"{self.ref.get_string()} doesn't have column(s) "
            f"{', '.join(missing_columns)}."
        )
        result = len(missing_columns) == 0
        return result, assertion_message


class ColumnType(Constraint):
    def __init__(self, ref: DataReference, column_type: str):
        super().__init__(ref, ref_value=column_type.lower())
        self.column_type = column_type

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[str, OptionalSelections]:
        result, selections = db_access.get_column_type(engine, ref)
        return result.lower(), selections

    def compare(self, column_type_factual, column_type_target) -> Tuple[bool, str]:
        assertion_message = (
            f"{self.ref.get_string()} is {column_type_factual} "
            f"instead of {column_type_target}."
        )
        result = column_type_factual.startswith(column_type_target)
        return result, assertion_message
