from __future__ import annotations

import datetime as dt
from typing import Any

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, _OptionalSelections
from .interval import NoGapConstraint, NoOverlapConstraint, _Selects

_INPUT_DATE_FORMAT = "'%Y-%m-%d'"

Date = str | dt.date | dt.datetime


def _get_format_from_column_type(column_type: str) -> str:
    if column_type.lower() == "date":
        return "%Y-%m-%d"
    if column_type.lower() in ["datetime", "datetime2", "smalldatetime"]:
        return "%Y-%m-%d %H:%M:%S"
    raise ValueError(f"Illegal date column type: {column_type}")


def _convert_to_date(db_result: Date, format: str) -> dt.date:
    if isinstance(db_result, dt.datetime):
        return db_result.date()
    if isinstance(db_result, dt.date):
        return db_result
    if isinstance(db_result, str):
        # Get rid of nanoseconds as they cannot be parsed by strptime.
        return dt.datetime.strptime(db_result.split(".")[0], format).date()
    raise TypeError(f"Value hast type {type(db_result)} cannot be converted to date.")


class DateMin(Constraint):
    def __init__(
        self,
        ref: DataReference,
        use_lower_bound_reference: bool,
        column_type: str,
        name: str | None = None,
        cache_size=None,
        *,
        ref2: DataReference | None = None,
        min_value: str | None = None,
    ):
        self._format = _get_format_from_column_type(column_type)
        self._use_lower_bound_reference = use_lower_bound_reference
        min_date: dt.date | None = None
        if min_value is not None:
            min_date = dt.datetime.strptime(min_value, _INPUT_DATE_FORMAT).date()
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=min_date,
            name=name,
            cache_size=cache_size,
        )

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[dt.date, _OptionalSelections]:
        result, selections = db_access.get_min(engine, ref)
        return _convert_to_date(result, self._format), selections

    def _compare(
        self, value_factual: dt.date, value_target: dt.date
    ) -> tuple[bool, str | None]:
        if value_target is None:
            return True, None
        if value_factual is None:
            return value_target == 0, "Empty set."
        if self._use_lower_bound_reference:
            assertion_text = (
                f"{self._ref} has min {value_factual} < "
                f"{self._target_prefix} {value_target}. "
                f"{self._condition_string}"
            )
            result = value_factual >= value_target
        else:
            assertion_text = (
                f"{self._ref} has min {value_factual} > "
                f"{self._target_prefix} {value_target}. "
                f"{self._condition_string}"
            )
            result = value_factual <= value_target
        return result, assertion_text


class DateMax(Constraint):
    def __init__(
        self,
        ref: DataReference,
        use_upper_bound_reference: bool,
        column_type: str,
        name: str | None = None,
        cache_size=None,
        *,
        ref2: DataReference | None = None,
        max_value: str | None = None,
    ):
        self._format = _get_format_from_column_type(column_type)
        self._use_upper_bound_reference = use_upper_bound_reference
        max_date: dt.date | None = None
        if max_value is not None:
            max_date = dt.datetime.strptime(max_value, _INPUT_DATE_FORMAT).date()
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=max_date,
            name=name,
            cache_size=cache_size,
        )

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[dt.date, _OptionalSelections]:
        value, selections = db_access.get_max(engine, ref)
        return _convert_to_date(value, self._format), selections

    def _compare(
        self, value_factual: dt.date, value_target: dt.date
    ) -> tuple[bool, str | None]:
        if value_factual is None:
            return True, None
        if value_target is None:
            return value_factual == 0, "Empty reference set."
        if self._use_upper_bound_reference:
            assertion_text = (
                f"{self._ref} has max {value_factual} > "
                f"{self._target_prefix} {value_target}. "
                f"{self._condition_string}"
            )
            result = value_factual <= value_target
        else:
            assertion_text = (
                f"{self._ref} has max {value_factual} < "
                f"{self._target_prefix} {value_target}. "
                f"{self._condition_string}"
            )
            result = value_factual >= value_target

        return result, assertion_text


class DateBetween(Constraint):
    def __init__(
        self,
        ref: DataReference,
        min_fraction: float,
        lower_bound: str,
        upper_bound: str,
        name: str | None = None,
        cache_size=None,
    ):
        super().__init__(ref, ref_value=min_fraction, name=name, cache_size=cache_size)
        self._lower_bound = lower_bound
        self._upper_bound = upper_bound

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[float | None, _OptionalSelections]:
        return db_access.get_fraction_between(
            engine, ref, self._lower_bound, self._upper_bound
        )

    def _compare(self, value_factual: float, value_target: float) -> tuple[bool, str]:
        assertion_text = (
            f"{self._ref} has {value_factual} < "
            f"{value_target} of values between {self._lower_bound} and "
            f"{self._upper_bound}. {self._condition_string} "
        )
        result = value_factual >= value_target
        return result, assertion_text


class DateNoOverlap(NoOverlapConstraint):
    _DIMENSIONS = 1

    def _compare(
        self, value_factual: tuple[int, int], value_target: Any
    ) -> tuple[bool, str | None]:
        n_violation_keys, n_distinct_key_values = value_factual
        if n_distinct_key_values == 0:
            return True, None
        violation_fraction = n_violation_keys / n_distinct_key_values
        assertion_text = (
            f"{self._ref} has a ratio of {violation_fraction} > "
            f"{self._max_relative_n_violations} keys in columns {self._key_columns} "
            f"with overlapping date ranges in {self._start_columns[0]} and {self._end_columns[0]}."
            f"E.g. for: {self.sample}."
        )
        result = violation_fraction <= self._max_relative_n_violations
        return result, assertion_text


class DateNoOverlap2d(NoOverlapConstraint):
    _DIMENSIONS = 2

    def _compare(
        self, value_factual: tuple[int, int], value_target: Any
    ) -> tuple[bool, str | None]:
        n_violation_keys, n_distinct_key_values = value_factual
        if n_distinct_key_values == 0:
            return True, None
        violation_fraction = n_violation_keys / n_distinct_key_values
        assertion_text = (
            f"{self._ref} has a ratio of {violation_fraction} > "
            f"{self._max_relative_n_violations} keys in columns {self._key_columns} "
            f"with overlapping date ranges in {self._start_columns[0]} and {self._end_columns[0]}."
            f"and {self._start_columns[1]} and {self._end_columns[1]}."
            f"E.g. for: {self.sample}."
        )
        result = violation_fraction <= self._max_relative_n_violations
        return result, assertion_text


class DateNoGap(NoGapConstraint):
    _DIMENSIONS = 1

    def _select(self, engine: sa.engine.Engine, ref: DataReference) -> _Selects:
        sample_selection, n_violations_selection = db_access.get_date_gaps(
            engine,
            ref,
            self._key_columns,
            self._start_columns[0],
            self._end_columns[0],
            self._legitimate_gap_size,
        )
        # TODO: Once get_unique_count also only returns a selection without
        # executing it, one would want to list this selection here as well.
        return sample_selection, n_violations_selection

    def _compare(
        self, value_factual: tuple[int, int], value_target: Any
    ) -> tuple[bool, str | None]:
        n_violation_keys, n_distinct_key_values = value_factual
        if n_distinct_key_values == 0:
            return True, None
        violation_fraction = n_violation_keys / n_distinct_key_values
        assertion_text = (
            f"{self._ref} has a ratio of {violation_fraction} > "
            f"{self._max_relative_n_violations} keys in columns {self._key_columns} "
            f"with a gap in the date range in {self._start_columns[0]} and {self._end_columns[0]}."
            f"E.g. for: {self.sample}."
        )
        result = violation_fraction <= self._max_relative_n_violations
        return result, assertion_text
