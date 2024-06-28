import datetime as dt
from typing import Any, Optional, Tuple, Union

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, OptionalSelections, TestResult
from .interval import NoGapConstraint, NoOverlapConstraint

INPUT_DATE_FORMAT = "'%Y-%m-%d'"

Date = Union[str, dt.date, dt.datetime]


def get_format_from_column_type(column_type: str) -> str:
    if column_type.lower() == "date":
        return "%Y-%m-%d"
    if column_type.lower() in ["datetime", "datetime2", "smalldatetime"]:
        return "%Y-%m-%d %H:%M:%S"
    raise ValueError(f"Illegal date column type: {column_type}")


def convert_to_date(db_result: Date, format: str) -> dt.date:
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
        name: Optional[str] = None,
        cache_size=None,
        *,
        ref2: Optional[DataReference] = None,
        min_value: Optional[str] = None,
    ):
        self.format = get_format_from_column_type(column_type)
        self.use_lower_bound_reference = use_lower_bound_reference
        min_date: Optional[dt.date] = None
        if min_value is not None:
            min_date = dt.datetime.strptime(min_value, INPUT_DATE_FORMAT).date()
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=min_date,
            name=name,
            cache_size=cache_size,
        )

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[dt.date, OptionalSelections]:
        result, selections = db_access.get_min(engine, ref)
        return convert_to_date(result, self.format), selections

    def compare(self, min_factual: dt.date, min_target: dt.date) -> Tuple[bool, str]:
        if min_target is None:
            return TestResult(True, "")
        if min_factual is None:
            return TestResult(min_target == 0, "Empty set.")
        if self.use_lower_bound_reference:
            assertion_text = (
                f"{self.ref} has min {min_factual} < "
                f"{self.target_prefix} {min_target}. "
                f"{self.condition_string}"
            )
            result = min_factual >= min_target
        else:
            assertion_text = (
                f"{self.ref} has min {min_factual} > "
                f"{self.target_prefix} {min_target}. "
                f"{self.condition_string}"
            )
            result = min_factual <= min_target
        return result, assertion_text


class DateMax(Constraint):
    def __init__(
        self,
        ref: DataReference,
        use_upper_bound_reference: bool,
        column_type: str,
        name: Optional[str] = None,
        cache_size=None,
        *,
        ref2: Optional[DataReference] = None,
        max_value: Optional[str] = None,
    ):
        self.format = get_format_from_column_type(column_type)
        self.use_upper_bound_reference = use_upper_bound_reference
        max_date: Optional[dt.date] = None
        if max_value is not None:
            max_date = dt.datetime.strptime(max_value, INPUT_DATE_FORMAT).date()
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=max_date,
            name=name,
            cache_size=cache_size,
        )

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[dt.date, OptionalSelections]:
        value, selections = db_access.get_max(engine, ref)
        return convert_to_date(value, self.format), selections

    def compare(self, max_factual: dt.date, max_target: dt.date) -> Tuple[bool, str]:
        if max_factual is None:
            return True, None
        if max_target is None:
            return max_factual == 0, "Empty reference set."
        if self.use_upper_bound_reference:
            assertion_text = (
                f"{self.ref} has max {max_factual} > "
                f"{self.target_prefix} {max_target}. "
                f"{self.condition_string}"
            )
            result = max_factual <= max_target
        else:
            assertion_text = (
                f"{self.ref} has max {max_factual} < "
                f"{self.target_prefix} {max_target}. "
                f"{self.condition_string}"
            )
            result = max_factual >= max_target

        return result, assertion_text


class DateBetween(Constraint):
    def __init__(
        self,
        ref: DataReference,
        min_fraction: float,
        lower_bound: str,
        upper_bound: str,
        name: Optional[str] = None,
        cache_size=None,
    ):
        super().__init__(ref, ref_value=min_fraction, name=name, cache_size=cache_size)
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[float, OptionalSelections]:
        return db_access.get_fraction_between(
            engine, ref, self.lower_bound, self.upper_bound
        )

    def compare(
        self, fraction_factual: float, fraction_target: float
    ) -> Tuple[bool, str]:
        assertion_text = (
            f"{self.ref} has {fraction_factual} < "
            f"{fraction_target} of values between {self.lower_bound} and "
            f"{self.upper_bound}. {self.condition_string} "
        )
        result = fraction_factual >= fraction_target
        return result, assertion_text


class DateNoOverlap(NoOverlapConstraint):
    _DIMENSIONS = 1

    def compare(self, factual: Tuple[int, int], target: Any) -> Tuple[bool, str]:
        n_violation_keys, n_distinct_key_values = factual
        if n_distinct_key_values == 0:
            return TestResult.success()
        violation_fraction = n_violation_keys / n_distinct_key_values
        assertion_text = (
            f"{self.ref} has a ratio of {violation_fraction} > "
            f"{self.max_relative_n_violations} keys in columns {self.key_columns} "
            f"with overlapping date ranges in {self.start_columns[0]} and {self.end_columns[0]}."
            f"E.g. for: {self.sample}."
        )
        result = violation_fraction <= self.max_relative_n_violations
        return result, assertion_text


class DateNoOverlap2d(NoOverlapConstraint):
    _DIMENSIONS = 2

    def compare(self, factual: Tuple[int, int], target: Any) -> Tuple[bool, str]:
        n_violation_keys, n_distinct_key_values = factual
        if n_distinct_key_values == 0:
            return TestResult.success()
        violation_fraction = n_violation_keys / n_distinct_key_values
        assertion_text = (
            f"{self.ref} has a ratio of {violation_fraction} > "
            f"{self.max_relative_n_violations} keys in columns {self.key_columns} "
            f"with overlapping date ranges in {self.start_columns[0]} and {self.end_columns[0]}."
            f"and {self.start_columns[1]} and {self.end_columns[1]}."
            f"E.g. for: {self.sample}."
        )
        result = violation_fraction <= self.max_relative_n_violations
        return result, assertion_text


class DateNoGap(NoGapConstraint):
    _DIMENSIONS = 1

    def select(self, engine: sa.engine.Engine, ref: DataReference):
        sample_selection, n_violations_selection = db_access.get_date_gaps(
            engine,
            ref,
            self.key_columns,
            self.start_columns[0],
            self.end_columns[0],
            self.legitimate_gap_size,
        )
        # TODO: Once get_unique_count also only returns a selection without
        # executing it, one would want to list this selection here as well.
        return sample_selection, n_violations_selection

    def compare(self, factual: Tuple[int, int], target: Any) -> Tuple[bool, str]:
        n_violation_keys, n_distinct_key_values = factual
        if n_distinct_key_values == 0:
            return TestResult.success()
        violation_fraction = n_violation_keys / n_distinct_key_values
        assertion_text = (
            f"{self.ref} has a ratio of {violation_fraction} > "
            f"{self.max_relative_n_violations} keys in columns {self.key_columns} "
            f"with a gap in the date range in {self.start_columns[0]} and {self.end_columns[0]}."
            f"E.g. for: {self.sample}."
        )
        result = violation_fraction <= self.max_relative_n_violations
        return result, assertion_text
