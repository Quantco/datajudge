from __future__ import annotations

from typing import Any

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, TestResult, _OptionalSelections
from .interval import NoGapConstraint, NoOverlapConstraint, _Selects


class NumericMin(Constraint):
    def __init__(
        self,
        ref: DataReference,
        name: str | None = None,
        cache_size=None,
        *,
        ref2: DataReference | None = None,
        min_value: float | None = None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=min_value,
            name=name,
            cache_size=cache_size,
        )

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[float, _OptionalSelections]:
        return db_access.get_min(engine, ref)

    def _compare(
        self, value_factual: float, value_target: float
    ) -> tuple[bool, str | None]:
        if value_target is None:
            return True, None
        if value_factual is None:
            return value_target == 0, "Empty set."
        assertion_text = (
            f"{self._ref} has min "
            f"{value_factual} instead of {self._target_prefix}"
            f"{value_target} . "
            f"{self._condition_string}"
        )
        result = value_factual >= value_target
        return result, assertion_text


class NumericMax(Constraint):
    def __init__(
        self,
        ref: DataReference,
        name: str | None = None,
        cache_size=None,
        *,
        ref2: DataReference | None = None,
        max_value: float | None = None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=max_value,
            name=name,
            cache_size=cache_size,
        )

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[float, _OptionalSelections]:
        return db_access.get_max(engine, ref)

    def _compare(
        self, value_factual: float, value_target: float
    ) -> tuple[bool, str | None]:
        if value_factual is None:
            return True, None
        if value_target is None:
            return value_factual == 0, "Empty reference set."
        assertion_text = (
            f"{self._ref} has max "
            f"{value_factual} instead of {self._target_prefix}"
            f"{value_target}. "
            f"{self._condition_string}"
        )
        result = value_factual <= value_target
        return result, assertion_text


class NumericBetween(Constraint):
    def __init__(
        self,
        ref: DataReference,
        min_fraction: float,
        lower_bound: float,
        upper_bound: float,
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
            engine,
            ref,
            self._lower_bound,
            self._upper_bound,
        )

    def _compare(
        self, value_factual: float, value_target: float
    ) -> tuple[bool, str | None]:
        if value_factual is None:
            return True, "Empty selection."
        assertion_text = (
            f"{self._ref} "
            f"has {value_factual} < {value_target} of rows "
            f"between {self._lower_bound} and {self._upper_bound}. "
            f"{self._condition_string}"
        )
        result = value_factual >= value_target
        return result, assertion_text


class NumericMean(Constraint):
    def __init__(
        self,
        ref: DataReference,
        max_absolute_deviation: float,
        name: str | None = None,
        cache_size=None,
        *,
        ref2: DataReference | None = None,
        mean_value: float | None = None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=mean_value,
            name=name,
            cache_size=cache_size,
        )
        self._max_absolute_deviation = max_absolute_deviation

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[float, _OptionalSelections]:
        result, selections = db_access.get_mean(engine, ref)
        return result, selections

    def test(self, engine: sa.engine.Engine) -> TestResult:
        # ty can't figure out that this is a method and that self is passed
        # as the first argument.
        mean_factual = self._get_factual_value(engine=engine)  # type: ignore[missing-argument]
        # ty can't figure out that this is a method and that self is passed
        # as the first argument.
        mean_target = self._get_target_value(engine=engine)  # type: ignore[missing-argument]

        if mean_factual is None or mean_target is None:
            return TestResult(
                mean_factual is None and mean_target is None,
                "Mean over empty set.",
            )
        deviation = abs(mean_factual - mean_target)
        assertion_text = (
            f"{self._ref} "
            f"has mean {mean_factual}, deviating more than "
            f"{self._max_absolute_deviation} from "
            f"{self._target_prefix} {mean_target}. "
            f"{self._condition_string}"
        )
        result = deviation <= self._max_absolute_deviation
        return TestResult(result, assertion_text)


class NumericPercentile(Constraint):
    def __init__(
        self,
        ref: DataReference,
        percentage: float,
        max_absolute_deviation: float | None = None,
        max_relative_deviation: float | None = None,
        name: str | None = None,
        cache_size=None,
        *,
        ref2: DataReference | None = None,
        expected_percentile: float | None = None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=expected_percentile,
            name=name,
            cache_size=cache_size,
        )
        if not (0 <= percentage <= 100):
            raise ValueError(
                f"Expected percentage to be a value between 0 and 100, got {percentage}."
            )
        self.percentage = percentage
        if max_absolute_deviation is None and max_relative_deviation is None:
            raise ValueError(
                "At least one of 'max_absolute_deviation' and 'max_relative_deviation' "
                "must be given."
            )
        if max_absolute_deviation is not None and max_absolute_deviation < 0:
            raise ValueError(
                f"max_absolute_deviation must be at least 0 but is {max_absolute_deviation}."
            )
        if max_relative_deviation is not None and max_relative_deviation < 0:
            raise ValueError(
                f"max_relative_deviation must be at least 0 but is {max_relative_deviation}."
            )
        self._max_absolute_deviation = max_absolute_deviation
        self._max_relative_deviation = max_relative_deviation

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[float, _OptionalSelections]:
        result, selections = db_access.get_percentile(engine, ref, self.percentage)
        return result, selections

    def _compare(
        self, value_factual: float, value_target: float
    ) -> tuple[bool, str | None]:
        abs_diff = abs(value_factual - value_target)
        if (
            self._max_absolute_deviation is not None
            and abs_diff > self._max_absolute_deviation
        ):
            assertion_message = (
                f"The {self.percentage}-th percentile of {self._ref} was "
                f"expected to be {self._target_prefix}{value_target} but was "
                f"{value_factual}, resulting in an absolute difference of "
                f"{abs_diff}. The maximally allowed absolute deviation would've been "
                f"{self._max_absolute_deviation}."
            )
            return False, assertion_message
        if self._max_relative_deviation is not None:
            if value_target == 0:
                raise ValueError("Cannot compute relative deviation wrt 0.")
            if (
                rel_diff := abs_diff / abs(value_target)
            ) > self._max_relative_deviation:
                assertion_message = (
                    f"The {self.percentage}-th percentile of {self._ref}  was "
                    f"expected to be {self._target_prefix}{value_target} but "
                    f"was {value_factual}, resulting in a relative difference of "
                    f"{rel_diff}. The maximally allowed relative deviation would've been "
                    f"{self._max_relative_deviation}."
                )
                return False, assertion_message
        return True, None


class NumericNoGap(NoGapConstraint):
    _DIMENSIONS = 1

    def _select(self, engine: sa.engine.Engine, ref: DataReference) -> _Selects:
        sample_selection, n_violations_selection = db_access.get_numeric_gaps(
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
    ) -> tuple[bool, str]:
        n_violation_keys, n_distinct_key_values = value_factual
        if n_distinct_key_values == 0:
            return True, "No key values found."
        violation_fraction = n_violation_keys / n_distinct_key_values
        assertion_text = (
            f"{self._ref} has a ratio of {violation_fraction} > "
            f"{self._max_relative_n_violations} keys in columns {self._key_columns} "
            f"with a gap in the range in {self._start_columns[0]} and {self._end_columns[0]}."
            f"E.g. for: {self.sample}."
        )
        result = violation_fraction <= self._max_relative_n_violations
        return result, assertion_text


class NumericNoOverlap(NoOverlapConstraint):
    _DIMENSIONS = 1

    def _compare(
        self, value_factual: tuple[int, int], value_target: Any
    ) -> tuple[bool, str]:
        n_violation_keys, n_distinct_key_values = value_factual
        if n_distinct_key_values == 0:
            return True, "No key values found."
        violation_fraction = n_violation_keys / n_distinct_key_values
        assertion_text = (
            f"{self._ref} has a ratio of {violation_fraction} > "
            f"{self._max_relative_n_violations} keys in columns {self._key_columns} "
            f"with overlapping ranges in {self._start_columns[0]} and {self._end_columns[0]}."
            f"E.g. for: {self.sample}."
        )
        result = violation_fraction <= self._max_relative_n_violations
        return result, assertion_text
