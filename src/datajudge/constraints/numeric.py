from typing import Any, Optional, Tuple

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, OptionalSelections, TestResult
from .interval import NoGapConstraint, NoOverlapConstraint


class NumericMin(Constraint):
    def __init__(
        self,
        ref: DataReference,
        name: Optional[str] = None,
        cache_size=None,
        *,
        ref2: Optional[DataReference] = None,
        min_value: Optional[float] = None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=min_value,
            name=name,
            cache_size=cache_size,
        )

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[float, OptionalSelections]:
        return db_access.get_min(engine, ref)

    def compare(
        self, min_factual: float, min_target: float
    ) -> Tuple[bool, Optional[str]]:
        if min_target is None:
            return True, None
        if min_factual is None:
            return min_target == 0, "Empty set."
        assertion_text = (
            f"{self.ref} has min "
            f"{min_factual} instead of {self.target_prefix}"
            f"{min_target} . "
            f"{self.condition_string}"
        )
        result = min_factual >= min_target
        return result, assertion_text


class NumericMax(Constraint):
    def __init__(
        self,
        ref: DataReference,
        name: Optional[str] = None,
        cache_size=None,
        *,
        ref2: Optional[DataReference] = None,
        max_value: Optional[float] = None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=max_value,
            name=name,
            cache_size=cache_size,
        )

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[float, OptionalSelections]:
        return db_access.get_max(engine, ref)

    def compare(
        self, max_factual: float, max_target: float
    ) -> Tuple[bool, Optional[str]]:
        if max_factual is None:
            return True, None
        if max_target is None:
            return max_factual == 0, "Empty reference set."
        assertion_text = (
            f"{self.ref} has max "
            f"{max_factual} instead of {self.target_prefix}"
            f"{max_target}. "
            f"{self.condition_string}"
        )
        result = max_factual <= max_target
        return result, assertion_text


class NumericBetween(Constraint):
    def __init__(
        self,
        ref: DataReference,
        min_fraction: float,
        lower_bound: float,
        upper_bound: float,
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
            engine,
            ref,
            self.lower_bound,
            self.upper_bound,
        )

    def compare(
        self, fraction_factual: float, fraction_target: float
    ) -> Tuple[bool, Optional[str]]:
        if fraction_factual is None:
            return True, "Empty selection."
        assertion_text = (
            f"{self.ref} "
            f"has {fraction_factual} < {fraction_target} of rows "
            f"between {self.lower_bound} and {self.upper_bound}. "
            f"{self.condition_string}"
        )
        result = fraction_factual >= fraction_target
        return result, assertion_text


class NumericMean(Constraint):
    def __init__(
        self,
        ref: DataReference,
        max_absolute_deviation: float,
        name: Optional[str] = None,
        cache_size=None,
        *,
        ref2: Optional[DataReference] = None,
        mean_value: Optional[float] = None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=mean_value,
            name=name,
            cache_size=cache_size,
        )
        self.max_absolute_deviation = max_absolute_deviation

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[float, OptionalSelections]:
        result, selections = db_access.get_mean(engine, ref)
        return result, selections

    def test(self, engine: sa.engine.Engine) -> TestResult:
        mean_factual = self.get_factual_value(engine)
        mean_target = self.get_target_value(engine)
        if mean_factual is None or mean_target is None:
            return TestResult(
                mean_factual is None and mean_target is None,
                "Mean over empty set.",
            )
        deviation = abs(mean_factual - mean_target)
        assertion_text = (
            f"{self.ref} "
            f"has mean {mean_factual}, deviating more than "
            f"{self.max_absolute_deviation} from "
            f"{self.target_prefix} {mean_target}. "
            f"{self.condition_string}"
        )
        result = deviation <= self.max_absolute_deviation
        return TestResult(result, assertion_text)


class NumericPercentile(Constraint):
    def __init__(
        self,
        ref: DataReference,
        percentage: float,
        max_absolute_deviation: Optional[float] = None,
        max_relative_deviation: Optional[float] = None,
        name: Optional[str] = None,
        cache_size=None,
        *,
        ref2: Optional[DataReference] = None,
        expected_percentile: Optional[float] = None,
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
        self.max_absolute_deviation = max_absolute_deviation
        self.max_relative_deviation = max_relative_deviation

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[float, OptionalSelections]:
        result, selections = db_access.get_percentile(engine, ref, self.percentage)
        return result, selections

    def compare(
        self, percentile_factual: float, percentile_target: float
    ) -> Tuple[bool, Optional[str]]:
        abs_diff = abs(percentile_factual - percentile_target)
        if (
            self.max_absolute_deviation is not None
            and abs_diff > self.max_absolute_deviation
        ):
            assertion_message = (
                f"The {self.percentage}-th percentile of {self.ref} was "
                f"expected to be {self.target_prefix}{percentile_target} but was "
                f"{percentile_factual}, resulting in an absolute difference of "
                f"{abs_diff}. The maximally allowed absolute deviation would've been "
                f"{self.max_absolute_deviation}."
            )
            return False, assertion_message
        if self.max_relative_deviation is not None:
            if percentile_target == 0:
                raise ValueError("Cannot compute relative deviation wrt 0.")
            if (
                rel_diff := abs_diff / abs(percentile_target)
            ) > self.max_relative_deviation:
                assertion_message = (
                    f"The {self.percentage}-th percentile of {self.ref}  was "
                    f"expected to be {self.target_prefix}{percentile_target} but "
                    f"was {percentile_factual}, resulting in a relative difference of "
                    f"{rel_diff}. The maximally allowed relative deviation would've been "
                    f"{self.max_relative_deviation}."
                )
                return False, assertion_message
        return True, None


class NumericNoGap(NoGapConstraint):
    _DIMENSIONS = 1

    def select(self, engine: sa.engine.Engine, ref: DataReference):
        sample_selection, n_violations_selection = db_access.get_numeric_gaps(
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
            f"with a gap in the range in {self.start_columns[0]} and {self.end_columns[0]}."
            f"E.g. for: {self.sample}."
        )
        result = violation_fraction <= self.max_relative_n_violations
        return result, assertion_text


class NumericNoOverlap(NoOverlapConstraint):
    _DIMENSIONS = 1

    def compare(self, factual: Tuple[int, int], target: Any) -> Tuple[bool, str]:
        n_violation_keys, n_distinct_key_values = factual
        if n_distinct_key_values == 0:
            return TestResult.success()
        violation_fraction = n_violation_keys / n_distinct_key_values
        assertion_text = (
            f"{self.ref} has a ratio of {violation_fraction} > "
            f"{self.max_relative_n_violations} keys in columns {self.key_columns} "
            f"with overlapping ranges in {self.start_columns[0]} and {self.end_columns[0]}."
            f"E.g. for: {self.sample}."
        )
        result = violation_fraction <= self.max_relative_n_violations
        return result, assertion_text
