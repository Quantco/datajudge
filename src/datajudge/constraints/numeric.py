from typing import Optional, Tuple

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, OptionalSelections, TestResult


class NumericMin(Constraint):
    def __init__(
        self,
        ref: DataReference,
        *,
        ref2: DataReference = None,
        min_value: float = None,
    ):
        super().__init__(ref, ref2=ref2, ref_value=min_value)

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
            f"{self.ref.get_string()} has min "
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
        *,
        ref2: DataReference = None,
        max_value: float = None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=max_value,
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
            f"{self.ref.get_string()} has max "
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
    ):
        super().__init__(ref, ref_value=min_fraction)
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
            f"{self.ref.get_string()} "
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
        *,
        ref2: DataReference = None,
        mean_value: float = None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=mean_value,
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
            f"{self.ref.get_string()} "
            f"has mean {mean_factual}, deviating more than "
            f"{self.max_absolute_deviation} from "
            f"{self.target_prefix} {mean_target}. "
            f"{self.condition_string}"
        )
        result = deviation <= self.max_absolute_deviation
        return TestResult(result, assertion_text)
