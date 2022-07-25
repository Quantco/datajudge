import itertools
import re
from typing import Optional, Tuple

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, OptionalSelections, TestResult


class VarCharRegex(Constraint):
    def __init__(
        self,
        ref: DataReference,
        regex: str,
        allow_none: bool = False,
        relative_tolerance: float = 0.0,
        aggregated: bool = True,
        n_counterexamples: int = 5,
    ):
        super().__init__(ref, ref_value=regex)
        self.allow_none = allow_none
        self.relative_tolerance = relative_tolerance
        self.aggregated = aggregated
        self.n_counterexamples = n_counterexamples

    def test(self, engine: sa.engine.Engine) -> TestResult:
        uniques_counter, selections = db_access.get_uniques(engine, self.ref)
        self.factual_selections = selections
        if not self.allow_none and uniques_counter.get(None):
            return TestResult.failure(
                "The column contains a None value when it's not allowed. "
                "To ignore None values, please use `allow_none=True` option."
            )
        elif None in uniques_counter:
            uniques_counter.pop(None)

        uniques_factual = list(uniques_counter.keys())
        if not self.ref_value:
            return TestResult.failure("No regex pattern given")

        pattern = re.compile(self.ref_value)
        uniques_mismatching = {
            x for x in uniques_factual if not pattern.match(x)  # type: ignore
        }

        if self.aggregated:
            n_violations = len(uniques_mismatching)
            n_total = len(uniques_factual)
        else:
            n_violations = sum(uniques_counter[key] for key in uniques_mismatching)
            n_total = sum(count for _, count in uniques_counter.items())

        n_relative_violations = n_violations / n_total

        if self.n_counterexamples == -1:
            counterexamples = list(uniques_mismatching)
        else:
            counterexamples = list(
                itertools.islice(uniques_mismatching, self.n_counterexamples)
            )

        if n_relative_violations > self.relative_tolerance:
            assertion_text = (
                f"{self.ref.get_string()} "
                f"breaks regex '{self.ref_value}' in {n_relative_violations} > "
                f"{self.relative_tolerance} of the cases. "
                f"In absolute terms, {n_violations} of the {n_total} samples violated the regex. "
                f"Some counterexamples consist of the following: {counterexamples}. "
                f"{self.condition_string}"
            )
            return TestResult.failure(assertion_text)
        return TestResult.success()


class VarCharMinLength(Constraint):
    def __init__(self, ref, *, ref2: DataReference = None, min_length: int = None):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=min_length,
        )

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[int, OptionalSelections]:
        return db_access.get_min_length(engine, ref)

    def compare(
        self, length_factual: int, length_target: int
    ) -> Tuple[bool, Optional[str]]:
        if length_target is None:
            return True, None
        if length_factual is None:
            return length_target == 0, "Empty set."
        assertion_text = (
            f"{self.ref.get_string()} "
            f"has min length {length_factual} instead of "
            f"{self.target_prefix} {length_target}. "
            f"{self.condition_string}"
        )
        result = length_factual >= length_target
        return result, assertion_text


class VarCharMaxLength(Constraint):
    def __init__(
        self, ref: DataReference, *, ref2: DataReference = None, max_length: int = None
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=max_length,
        )

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[int, OptionalSelections]:
        return db_access.get_max_length(engine, ref)

    def compare(
        self, length_factual: int, length_target: int
    ) -> Tuple[bool, Optional[str]]:
        if length_factual is None:
            return True, None
        if length_target is None:
            return length_factual == 0, "Reference value is None."
        assertion_text = (
            f"{self.ref.get_string()} "
            f"has max length {length_factual} instead of "
            f"{self.target_prefix} {length_target}. "
            f"{self.condition_string}"
        )
        result = length_factual <= length_target
        return result, assertion_text
