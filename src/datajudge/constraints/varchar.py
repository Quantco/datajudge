import itertools
import re
from typing import Optional, Tuple

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, OptionalSelections, TestResult


class VarCharRegexDb(Constraint):
    def __init__(
        self,
        ref: DataReference,
        regex: str,
        relative_tolerance: float = 0.0,
        aggregated: bool = True,
        n_counterexamples: int = 5,
        name: Optional[str] = None,
        cache_size=None,
    ):
        super().__init__(
            ref,
            ref_value=relative_tolerance,
            name=name,
            cache_size=cache_size,
        )
        self.regex = regex
        self.aggregated = aggregated
        self.n_counterexamples = n_counterexamples

    def retrieve(self, engine: sa.engine.Engine, ref: DataReference):
        (
            (
                n_violations,
                counterexamples,
            ),
            violations_selections,
        ) = db_access.get_regex_violations(
            engine=engine,
            ref=ref,
            aggregated=self.aggregated,
            regex=self.regex,
            n_counterexamples=self.n_counterexamples,
        )
        if self.aggregated:
            n_rows, n_rows_selections = db_access.get_unique_count(
                engine=engine, ref=ref
            )
        else:
            n_rows, n_rows_selections = db_access.get_row_count(engine=engine, ref=ref)
        return (
            n_violations,
            n_rows,
            counterexamples,
        ), violations_selections + n_rows_selections

    def compare(self, violations_factual, violations_target):
        (
            factual_n_violations,
            factual_n_rows,
            factual_counterexamples,
        ) = violations_factual
        factual_relative_violations = factual_n_violations / factual_n_rows
        result = factual_relative_violations <= violations_target
        counterexample_string = (
            (
                "Some counterexamples consist of the following: "
                f"{factual_counterexamples}. "
            )
            if factual_counterexamples and len(factual_counterexamples) > 0
            else ""
        )
        assertion_text = (
            f"{self.ref} "
            f"breaks regex '{self.regex}' in {factual_relative_violations} > "
            f"{violations_target} of the cases. "
            f"In absolute terms, {factual_n_violations} of the {factual_n_rows} samples "
            f"violated the regex. {counterexample_string}{self.condition_string}"
        )
        return result, assertion_text


class VarCharRegex(Constraint):
    def __init__(
        self,
        ref: DataReference,
        regex: str,
        allow_none: bool = False,
        relative_tolerance: float = 0.0,
        aggregated: bool = True,
        n_counterexamples: int = 5,
        name: Optional[str] = None,
        cache_size=None,
    ):
        super().__init__(ref, ref_value=regex, name=name, cache_size=cache_size)
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
            x
            for x in uniques_factual
            if not pattern.match(x)  # type: ignore
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

        counterexample_string = (
            ("Some counterexamples consist of the following: " f"{counterexamples}. ")
            if counterexamples and len(counterexamples) > 0
            else ""
        )

        if n_relative_violations > self.relative_tolerance:
            assertion_text = (
                f"{self.ref} "
                f"breaks regex '{self.ref_value}' in {n_relative_violations} > "
                f"{self.relative_tolerance} of the cases. "
                f"In absolute terms, {n_violations} of the {n_total} samples violated the regex. "
                f"{counterexample_string}{self.condition_string}"
            )
            return TestResult.failure(assertion_text)
        return TestResult.success()


class VarCharMinLength(Constraint):
    def __init__(
        self,
        ref,
        *,
        ref2: Optional[DataReference] = None,
        min_length: Optional[int] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=min_length,
            name=name,
            cache_size=cache_size,
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
            f"{self.ref} "
            f"has min length {length_factual} instead of "
            f"{self.target_prefix} {length_target}. "
            f"{self.condition_string}"
        )
        result = length_factual >= length_target
        return result, assertion_text


class VarCharMaxLength(Constraint):
    def __init__(
        self,
        ref: DataReference,
        *,
        ref2: Optional[DataReference] = None,
        max_length: Optional[int] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=max_length,
            name=name,
            cache_size=cache_size,
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
            f"{self.ref} "
            f"has max length {length_factual} instead of "
            f"{self.target_prefix} {length_target}. "
            f"{self.condition_string}"
        )
        result = length_factual <= length_target
        return result, assertion_text
