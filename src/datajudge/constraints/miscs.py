from typing import List, Optional, Set, Tuple

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, OptionalSelections, TestResult, format_sample


class PrimaryKeyDefinition(Constraint):
    def __init__(self, ref, primary_keys: List[str]):
        super().__init__(ref, ref_value=set(primary_keys))

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[Set[str], OptionalSelections]:
        values, selections = db_access.get_primary_keys(engine, self.ref)
        return set(values), selections

    # Note: Exact equality!
    def compare(
        self, primary_keys_factual: Set[str], primary_keys_target: Set[str]
    ) -> Tuple[bool, Optional[str]]:
        assertion_message = ""
        result = True
        # If both are true, just report one.
        if len(primary_keys_factual.difference(primary_keys_target)) > 0:
            example_key = next(
                iter(primary_keys_factual.difference(primary_keys_target))
            )
            assertion_message = (
                f"{self.ref.get_string()} incorrectly includes "
                f"{example_key} as primary key."
            )
            result = False
        if len(primary_keys_target.difference(primary_keys_factual)) > 0:
            example_key = next(
                iter(primary_keys_target.difference(primary_keys_factual))
            )
            assertion_message = (
                f"{self.ref.get_string()} doesn't include "
                f"{example_key} as primary key."
            )
            result = False
        return result, assertion_message


class Uniqueness(Constraint):
    # In contrast to PrimaryKeyDefinition, it is hardly imaginable to have
    # this constraint be used between tables.
    def __init__(
        self,
        ref: DataReference,
        max_duplicate_fraction: float = 0,
        max_absolute_n_duplicates: int = 0,
    ):
        if max_duplicate_fraction != 0 and max_absolute_n_duplicates != 0:
            raise ValueError(
                """Uniqueness constraint was attempted to be constructed
                with both a relative and an absolute tolerance. Only use one
                of both at a time."""
            )
        if max_duplicate_fraction != 0:
            ref_value = ("relative", max_duplicate_fraction)
        elif max_absolute_n_duplicates != 0:
            ref_value = ("absolute", max_absolute_n_duplicates)
        else:
            ref_value = ("relative", 0)
        super().__init__(ref, ref_value=ref_value)

    def test(self, engine: sa.engine.Engine) -> TestResult:
        unique_count, unique_selections = db_access.get_unique_count(engine, self.ref)
        row_count, row_selections = db_access.get_row_count(engine, self.ref)
        self.factual_selections = row_selections
        self.target_selections = unique_selections
        if row_count == 0:
            return TestResult(True, "No occurrences.")
        tolerance_kind, tolerance_value = self.ref_value  # type: ignore
        if tolerance_kind == "relative":
            result = unique_count >= row_count * (1 - tolerance_value)
        elif tolerance_kind == "absolute":
            result = unique_count >= row_count - tolerance_value
        else:
            raise ValueError(
                "Given tolerance is neither relative nor absolute: {tolerance_kind}."
            )
        if result:
            return TestResult.success()
        sample, _ = db_access.get_duplicate_sample(engine, self.ref)
        sample_string = format_sample(sample, self.ref)
        assertion_text = (
            f"{self.ref.get_string()} has {row_count} rows > {unique_count} "
            f"uniques. This surpasses the max_duplicate_fraction of "
            f"{self.ref_value}. An example tuple breaking the "
            f"uniqueness condition is: {sample_string}."
        )
        return TestResult.failure(assertion_text)


class NullAbsence(Constraint):
    def __init__(self, ref: DataReference):
        # This is arguably hacky. Passing this pointless string ensures that
        # None-checks fail.
        super().__init__(ref, ref_value="NoNull")

    def test(self, engine: sa.engine) -> TestResult:
        assertion_message = f"{self.ref.get_string()} contains NULLS."
        query_result, selections = db_access.contains_null(engine, self.ref)
        self.factual_selections = selections
        result = not query_result
        return TestResult(result, assertion_message)
