import abc
from functools import lru_cache
from typing import List, Optional, Tuple

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference, MatchAndCompare
from .base import Constraint, TestResult, ToleranceGetter, format_sample


class Row(Constraint, abc.ABC):
    def __init__(
        self,
        ref: DataReference,
        ref2: DataReference,
        max_missing_fraction_getter: ToleranceGetter,
        name: Optional[str] = None,
        cache_size=None,
    ):
        super().__init__(ref, ref2=ref2, name=name, cache_size=cache_size)
        self.max_missing_fraction_getter = max_missing_fraction_getter

    def test(self, engine: sa.engine.Engine) -> TestResult:
        if db_access.is_impala(engine):
            raise NotImplementedError("Currently not implemented for impala.")
        self.max_missing_fraction = self.max_missing_fraction_getter(engine)
        self.ref1_minus_ref2_sample, _ = db_access.get_row_difference_sample(
            engine, self.ref, self.ref2
        )
        self.ref2_minus_ref1_sample, _ = db_access.get_row_difference_sample(
            engine, self.ref2, self.ref
        )
        return super().test(engine)


class RowEquality(Row):
    def get_factual_value(self, engine: sa.engine.Engine) -> Tuple[int, int]:
        n_rows_missing_left, selections_left = db_access.get_row_difference_count(
            engine, self.ref, self.ref2
        )
        n_rows_missing_right, selections_right = db_access.get_row_difference_count(
            engine, self.ref2, self.ref
        )
        self.factual_selections = [*selections_left, *selections_right]
        return n_rows_missing_left, n_rows_missing_right

    def get_target_value(self, engine: sa.engine.Engine) -> int:
        n_rows_total, selections = db_access.get_unique_count_union(
            engine, self.ref, self.ref2
        )
        self.target_selections = selections
        return n_rows_total

    # fraction: (|T1 - T2| + |T2 - T1|) / |T1 U T2|
    def compare(
        self, n_rows_missing_tuple: Tuple[int, int], n_rows_total: int
    ) -> Tuple[bool, Optional[str]]:
        n_rows_missing_left, n_rows_missing_right = n_rows_missing_tuple
        missing_fraction = (n_rows_missing_left + n_rows_missing_right) / n_rows_total
        result = missing_fraction <= self.max_missing_fraction
        if result:
            return result, None
        if self.ref2 is None:
            raise ValueError("RowEquality constraint requires ref2.")
        if n_rows_missing_left > 0:
            sample_string = format_sample(self.ref1_minus_ref2_sample, self.ref2)
        else:
            sample_string = format_sample(self.ref2_minus_ref1_sample, self.ref)
        assertion_message = (
            f"{missing_fraction} > "
            f"{self.max_missing_fraction} of rows differ "
            f"between {self.ref} and "
            f"{self.ref2}. E.g. for "
            f"{sample_string}."
        )
        return result, assertion_message


class RowSubset(Row):
    @lru_cache(maxsize=None)
    def get_factual_value(self, engine: sa.engine.Engine) -> int:
        n_rows_missing, selections = db_access.get_row_difference_count(
            engine,
            self.ref,
            self.ref2,
        )
        self.factual_selections = selections
        return n_rows_missing

    @lru_cache(maxsize=None)
    def get_target_value(self, engine: sa.engine.Engine) -> int:
        n_rows_total, selections = db_access.get_unique_count(engine, self.ref)
        self.target_selections = selections
        return n_rows_total

    @lru_cache(maxsize=None)
    def compare(
        self, n_rows_missing: int, n_rows_total: int
    ) -> Tuple[bool, Optional[str]]:
        if n_rows_total == 0:
            return True, None
        missing_fraction = n_rows_missing / n_rows_total
        result = missing_fraction <= self.max_missing_fraction
        if result:
            return result, None
        sample_string = format_sample(self.ref1_minus_ref2_sample, self.ref)
        assertion_message = (
            f"{missing_fraction} > "
            f"{self.max_missing_fraction} of rows of "
            f"{self.ref} are "
            f"not in {self.ref2}. E.g. for "
            f"{sample_string}. "
            f"{self.condition_string} "
        )
        return result, assertion_message


class RowSuperset(Row):
    def get_factual_value(self, engine: sa.engine.Engine) -> int:
        n_rows_missing, selections = db_access.get_row_difference_count(
            engine, self.ref2, self.ref
        )
        self.factual_selections = selections
        return n_rows_missing

    def get_target_value(self, engine: sa.engine.Engine) -> int:
        n_rows_total, selections = db_access.get_unique_count(engine, self.ref2)
        self.target_selections = selections
        return n_rows_total

    def compare(
        self, n_rows_missing: int, n_rows_total: int
    ) -> Tuple[bool, Optional[str]]:
        if n_rows_total == 0:
            return True, None
        missing_fraction = n_rows_missing / n_rows_total
        result = missing_fraction <= self.max_missing_fraction
        if result:
            return result, None
        if self.ref2 is None:
            raise ValueError("RowSuperset constraint requires ref2.")
        sample_string = format_sample(self.ref2_minus_ref1_sample, self.ref2)
        assertion_message = (
            f"{missing_fraction} > "
            f"{self.max_missing_fraction} of rows of "
            f"{self.ref2} are "
            f"not in {self.ref}. E.g. for "
            f"{sample_string}. "
            f"{self.condition_string} "
        )
        return result, assertion_message


class RowMatchingEquality(Row):
    def __init__(
        self,
        ref: DataReference,
        ref2: DataReference,
        matching_columns1: List[str],
        matching_columns2: List[str],
        comparison_columns1: List[str],
        comparison_columns2: List[str],
        max_missing_fraction_getter: ToleranceGetter,
        name: Optional[str] = None,
        cache_size=None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            max_missing_fraction_getter=max_missing_fraction_getter,
            name=name,
            cache_size=cache_size,
        )
        self.match_and_compare = MatchAndCompare(
            matching_columns1,
            matching_columns2,
            comparison_columns1,
            comparison_columns2,
        )

    def test(self, engine: sa.engine.Engine) -> TestResult:
        missing_fraction, n_rows_match, selections = db_access.get_row_mismatch(
            engine, self.ref, self.ref2, self.match_and_compare
        )
        self.factual_selections = selections
        max_missing_fraction = self.max_missing_fraction_getter(engine)
        result = missing_fraction <= max_missing_fraction
        if result:
            return TestResult.success()
        assertion_message = (
            f"{missing_fraction} > "
            f"{max_missing_fraction} of the rows differ "
            f"on a match of {n_rows_match} rows between {self.ref} and "
            f"{self.ref2}. "
            f"{self.condition_string}"
            f"{self.match_and_compare} "
        )
        return TestResult.failure(assertion_message)
