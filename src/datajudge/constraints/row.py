from __future__ import annotations

import abc
from functools import cache

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference, _MatchAndCompare
from .base import Constraint, TestResult, _format_sample, _ToleranceGetter


class Row(Constraint, abc.ABC):
    def __init__(
        self,
        ref: DataReference,
        ref2: DataReference,
        max_missing_fraction_getter: _ToleranceGetter,
        name: str | None = None,
        cache_size=None,
    ):
        super().__init__(ref, ref2=ref2, name=name, cache_size=cache_size)
        self._max_missing_fraction_getter = max_missing_fraction_getter

    def test(self, engine: sa.engine.Engine) -> TestResult:
        if self._ref is None or self._ref2 is None:
            raise ValueError()
        self._max_missing_fraction = self._max_missing_fraction_getter(engine)
        self._ref1_minus_ref2_sample, _ = db_access.get_row_difference_sample(
            engine, self._ref, self._ref2
        )
        self._ref2_minus_ref1_sample, _ = db_access.get_row_difference_sample(
            engine, self._ref2, self._ref
        )
        return super().test(engine)


class RowEquality(Row):
    def _get_factual_value(self, engine: sa.engine.Engine) -> tuple[int, int]:
        if self._ref is None or self._ref2 is None:
            raise ValueError()
        n_rows_missing_left, selections_left = db_access.get_row_difference_count(
            engine, self._ref, self._ref2
        )
        n_rows_missing_right, selections_right = db_access.get_row_difference_count(
            engine, self._ref2, self._ref
        )
        self._factual_selections = [*selections_left, *selections_right]
        return n_rows_missing_left, n_rows_missing_right

    def _get_target_value(self, engine: sa.engine.Engine) -> int:
        if self._ref is None or self._ref2 is None:
            raise ValueError()
        n_rows_total, selections = db_access.get_unique_count_union(
            engine, self._ref, self._ref2
        )
        self.target_selections = selections
        return n_rows_total

    # fraction: (|T1 - T2| + |T2 - T1|) / |T1 U T2|
    def _compare(
        # We are abusing the _compare method's interface here. Rather than receiving
        # factual and target values, we get missing and overall values.
        self,
        n_rows_missing_tuple: tuple[int, int],
        n_rows_total: int,
    ) -> tuple[bool, str | None]:  # type: ignore[invalid-method-override]
        n_rows_missing_left, n_rows_missing_right = n_rows_missing_tuple
        missing_fraction = (n_rows_missing_left + n_rows_missing_right) / n_rows_total
        result = missing_fraction <= self._max_missing_fraction
        if result:
            return result, None
        if self._ref2 is None:
            raise ValueError("RowEquality constraint requires ref2.")
        if n_rows_missing_left > 0:
            sample_string = _format_sample(self._ref1_minus_ref2_sample, self._ref2)
        else:
            sample_string = _format_sample(self._ref2_minus_ref1_sample, self._ref)
        assertion_message = (
            f"{missing_fraction} > "
            f"{self._max_missing_fraction} of rows differ "
            f"between {self._ref} and "
            f"{self._ref2}. E.g. for "
            f"{sample_string}."
        )
        return result, assertion_message


class RowSubset(Row):
    @cache
    def _get_factual_value(self, engine: sa.engine.Engine) -> int:
        if self._ref is None or self._ref2 is None:
            raise ValueError()
        n_rows_missing, selections = db_access.get_row_difference_count(
            engine,
            self._ref,
            self._ref2,
        )
        self.factual_selections = selections
        return n_rows_missing

    @cache
    def _get_target_value(self, engine: sa.engine.Engine) -> int:
        n_rows_total, selections = db_access.get_unique_count(engine, self._ref)
        self.target_selections = selections
        return n_rows_total

    @cache
    def _compare(
        self,
        n_rows_missing: int,
        n_rows_total: int,
    ) -> tuple[bool, str | None]:
        if n_rows_total == 0:
            return True, None
        missing_fraction = n_rows_missing / n_rows_total
        result = missing_fraction <= self._max_missing_fraction
        if result:
            return result, None
        sample_string = _format_sample(self._ref1_minus_ref2_sample, self._ref)
        assertion_message = (
            f"{missing_fraction} > "
            f"{self._max_missing_fraction} of rows of "
            f"{self._ref} are "
            f"not in {self._ref2}. E.g. for "
            f"{sample_string}. "
            f"{self._condition_string} "
        )
        return result, assertion_message


class RowSuperset(Row):
    def _get_factual_value(self, engine: sa.engine.Engine) -> int:
        if self._ref is None or self._ref2 is None:
            raise ValueError()
        n_rows_missing, selections = db_access.get_row_difference_count(
            engine, self._ref2, self._ref
        )
        self._factual_selections = selections
        return n_rows_missing

    def _get_target_value(self, engine: sa.engine.Engine) -> int:
        if self._ref is None or self._ref2 is None:
            raise ValueError()
        n_rows_total, selections = db_access.get_unique_count(engine, self._ref2)
        self._target_selections = selections
        return n_rows_total

    def _compare(
        # We are abusing the _compare method's interface here. Rather than receiving
        # factual and target values, we get missing and overall values.
        self,
        n_rows_missing: int,
        n_rows_total: int,
    ) -> tuple[bool, str | None]:  # type: ignore[invalid-method-override]
        if n_rows_total == 0:
            return True, None
        missing_fraction = n_rows_missing / n_rows_total
        result = missing_fraction <= self._max_missing_fraction
        if result:
            return result, None
        if self._ref2 is None:
            raise ValueError("RowSuperset constraint requires ref2.")
        sample_string = _format_sample(self._ref2_minus_ref1_sample, self._ref2)
        assertion_message = (
            f"{missing_fraction} > "
            f"{self._max_missing_fraction} of rows of "
            f"{self._ref2} are "
            f"not in {self._ref}. E.g. for "
            f"{sample_string}. "
            f"{self._condition_string} "
        )
        return result, assertion_message


class RowMatchingEquality(Row):
    def __init__(
        self,
        ref: DataReference,
        ref2: DataReference,
        matching_columns1: list[str],
        matching_columns2: list[str],
        comparison_columns1: list[str],
        comparison_columns2: list[str],
        max_missing_fraction_getter: _ToleranceGetter,
        name: str | None = None,
        cache_size=None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            max_missing_fraction_getter=max_missing_fraction_getter,
            name=name,
            cache_size=cache_size,
        )
        self._match_and_compare = _MatchAndCompare(
            matching_columns1,
            matching_columns2,
            comparison_columns1,
            comparison_columns2,
        )

    def test(self, engine: sa.engine.Engine) -> TestResult:
        if self._ref is None or self._ref2 is None:
            raise ValueError()
        missing_fraction, n_rows_match, selections = db_access.get_row_mismatch(
            engine, self._ref, self._ref2, self._match_and_compare
        )
        self.factual_selections = selections
        max_missing_fraction = self._max_missing_fraction_getter(engine)
        result = missing_fraction <= max_missing_fraction
        if result:
            return TestResult.success()
        assertion_message = (
            f"{missing_fraction} > "
            f"{max_missing_fraction} of the rows differ "
            f"on a match of {n_rows_match} rows between {self._ref} and "
            f"{self._ref2}. "
            f"{self._condition_string}"
            f"{self._match_and_compare} "
        )
        return TestResult.failure(assertion_message)
