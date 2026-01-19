from __future__ import annotations

from typing import Any

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, _OptionalSelections


class AggregateNumericRangeEquality(Constraint):
    def __init__(
        self,
        ref: DataReference,
        aggregation_column: str,
        start_value: int = 0,
        name: str | None = None,
        cache_size=None,
        *,
        tolerance: float = 0,
        ref2: DataReference | None = None,
    ):
        super().__init__(ref, ref2=ref2, ref_value=object(), name=name)
        self._aggregation_column = aggregation_column
        self._tolerance = tolerance
        self._start_value = start_value
        self._selection = None

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[Any, _OptionalSelections]:
        result, selections = db_access.get_column_array_agg(
            engine, ref, self._aggregation_column
        )
        result = {fact[:-1]: fact[-1] for fact in result}
        return result, selections

    def _compare(
        self, value_factual: Any, value_target: Any
    ) -> tuple[bool, str | None]:
        def missing_from_range(values, start=0):
            return set(range(start, max(values) + start)) - set(values)

        results = {
            k: missing_from_range(v, self._start_value)
            for k, v in value_factual.items()
        }
        failed_results = dict(filter(lambda x: len(x[1]) > 0, results.items()))

        if len(failed_results) / len(value_factual) > self._tolerance:
            assertion_text = (
                f"{self._ref} has unfulfilled continuity requirement for "
                f"(key, missing values): `{failed_results}`."
                f"{self._condition_string}"
            )
            return False, assertion_text
        return True, None
