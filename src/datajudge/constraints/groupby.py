from typing import Any, Optional, Tuple

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, OptionalSelections


class AggregateNumericRangeEquality(Constraint):
    def __init__(
        self,
        ref: DataReference,
        aggregation_column: str,
        start_value: int = 0,
        *,
        tolerance: float = 0,
        ref2: DataReference = None,
    ):
        super().__init__(ref, ref2=ref2, ref_value=object())
        self.aggregation_column = aggregation_column
        self.tolerance = tolerance
        self.start_value = start_value
        self._selection = None

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[Any, OptionalSelections]:
        result, selections = db_access.get_column_array_agg(
            engine, ref, self.aggregation_column
        )
        result = {fact[:-1]: fact[-1] for fact in result}
        return result, selections

    def compare(self, factual: Any, target: Any) -> Tuple[bool, Optional[str]]:
        def missing_from_range(values, start=0):
            return set(range(start, max(values) + start)) - set(values)

        results = {
            k: missing_from_range(v, self.start_value) for k, v in factual.items()
        }
        failed_results = dict(filter(lambda x: len(x[1]) > 0, results.items()))

        if len(failed_results) / len(factual) > self.tolerance:
            assertion_text = (
                f"{self.ref.get_string()} has unfulfilled continuity requirement for "
                f"(key, missing values): `{failed_results}`."
                f"{self.condition_string}"
            )
            return False, assertion_text
        return True, None
