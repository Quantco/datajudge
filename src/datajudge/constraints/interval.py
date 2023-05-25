import abc
from typing import Any, List, Optional, Tuple
from datajudge import db_access
from datajudge.constraints.base import Constraint, OptionalSelections, TestResult
from datajudge.db_access import DataReference
import sqlalchemy as sa


class IntervalConstraint(Constraint, abc.ABC):
    _DIMENSIONS = 0

    def __init__(
        self,
        ref: DataReference,
        key_columns: Optional[List[str]],
        start_columns: List[str],
        end_columns: List[str],
        end_included: bool,
        max_relative_n_violations: float,
        name: str = None,
    ):
        super().__init__(ref, ref_value=object(), name=name)
        self.key_columns = key_columns
        self.start_columns = start_columns
        self.end_columns = end_columns
        self.end_included = end_included
        self.max_relative_n_violations = max_relative_n_violations
        self._validate_dimensions()

    @abc.abstractmethod
    def select(self, engine: sa.engine.Engine, ref: DataReference):
        pass

    def _validate_dimensions(self):
        if (length := len(self.start_columns)) != self._DIMENSIONS:
            raise ValueError(
                f"Expected {self._DIMENSIONS} start_column(s), got {length}."
            )
        if (length := len(self.end_columns)) != self._DIMENSIONS:
            raise ValueError(
                f"Expected {self._DIMENSIONS} end_column(s), got {length}."
            )

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[Tuple[int, int], OptionalSelections]:
        keys_ref = DataReference(
            data_source=self.ref.data_source,
            columns=self.key_columns,
            condition=self.ref.condition,
        )
        n_distinct_key_values, n_keys_selections = db_access.get_unique_count(
            engine, keys_ref
        )

        sample_selection, n_violations_selection = self.select(engine, ref)
        with engine.connect() as connection:
            self.sample = connection.execute(sample_selection).first()
            n_violation_keys = connection.execute(n_violations_selection).scalar()

        selections = [*n_keys_selections, sample_selection, n_violations_selection]
        return (n_violation_keys, n_distinct_key_values), selections
    

class IntegerNoGap(IntervalConstraint):
    _DIMENSIONS = 1

    def select(self, engine: sa.engine.Engine, ref: DataReference):
        sample_selection, n_violations_selection = db_access.get_integer_gaps(
            engine,
            ref,
            self.key_columns,
            self.start_columns[0],
            self.end_columns[0],
            self.end_included,
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
            f"{self.ref.get_string()} has a ratio of {violation_fraction} > "
            f"{self.max_relative_n_violations} keys in columns {self.key_columns} "
            f"with a gap in the range in {self.start_columns[0]} and {self.end_columns[0]}."
            f"E.g. for: {self.sample}."
        )
        result = violation_fraction <= self.max_relative_n_violations
        return result, assertion_text
