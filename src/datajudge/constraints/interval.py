import abc
from typing import Any, List, Optional, Tuple

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, OptionalSelections


class IntervalConstraint(Constraint):
    _DIMENSIONS = 0

    def __init__(
        self,
        ref: DataReference,
        key_columns: Optional[List[str]],
        start_columns: List[str],
        end_columns: List[str],
        max_relative_n_violations: float,
        name: Optional[str] = None,
        cache_size=None,
    ):
        super().__init__(ref, ref_value=object(), name=name)
        self.key_columns = key_columns
        self.start_columns = start_columns
        self.end_columns = end_columns
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
            n_violation_keys = int(
                str(connection.execute(n_violations_selection).scalar())
            )

        selections = [*n_keys_selections, sample_selection, n_violations_selection]
        return (n_violation_keys, n_distinct_key_values), selections


class NoOverlapConstraint(IntervalConstraint):
    def __init__(
        self,
        ref: DataReference,
        key_columns: Optional[List[str]],
        start_columns: List[str],
        end_columns: List[str],
        max_relative_n_violations: float,
        end_included: bool,
        name: Optional[str] = None,
        cache_size=None,
    ):
        self.end_included = end_included
        super().__init__(
            ref,
            key_columns,
            start_columns,
            end_columns,
            max_relative_n_violations,
            name=name,
            cache_size=cache_size,
        )

    def select(self, engine: sa.engine.Engine, ref: DataReference):
        sample_selection, n_violations_selection = db_access.get_interval_overlaps_nd(
            engine,
            ref,
            self.key_columns,
            start_columns=self.start_columns,
            end_columns=self.end_columns,
            end_included=self.end_included,
        )
        # TODO: Once get_unique_count also only returns a selection without
        # executing it, one would want to list this selection here as well.
        return sample_selection, n_violations_selection

    @abc.abstractmethod
    def compare(self, factual: Any, target: Any):
        pass


class NoGapConstraint(IntervalConstraint):
    def __init__(
        self,
        ref: DataReference,
        key_columns: Optional[List[str]],
        start_columns: List[str],
        end_columns: List[str],
        max_relative_n_violations: float,
        legitimate_gap_size: float,
        name: Optional[str] = None,
        cache_size=None,
    ):
        self.legitimate_gap_size = legitimate_gap_size
        super().__init__(
            ref,
            key_columns,
            start_columns,
            end_columns,
            max_relative_n_violations,
            name=name,
            cache_size=cache_size,
        )

    @abc.abstractmethod
    def select(self, engine: sa.engine.Engine, ref: DataReference):
        pass

    @abc.abstractmethod
    def compare(self, factual: Tuple[int, int], target: Any) -> Tuple[bool, str]:
        pass
