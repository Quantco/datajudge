from __future__ import annotations

import abc
from typing import Any, TypeAlias

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, _OptionalSelections, _Select

# Both sa.Select and sa.CompoundSelect inherit from sa.GenerativeSelect.
_Selects: TypeAlias = tuple[_Select, _Select]


class IntervalConstraint(Constraint):
    _DIMENSIONS = 0

    def __init__(
        self,
        ref: DataReference,
        key_columns: list[str] | None,
        start_columns: list[str],
        end_columns: list[str],
        max_relative_n_violations: float,
        name: str | None = None,
        cache_size=None,
    ):
        super().__init__(ref, ref_value=object(), name=name)
        self._key_columns = key_columns
        self._start_columns = start_columns
        self._end_columns = end_columns
        self._max_relative_n_violations = max_relative_n_violations
        self._validate_dimensions()

    @abc.abstractmethod
    def _select(self, engine: sa.engine.Engine, ref: DataReference) -> _Selects: ...

    def _validate_dimensions(self):
        if (length := len(self._start_columns)) != self._DIMENSIONS:
            raise ValueError(
                f"Expected {self._DIMENSIONS} start_column(s), got {length}."
            )
        if (length := len(self._end_columns)) != self._DIMENSIONS:
            raise ValueError(
                f"Expected {self._DIMENSIONS} end_column(s), got {length}."
            )

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[tuple[int, int], _OptionalSelections]:
        keys_ref = DataReference(
            data_source=self._ref.data_source,
            columns=self._key_columns,
            condition=self._ref.condition,
        )
        n_distinct_key_values, n_keys_selections = db_access.get_unique_count(
            engine, keys_ref
        )

        sample_selection, n_violations_selection = self._select(engine, ref)
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
        key_columns: list[str] | None,
        start_columns: list[str],
        end_columns: list[str],
        max_relative_n_violations: float,
        end_included: bool,
        name: str | None = None,
        cache_size=None,
    ):
        self._end_included = end_included
        super().__init__(
            ref,
            key_columns,
            start_columns,
            end_columns,
            max_relative_n_violations,
            name=name,
            cache_size=cache_size,
        )

    def _select(self, engine: sa.engine.Engine, ref: DataReference) -> _Selects:
        sample_selection, n_violations_selection = db_access.get_interval_overlaps_nd(
            engine,
            ref,
            self._key_columns,
            start_columns=self._start_columns,
            end_columns=self._end_columns,
            end_included=self._end_included,
        )
        # TODO: Once get_unique_count also only returns a selection without
        # executing it, one would want to list this selection here as well.
        return sample_selection, n_violations_selection

    @abc.abstractmethod
    def _compare(
        self, value_factual: Any, value_target: Any
    ) -> tuple[bool, str | None]: ...


class NoGapConstraint(IntervalConstraint):
    def __init__(
        self,
        ref: DataReference,
        key_columns: list[str] | None,
        start_columns: list[str],
        end_columns: list[str],
        max_relative_n_violations: float,
        legitimate_gap_size: float,
        name: str | None = None,
        cache_size=None,
    ):
        self._legitimate_gap_size = legitimate_gap_size
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
    def _select(self, engine: sa.engine.Engine, ref: DataReference) -> _Selects: ...

    @abc.abstractmethod
    def _compare(
        self, value_factual: tuple[int, int], value_target: Any
    ) -> tuple[bool, str | None]: ...
