from __future__ import annotations

import abc

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from ..utils import format_difference
from .base import Constraint, TestResult, _OptionalSelections, _ToleranceGetter


class NRows(Constraint, abc.ABC):
    def __init__(
        self,
        ref: DataReference,
        *,
        ref2: DataReference | None = None,
        n_rows: int | None = None,
        name: str | None = None,
        cache_size=None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=n_rows,
            name=name,
            cache_size=cache_size,
        )

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[int, _OptionalSelections]:
        return db_access.get_row_count(engine, ref)


class NRowsMin(NRows):
    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[int, _OptionalSelections]:
        # Explicitly set a row_limit since we only care about the binary outcome
        # "are there enough rows" and not the actual value. This speeds up queries
        # substantially and allows for dealing with tables with more rows than the
        # size of an int.
        # Mssql can deal with this by using `COUNT_BIG` instead of `COUNT`. The
        # former is unfortunately not available via sqlalchemy as of now.
        # Other db systems/dialects seem to deal with this problem by casting to
        # bigint, which we do by default.
        return db_access.get_row_count(
            engine=engine, ref=ref, row_limit=self._ref_value
        )

    def _compare(self, value_factual: int, value_target: int) -> tuple[bool, str]:
        result = value_factual >= value_target
        assertion_text = (
            f"{self._ref} has {value_factual} "
            f"< {self._target_prefix} {value_target} rows. "
            f"{self._condition_string}"
        )
        return result, assertion_text


class NRowsMax(NRows):
    def _compare(self, value_factual: int, value_target: int) -> tuple[bool, str]:
        result = value_factual <= value_target
        value_factual_fmt, value_target_fmt = format_difference(
            value_factual, value_target
        )
        assertion_text = (
            f"{self._ref} has {value_factual_fmt} "
            f"> {self._target_prefix} {value_target_fmt} rows. "
            f"{self._condition_string}"
        )
        return result, assertion_text


class NRowsEquality(NRows):
    def _compare(self, value_factual: int, value_target: int) -> tuple[bool, str]:
        result = value_factual == value_target
        value_factual_fmt, value_target_fmt = format_difference(
            value_factual, value_target
        )
        assertion_text = (
            f"{self._ref} has {value_factual_fmt} row(s) "
            f"instead of {self._target_prefix} {value_target_fmt}. "
            f"{self._condition_string}"
        )
        return result, assertion_text


class NRowsMaxLoss(NRows):
    def __init__(
        self,
        ref: DataReference,
        ref2: DataReference,
        max_relative_loss_getter: _ToleranceGetter,
        name: str | None = None,
        cache_size=None,
    ):
        super().__init__(ref, ref2=ref2, name=name, cache_size=cache_size)
        self.max_relative_loss_getter = max_relative_loss_getter

    def _compare(self, value_factual: int, value_target: int) -> tuple[bool, str]:
        if value_target == 0:
            return True, "Empty target table."
        if value_factual > value_target:
            return True, "Row gain."
        relative_loss = (value_target - value_factual) / value_target
        assertion_text = (
            f"The #rows from {self._ref} have decreased by "
            f"{relative_loss:%} compared to table {self._ref2}. "
            f"They were expected to decrease by at most {self.max_relative_loss:%}. "
            f"{self._condition_string}"
        )
        result = relative_loss <= self.max_relative_loss
        return result, assertion_text

    def test(self, engine: sa.engine.Engine) -> TestResult:
        self.max_relative_loss = self.max_relative_loss_getter(engine)
        return super().test(engine)


class NRowsMaxGain(NRows):
    def __init__(
        self,
        ref: DataReference,
        ref2: DataReference,
        max_relative_gain_getter: _ToleranceGetter,
        name: str | None = None,
        cache_size=None,
    ):
        super().__init__(ref, ref2=ref2, name=name, cache_size=cache_size)
        self._max_relative_gain_getter = max_relative_gain_getter

    def _compare(self, value_factual: int, value_target: int) -> tuple[bool, str]:
        if value_target == 0:
            return True, "Empty target table."
        if value_factual < value_target:
            return True, "Row loss."
        relative_gain = (value_factual - value_target) / value_target
        assertion_text = (
            f"{self._ref} has {relative_gain:%} gain in #rows compared to "
            f"{self._ref2}. It was only allowed "
            f"to increase by {self._max_relative_gain:%}. "
            f"{self._condition_string}"
        )
        result = relative_gain <= self._max_relative_gain
        return result, assertion_text

    def test(self, engine: sa.engine.Engine) -> TestResult:
        self._max_relative_gain = self._max_relative_gain_getter(engine)
        return super().test(engine)


class NRowsMinGain(NRows):
    def __init__(
        self,
        ref: DataReference,
        ref2: DataReference,
        min_relative_gain_getter: _ToleranceGetter,
        name: str | None = None,
        cache_size=None,
    ):
        super().__init__(ref, ref2=ref2, name=name, cache_size=cache_size)
        self._min_relative_gain_getter = min_relative_gain_getter

    def _compare(self, value_factual: int, value_target: int) -> tuple[bool, str]:
        if value_target == 0:
            return True, "Empty target table."
        if value_factual < value_target:
            return False, "Row loss."
        relative_gain = (value_factual - value_target) / value_target
        assertion_text = (
            f"{self._ref} has {relative_gain:%} gain in #rows compared to "
            f"{self._ref2}. It was supposed "
            f"to increase at least by {self._min_relative_gain:%}. "
            f"{self._condition_string}"
        )
        result = relative_gain >= self._min_relative_gain
        return result, assertion_text

    def test(self, engine: sa.engine.Engine) -> TestResult:
        self._min_relative_gain = self._min_relative_gain_getter(engine)
        return super().test(engine)
