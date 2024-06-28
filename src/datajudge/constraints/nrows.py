import abc
from typing import Optional, Tuple

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from ..utils import format_difference
from .base import Constraint, OptionalSelections, TestResult, ToleranceGetter


class NRows(Constraint, abc.ABC):
    def __init__(
        self,
        ref,
        *,
        ref2: Optional[DataReference] = None,
        n_rows: Optional[int] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=n_rows,
            name=name,
            cache_size=cache_size,
        )

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[int, OptionalSelections]:
        return db_access.get_row_count(engine, ref)


class NRowsMin(NRows):
    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[int, OptionalSelections]:
        # Explicitly set a row_limit since we only care about the binary outcome
        # "are there enough rows" and not the actual value. This speeds up queries
        # substantially and allows for dealing with tables with more rows than the
        # size of an int.
        # Mssql can deal with this by using `COUNT_BIG` instead of `COUNT`. The
        # former is unfortunately not available via sqlalchemy as of now.
        # Other db systems/dialects seem to deal with this problem by casting to
        # bigint, which we do by default.
        return db_access.get_row_count(engine=engine, ref=ref, row_limit=self.ref_value)

    def compare(self, n_rows_factual: int, n_rows_target: int) -> Tuple[bool, str]:
        result = n_rows_factual >= n_rows_target
        assertion_text = (
            f"{self.ref} has {n_rows_factual} "
            f"< {self.target_prefix} {n_rows_target} rows. "
            f"{self.condition_string}"
        )
        return result, assertion_text


class NRowsMax(NRows):
    def compare(self, n_rows_factual: int, n_rows_target: int) -> Tuple[bool, str]:
        result = n_rows_factual <= n_rows_target
        n_rows_factual_fmt, n_rows_target_fmt = format_difference(
            n_rows_factual, n_rows_target
        )
        assertion_text = (
            f"{self.ref} has {n_rows_factual_fmt} "
            f"> {self.target_prefix} {n_rows_target_fmt} rows. "
            f"{self.condition_string}"
        )
        return result, assertion_text


class NRowsEquality(NRows):
    def compare(self, n_rows_factual: int, n_rows_target: int) -> Tuple[bool, str]:
        result = n_rows_factual == n_rows_target
        n_rows_factual_fmt, n_rows_target_fmt = format_difference(
            n_rows_factual, n_rows_target
        )
        assertion_text = (
            f"{self.ref} has {n_rows_factual_fmt} row(s) "
            f"instead of {self.target_prefix} {n_rows_target_fmt}. "
            f"{self.condition_string}"
        )
        return result, assertion_text


class NRowsMaxLoss(NRows):
    def __init__(
        self,
        ref: DataReference,
        ref2: DataReference,
        max_relative_loss_getter: ToleranceGetter,
        name: Optional[str] = None,
        cache_size=None,
    ):
        super().__init__(ref, ref2=ref2, name=name, cache_size=cache_size)
        self.max_relative_loss_getter = max_relative_loss_getter

    def compare(self, n_rows_factual: int, n_rows_target: int) -> Tuple[bool, str]:
        if n_rows_target == 0:
            return True, "Empty target table."
        if n_rows_factual > n_rows_target:
            return True, "Row gain."
        relative_loss = (n_rows_target - n_rows_factual) / n_rows_target
        assertion_text = (
            f"The #rows from {self.ref} have decreased by "
            f"{relative_loss:%} compared to table {self.ref2}. "
            f"They were expected to decrease by at most {self.max_relative_loss:%}. "
            f"{self.condition_string}"
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
        max_relative_gain_getter: ToleranceGetter,
        name: Optional[str] = None,
        cache_size=None,
    ):
        super().__init__(ref, ref2=ref2, name=name, cache_size=cache_size)
        self.max_relative_gain_getter = max_relative_gain_getter

    def compare(self, n_rows_factual: int, n_rows_target: int) -> Tuple[bool, str]:
        if n_rows_target == 0:
            return True, "Empty target table."
        if n_rows_factual < n_rows_target:
            return True, "Row loss."
        relative_gain = (n_rows_factual - n_rows_target) / n_rows_target
        assertion_text = (
            f"{self.ref} has {relative_gain:%} gain in #rows compared to "
            f"{self.ref2}. It was only allowed "
            f"to increase by {self.max_relative_gain:%}. "
            f"{self.condition_string}"
        )
        result = relative_gain <= self.max_relative_gain
        return result, assertion_text

    def test(self, engine: sa.engine.Engine) -> TestResult:
        self.max_relative_gain = self.max_relative_gain_getter(engine)
        return super().test(engine)


class NRowsMinGain(NRows):
    def __init__(
        self,
        ref: DataReference,
        ref2: DataReference,
        min_relative_gain_getter: ToleranceGetter,
        name: Optional[str] = None,
        cache_size=None,
    ):
        super().__init__(ref, ref2=ref2, name=name, cache_size=cache_size)
        self.min_relative_gain_getter = min_relative_gain_getter

    def compare(self, n_rows_factual: int, n_rows_target: int) -> Tuple[bool, str]:
        if n_rows_target == 0:
            return True, "Empty target table."
        if n_rows_factual < n_rows_target:
            return False, "Row loss."
        relative_gain = (n_rows_factual - n_rows_target) / n_rows_target
        assertion_text = (
            f"{self.ref} has {relative_gain:%} gain in #rows compared to "
            f"{self.ref2}. It was supposed "
            f"to increase at least by {self.min_relative_gain:%}. "
            f"{self.condition_string}"
        )
        result = relative_gain >= self.min_relative_gain
        return result, assertion_text

    def test(self, engine: sa.engine.Engine) -> TestResult:
        self.min_relative_gain = self.min_relative_gain_getter(engine)
        return super().test(engine)
