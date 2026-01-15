from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class Condition:
    """Condition allows for further narrowing down of a DataSource in a Constraint.

    A ``Condition`` can be thought of as a filter, the content of a sql 'where' clause
    or a condition as known from probability theory.

    While a ``DataSource`` is expressed more generally, one might be interested
    in testing properties of a specific part of said ``DataSource`` in light
    of a particular constraint. Hence using ``Condition`` allows for the reusage
    of a ``DataSource``, in lieu of creating a new custom ``DataSource`` with
    the ``Condition`` implicitly built in.

    A ``Condition`` can either be 'atomic', i.e. not further reducible to sub-conditions
    or 'composite', i.e. combining multiple subconditions. In the former case, it can
    be instantiated with help of the ``raw_string`` parameter, e.g. ``"col1 > 0"``. In the
    latter case, it can be instantiated with help of the ``conditions`` and
    ``reduction_operator`` parameters. ``reduction_operator`` allows for two values: ``"and"`` (logical
    conjunction) and ``"or"`` (logical disjunction). Note that composition of ``Condition``
    supports arbitrary degrees of nesting.
    """

    raw_string: str | None = None
    conditions: Sequence[Condition] | None = None
    reduction_operator: str | None = None

    def __post_init__(self):
        if self._is_atomic() and self.conditions is not None:
            raise ValueError(
                "Condition can either be instantiated atomically, with "
                "the raw_query parameter, or in a composite fashion, with "
                "the conditions parameter. "
                "Exactly one of them needs to be provided, yet both are."
            )
        if not self._is_atomic() and (
            self.conditions is None or len(self.conditions) == 0
        ):
            raise ValueError(
                "Condition can either be instantiated atomically, with "
                "the raw_query parameter, or in a composite fashion, with "
                "the conditions parameter. "
                "Exactly one of them needs to be provided, yet none is."
            )
        if not self._is_atomic() and self.reduction_operator not in ["and", "or"]:
            raise ValueError(
                "reuction_operator has to be either 'and' or 'or' but "
                f"obtained {self.reduction_operator}."
            )

    def _is_atomic(self) -> bool:
        return self.raw_string is not None

    def __str__(self) -> str:
        if self._is_atomic():
            if self.raw_string is None:
                raise ValueError(
                    "Condition can either be instantiated atomically, with "
                    "the raw_query parameter, or in a composite fashion, with "
                    "the conditions parameter. "
                    "Exactly one of them needs to be provided, yet none is."
                )
            return self.raw_string
        if not self.conditions:
            raise ValueError("This should never happen thanks to __post__init.")
        return f" {self.reduction_operator} ".join(
            f"({condition})" for condition in self.conditions
        )

    def _snowflake_str(self) -> str:
        # Temporary method - should be removed as soon as snowflake-sqlalchemy
        # bug is fixed.
        return str(self)
