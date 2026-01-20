import abc
from collections.abc import Callable, Collection, Sequence
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, TypeVar

import sqlalchemy as sa
from sqlalchemy.sql import selectable

from ..db_access import DataReference
from ..formatter import Formatter
from ..utils import OutputProcessor, output_processor_limit

_T = TypeVar("_T")

_DEFAULT_FORMATTER = Formatter()

_Select = selectable.Select | selectable.CompoundSelect
_OptionalSelections = Sequence[_Select] | None
_ToleranceGetter = Callable[[sa.engine.Engine], float]


def _uncommon_substrings(string1: str, string2: str) -> tuple[str, str]:
    qualifiers1 = string1.split(".")
    qualifiers2 = string2.split(".")
    if qualifiers1[0] != qualifiers2[0]:
        return string1, string2
    if qualifiers1[1] != qualifiers2[0]:
        return ".".join(qualifiers1[1:]), ".".join(qualifiers2[1:])
    return qualifiers1[-1], qualifiers2[-1]


@dataclass(frozen=True)
class TestResult:
    """The result of the execution of a Constraint."""

    outcome: bool
    _failure_message: str | None = field(default=None, repr=False)
    _constraint_description: str | None = field(default=None, repr=False)
    _factual_queries: str | None = field(default=None, repr=False)
    _target_queries: str | None = field(default=None, repr=False)

    def formatted_failure_message(self, formatter: Formatter) -> str | None:
        return (
            formatter.fmt_str(self._failure_message) if self._failure_message else None
        )

    def formatted_constraint_description(self, formatter: Formatter) -> str | None:
        return (
            formatter.fmt_str(self._constraint_description)
            if self._constraint_description
            else None
        )

    @property
    def failure_message(self) -> str | None:
        return self.formatted_failure_message(_DEFAULT_FORMATTER)

    @property
    def constraint_description(self) -> str | None:
        return self.formatted_constraint_description(_DEFAULT_FORMATTER)

    @property
    def logging_message(self):
        constraint_description_message = (
            f"/*\n\t{self.constraint_description}\n*/"
            if self.constraint_description
            else ""
        )
        failure_message = (
            f"\n\n/*\nFailure message:\n{self.failure_message}\n*/"
            if self.failure_message
            else ""
        )
        factual_query_message = ""
        if self._factual_queries is not None:
            factual_query_message = "\n\n --Factual queries: \n " + "\n".join(
                self._factual_queries
            )

        target_query_message = ""
        if self._target_queries is not None:
            target_query_message = "\n\n-- Target queries: \n " + "\n".join(
                self._target_queries
            )

        return (
            constraint_description_message
            + failure_message
            + factual_query_message
            + target_query_message
            + "\n --- \n"
        )

    @classmethod
    def success(cls):
        return cls(True)

    @classmethod
    def failure(cls, *args, **kwargs):
        return cls(False, *args, **kwargs)


class Constraint(abc.ABC):
    """Express a DataReference constraint against either another DataReference or a reference value.

    Constraints against other DataReferences are typically referred to as 'between' constraints.
    Please use the the ``ref2`` argument to instantiate such a constraint.
    Constraints against a fixed reference value are typically referred to as 'within' constraints.
    Please use the ``ref_value`` argument to instantiate such a constraint.

    A constraint typically relies on the comparison of factual and target values. The former
    represent the key quantity of interest as seen in the database, the latter the key quantity of
    interest as expected a priori. Such a comparison is meant to be carried out in the `test`
    method.

    In order to obtain such values, the ``retrieve`` method defines a mapping from DataReference,
    be it the DataReference of primary interest, ``ref``, or a baseline DataReference, ``ref2``, to
    value. If ``ref_value`` is already provided, usually no further mapping needs to be taken care of.

    By default, retrieved arguments are cached indefinitely ``@lru_cache(maxsize=None)``.
    This can be controlled by setting the `cache_size` argument to a different value.
    ``0`` disables caching.
    """

    def __init__(
        self,
        ref: DataReference,
        *,
        ref2: DataReference | None = None,
        ref_value: Any = None,
        name: str | None = None,
        output_processors: OutputProcessor
        | list[OutputProcessor]
        | None = output_processor_limit,
        cache_size=None,
    ):
        self._check_if_valid_between_or_within(ref2, ref_value)
        self._ref = ref
        self._ref2 = ref2
        self._ref_value = ref_value
        self.name = name
        self._factual_selections: _OptionalSelections = None
        self._target_selections: _OptionalSelections = None
        self._factual_queries: list[str] | None = None
        self._target_queries: list[str] | None = None

        if (output_processors is not None) and (
            not isinstance(output_processors, list)
        ):
            output_processors = [output_processors]

        self._output_processors: list[OutputProcessor] | None = output_processors

        self._cache_size = cache_size
        self._setup_caching()

    def _setup_caching(self):
        # We don't use cache or lru_cache decorators since those would lead
        # to class-based, not instance-based caching.
        #
        # Using this approach has the added benefit of allowing the class to be garbage collected
        # according to https://rednafi.com/python/lru_cache_on_methods/
        # and https://docs.astral.sh/ruff/rules/cached-instance-method/
        self._get_factual_value: Callable[[Constraint, sa.engine.Engine], Any] = (
            lru_cache(self._cache_size)(self._get_factual_value)
        )
        self._get_target_value: Callable[[Constraint, sa.engine.Engine], Any] = (
            lru_cache(self._cache_size)(self._get_target_value)
        )

    def _check_if_valid_between_or_within(
        self,
        ref2: DataReference | None,
        ref_value: Any,
    ):
        """Check whether exactly one of ref2 and ref_value arguments have been used."""
        class_name = self.__class__.__name__
        if ref2 is not None and ref_value is not None:
            raise ValueError(
                "Both table 2 ref and constant given to "
                f"{class_name}. Use either of them, not both."
            )
        if ref2 is None and ref_value is None:
            raise ValueError(
                "Neither table 2 ref nor constant given to "
                f"{class_name}. Use exactly either of them."
            )

    def _get_factual_value(self, engine: sa.engine.Engine) -> Any:
        factual_value, factual_selections = self._retrieve(engine, self._ref)
        self._factual_selections = factual_selections
        return factual_value

    def _get_target_value(self, engine: sa.engine.Engine) -> Any:
        if self._ref2 is None:
            return self._ref_value
        target_value, target_selections = self._retrieve(engine, self._ref2)
        self._target_selections = target_selections
        return target_value

    def get_description(self) -> str:
        if self.name is not None:
            return self.name
        if self._ref2 is None:
            data_source_string = str(self._ref.data_source)
        else:
            data_source1_string = str(self._ref.data_source)
            data_source2_string = str(self._ref2.data_source)

            data_source1_substring, data_source2_substring = _uncommon_substrings(
                data_source1_string, data_source2_string
            )
            data_source_string = f"{data_source1_substring} | {data_source2_substring}"
        return self.__class__.__name__ + "::" + data_source_string

    @property
    def _target_prefix(self) -> str:
        return f"{self._ref2}'s " if (self._ref2 is not None) else ""

    @property
    def _condition_string(self) -> str:
        if self._ref.condition is None and (
            self._ref2 is None or self._ref2.condition is None
        ):
            return ""
        ref1_clause = self._ref._get_clause_string()
        if self._ref2 is None:
            # within constraint
            return f"Condition: {ref1_clause}"
        ref2_clause = self._ref2._get_clause_string()
        if self._ref.condition == self._ref2.condition:
            return f"Condition on both tables: {ref1_clause}; "
        if self._ref.condition is None:
            return f"Condition on second table: {ref2_clause}; "
        if self._ref2.condition is None:
            return f"Condition on first table: {ref1_clause}; "
        return (
            f"Condition on first table: {ref1_clause}. "
            f"Condition on second table: {ref2_clause}. "
        )

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[Any, _OptionalSelections]:
        """Retrieve the value of interest for a DataReference from database."""
        raise NotImplementedError()

    def _compare(
        self, value_factual: Any, value_target: Any
    ) -> tuple[bool, str | None]:
        raise NotImplementedError()

    def test(self, engine: sa.engine.Engine) -> TestResult:
        # ty can't figure out that this is a method and that self is passed
        # as the first argument.
        value_factual = self._get_factual_value(engine=engine)  # type: ignore[missing-argument]
        # ty can't figure out that this is a method and that self is passed
        # as the first argument.
        value_target = self._get_target_value(engine=engine)  # type: ignore[missing-argument]
        is_success, assertion_message = self._compare(value_factual, value_target)

        if is_success:
            return TestResult.success()

        factual_queries = None
        if self._factual_selections:
            factual_queries = [
                str(
                    factual_selection.compile(
                        engine, compile_kwargs={"literal_binds": True}
                    )
                )
                for factual_selection in self._factual_selections
            ]
        target_queries = None
        if self._target_selections:
            target_queries = [
                str(
                    target_selection.compile(
                        engine, compile_kwargs={"literal_binds": True}
                    )
                )
                for target_selection in self._target_selections
            ]
        return TestResult.failure(
            assertion_message,
            self.get_description(),
            factual_queries,
            target_queries,
        )

    def _apply_output_formatting(self, values: Collection) -> Collection:
        if self._output_processors is not None:
            for output_processor in self._output_processors:
                values, _ = output_processor(values)
        return values


def _format_sample(sample, ref: DataReference) -> str:
    """Build a string from a database row indicating its column values."""
    if ref.columns is None:
        return str(sample)
    sample_string = " , ".join(
        map(lambda x: f"{x[0]} = {x[1]}", zip(ref.columns, list(sample)))
    )
    return sample_string
