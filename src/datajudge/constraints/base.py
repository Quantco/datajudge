import abc
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Callable, List, Optional, Tuple, TypeVar

import sqlalchemy as sa

from ..db_access import DataReference

T = TypeVar("T")
OptionalSelections = Optional[List[sa.sql.expression.Select]]
ToleranceGetter = Callable[[sa.engine.Engine], float]


def uncommon_substrings(string1: str, string2: str) -> Tuple[str, str]:
    qualifiers1 = string1.split(".")
    qualifiers2 = string2.split(".")
    if qualifiers1[0] != qualifiers2[0]:
        return string1, string2
    if qualifiers1[1] != qualifiers2[0]:
        return ".".join(qualifiers1[1:]), ".".join(qualifiers2[1:])
    return qualifiers1[-1], qualifiers2[-1]


@dataclass(frozen=True)
class TestResult:
    outcome: bool
    failure_message: Optional[str] = field(default=None, repr=False)
    constraint_description: Optional[str] = field(default=None, repr=False)
    _factual_queries: Optional[str] = field(default=None, repr=False)
    _target_queries: Optional[str] = field(default=None, repr=False)

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
    Please use the the `ref2` argument to instantiate such a constraint.
    Constraints against a fixed reference value are typically referred to as 'within' constraints.
    Please use the `ref_value` argument to instantiate such a constraint.

    A constraint typically relies on the comparison of factual and target values. The former
    represent the key quantity of interest as seen in the database, the latter the key quantity of
    interest as expected a priori. Such a comparison is meant to be carried out in the `test`
    method.

    In order to obtain such values, the `retrieve` method defines a mapping from DataReference,
    be it the DataReference of primary interest, `ref`, or a baseline DataReference, `ref2`, to
    value. If `ref_value` is already provided, usually no further mapping needs to be taken care of.
    """

    def __init__(self, ref: DataReference, *, ref2=None, ref_value: Any = None):
        self._check_if_valid_between_or_within(ref2, ref_value)
        self.ref = ref
        self.ref2 = ref2
        self.ref_value = ref_value
        self.factual_selections: OptionalSelections = None
        self.target_selections: OptionalSelections = None
        self.factual_queries: Optional[List[str]] = None
        self.target_queries: Optional[List[str]] = None

    def _check_if_valid_between_or_within(
        self, ref2: Optional[DataReference], ref_value: Optional[Any]
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

    @lru_cache(maxsize=None)
    def get_factual_value(self, engine: sa.engine.Engine) -> Any:
        factual_value, factual_selections = self.retrieve(engine, self.ref)
        self.factual_selections = factual_selections
        return factual_value

    @lru_cache(maxsize=None)
    def get_target_value(self, engine: sa.engine.Engine) -> Any:
        if self.ref2 is None:
            return self.ref_value
        target_value, target_selections = self.retrieve(engine, self.ref2)
        self.target_selections = target_selections
        return target_value

    def get_description(self) -> str:
        if self.ref2 is None:
            data_source_string = str(self.ref.data_source)
        else:
            data_source1_string = str(self.ref.data_source)
            data_source2_string = str(self.ref2.data_source)

            data_source1_substring, data_source2_substring = uncommon_substrings(
                data_source1_string, data_source2_string
            )
            data_source_string = f"{data_source1_substring} | {data_source2_substring}"
        return self.__class__.__name__ + "::" + data_source_string

    @property
    def target_prefix(self) -> str:
        return f"{self.ref2.get_string()}'s " if (self.ref2 is not None) else ""

    @property
    def condition_string(self) -> str:
        if self.ref.condition is None and (
            self.ref2 is None or self.ref2.condition is None
        ):
            return ""
        ref1_clause = self.ref.get_clause_string()
        if self.ref2 is None:
            # within constraint
            return f"Condition: {ref1_clause}"
        ref2_clause = self.ref2.get_clause_string()
        if self.ref.condition == self.ref2.condition:
            return f"Condition on both tables: {ref1_clause}; "
        if self.ref.condition is None:
            return f"Condition on second table: {ref2_clause}; "
        if self.ref2.condition is None:
            return f"Condition on first table: {ref1_clause}; "
        return (
            f"Condition on first table: {ref1_clause}. "
            f"Condition on second table: {ref2_clause}. "
        )

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[Any, OptionalSelections]:
        """Retrieve the value of interest for a DataReference from database."""
        pass

    def compare(
        self, value_factual: Any, value_target: Any
    ) -> Tuple[bool, Optional[str]]:
        pass

    def test(self, engine: sa.engine.Engine) -> TestResult:
        value_factual = self.get_factual_value(engine)
        value_target = self.get_target_value(engine)
        is_success, assertion_message = self.compare(value_factual, value_target)
        if is_success:
            return TestResult.success()

        factual_queries = None
        if self.factual_selections:
            factual_queries = [
                str(
                    factual_selection.compile(
                        engine, compile_kwargs={"literal_binds": True}
                    )
                )
                for factual_selection in self.factual_selections
            ]
        target_queries = None
        if self.target_selections:
            target_queries = [
                str(
                    target_selection.compile(
                        engine, compile_kwargs={"literal_binds": True}
                    )
                )
                for target_selection in self.target_selections
            ]
        return TestResult.failure(
            assertion_message,
            self.get_description(),
            factual_queries,
            target_queries,
        )


def format_sample(sample, ref: DataReference) -> str:
    """Build a string from a database row indicating its column values."""
    if ref.columns is None:
        return str(sample)
    sample_string = " , ".join(
        map(lambda x: f"{x[0]} = {x[1]}", zip(ref.columns, list(sample)))
    )
    return sample_string
