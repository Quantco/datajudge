import abc
from collections import Counter
from itertools import zip_longest
from math import ceil, floor
from typing import Callable, Collection, Dict, List, Optional, Set, Tuple, Union

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, OptionalSelections, T, TestResult, ToleranceGetter


def _is_superset(values1: Collection[T], values2: Collection[T]) -> Tuple[bool, Set[T]]:
    """Check whether values1 is a superset of values2."""
    remainder = set(values2) - set(values1)
    return len(remainder) == 0, remainder


def _is_subset(values1: Collection[T], values2: Collection[T]) -> Tuple[bool, Set[T]]:
    """Check whether values1 is a subset of values2."""
    remainder = set(values1) - set(values2)
    return len(remainder) == 0, remainder


def _subset_violation_counts(
    values1: Collection[T], counts: List[int], values2: Collection[T]
) -> Tuple[bool, Dict[Union[T, int], int]]:
    """Count frequencies of elements from values1 not in values2."""
    remainder = {
        value: count
        for (value, count) in zip_longest(values1, counts, fillvalue=-1)
        if value not in values2
    }
    return len(remainder) == 0, remainder


class Uniques(Constraint, abc.ABC):
    """Uniques is an abstract class for comparisons between unique values of a column and a reference.

    The `Uniques` constraint asserts if the values contained in a column of a `DataSource`
    are part of a reference set of expected values - either externally supplied
    through parameter `uniques` or obtained from another `DataSource`.

    Null values in the column are ignored. To assert the non-existence of them use
    the `NullAbsence` constraint via the `add_null_absence_constraint` helper method for
    `WithinRequirement`.

    There are two ways to do some post processing of the data obtained from the
    database by providing a function to be executed. In general, no postprocessing
    is needed, but there are some cases where it's the only thing to do. For example,
    with text values that have some structure.

    One is `map_func`, it'll be executed over each obtained 'unique' value. This is a very
    local operation.

    If `map_func` is provided, it'll be executed over each obtained 'unique'
    value.

    The second one is `reduce_func` which will take the whole data retrieved and
    can perform global processing. If it is provided, it gets applied after the function
    given in `map_func` is finished. The output of this function has to be an iterable
    (eager or lazy) of the same type as the type of the values of the column (in their
    Python equivalent).

    One use is of this constraint is to test for consistency in columns with expected
    categorical values.
    """

    def __init__(
        self,
        ref: DataReference,
        name: str = None,
        *,
        ref2: DataReference = None,
        uniques: Collection = None,
        map_func: Callable[[T], T] = None,
        reduce_func: Callable[[Collection], Collection] = None,
        max_relative_violations=0,
    ):
        ref_value: Optional[Tuple[Collection, List]]
        ref_value = (uniques, []) if uniques else None
        super().__init__(ref, ref2=ref2, ref_value=ref_value, name=name)
        self.local_func = map_func
        self.global_func = reduce_func
        self.max_relative_violations = max_relative_violations

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[Tuple[List[T], List[int]], OptionalSelections]:
        uniques, selection = db_access.get_uniques(engine, ref)
        values = list(uniques.keys())
        values = list(filter(lambda value: value is not None, values))
        counts = [uniques[value] for value in values]
        if self.local_func:
            values = list(map(self.local_func, values))
        if self.global_func:
            values = list(self.global_func(values))
            if not isinstance(values, Collection):
                raise ValueError(
                    "The return value from `reduce_func` is not a Collection."
                )
        return (values, counts), selection


class UniquesEquality(Uniques):
    def __init__(self, args, name: str = None, **kwargs):
        if kwargs.get("max_relative_violations"):
            raise RuntimeError("Some useful message")
        super().__init__(args, name=name, **kwargs)

    def compare(
        self,
        factual: Tuple[List[T], List[int]],
        target: Tuple[Collection[T], List[int]],
    ) -> Tuple[bool, Optional[str]]:
        factual_values_list, _ = factual
        factual_values = set(factual_values_list)
        target_values_list, _ = target
        target_values = set(target_values_list)
        is_subset, excess_values = _is_subset(factual_values, target_values)
        is_superset, lacking_values = _is_superset(factual_values, target_values)
        if not is_subset and not is_superset:
            assertion_text = (
                f"{self.ref.get_string()} doesn't have the element(s) "
                f"'{lacking_values}' and has the excess element(s) "
                f"'{excess_values}' when compared with the reference values. "
                f"{self.condition_string}"
            )
            return False, assertion_text
        if not is_subset:
            assertion_text = (
                f"{self.ref.get_string()} has the excess element(s) "
                f"'{excess_values}' when compared with the reference values. "
                f"{self.condition_string}"
            )
            return False, assertion_text
        if not is_superset:
            assertion_text = (
                f"{self.ref.get_string()} doesn't have the element(s) "
                f"'{lacking_values}' when compared with the reference values. "
                f"{self.condition_string}"
            )
            return False, assertion_text
        return True, None


class UniquesSubset(Uniques):
    def compare(
        self,
        factual: Tuple[List[T], List[int]],
        target: Tuple[Collection[T], List[int]],
    ) -> Tuple[bool, Optional[str]]:
        factual_values, factual_counts = factual
        target_values, _ = target
        is_subset, remainder = _subset_violation_counts(
            factual_values, factual_counts, target_values
        )
        n_rows = sum(factual_counts)
        n_violations = sum(remainder.values())
        if (
            n_rows > 0
            and (relative_violations := (n_violations / n_rows))
            > self.max_relative_violations
        ):
            assertion_text = (
                f"{self.ref.get_string()} has a fraction of {relative_violations} > "
                f"{self.max_relative_violations} values not being an element of "
                f"'{set(target_values)}'. It has e.g. excess elements "
                f"'{list(remainder.keys())[:5]}'."
                f"{self.condition_string}"
            )
            return False, assertion_text
        return True, None


class UniquesSuperset(Uniques):
    def compare(
        self,
        factual: Tuple[List[T], List[int]],
        target: Tuple[Collection[T], List[int]],
    ) -> Tuple[bool, Optional[str]]:
        factual_values, _ = factual
        target_values, _ = target
        is_superset, remainder = _is_superset(factual_values, target_values)
        if (
            len(factual_values) > 0
            and (relative_violations := (len(remainder) / len(target_values)))
            > self.max_relative_violations
        ):
            assertion_text = (
                f"{self.ref.get_string()} has a fraction of "
                f"{relative_violations} > {self.max_relative_violations} "
                f"lacking unique values of '{set(target_values)}'. E.g. it "
                f"doesn't have the unique value(s) '{list(remainder)[:5]}'."
                f"{self.condition_string}"
            )
            return False, assertion_text
        return True, None


class NUniques(Constraint, abc.ABC):
    def __init__(
        self,
        ref: DataReference,
        *,
        ref2: DataReference = None,
        n_uniques: int = None,
        name: str = None,
    ):
        super().__init__(ref, ref2=ref2, ref_value=n_uniques, name=name)

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[int, OptionalSelections]:
        return db_access.get_unique_count(engine, ref)


class NUniquesEquality(NUniques):
    def compare(
        self, n_uniques_factual: int, n_uniques_target: int
    ) -> Tuple[bool, Optional[str]]:
        result = n_uniques_factual == n_uniques_target
        assertion_text = (
            f"{self.ref.get_string()} has {n_uniques_factual} "
            f"unique(s) instead of {self.target_prefix}"
            f"{n_uniques_target}. "
            f"{self.condition_string}"
        )
        return result, assertion_text


class NUniquesMaxLoss(NUniques):
    def __init__(
        self,
        ref: DataReference,
        ref2: DataReference,
        max_relative_loss_getter: ToleranceGetter,
        name: str = None,
    ):
        super().__init__(ref, ref2=ref2, name=name)
        self.max_relative_loss_getter = max_relative_loss_getter

    def compare(
        self, n_uniques_factual: int, n_uniques_target: int
    ) -> Tuple[bool, Optional[str]]:
        if n_uniques_target == 0 or n_uniques_factual > n_uniques_target:
            return True, None
        relative_loss = (n_uniques_target - n_uniques_factual) / n_uniques_target
        assertion_text = (
            f"{self.ref.get_string()} has lost {relative_loss} "
            f"of #uniques of table {self.ref2.get_string()}. It "
            f"was only allowed to decrease "
            f"{self.max_relative_loss}. "
            f"{self.condition_string}"
        )
        result = relative_loss <= self.max_relative_loss
        return result, assertion_text
        return TestResult(result, assertion_text)

    def test(self, engine: sa.engine.Engine) -> TestResult:
        self.max_relative_loss = self.max_relative_loss_getter(engine)
        return super().test(engine)


class NUniquesMaxGain(NUniques):
    def __init__(
        self,
        ref: DataReference,
        ref2: DataReference,
        max_relative_gain_getter: ToleranceGetter,
        name: str = None,
    ):
        super().__init__(ref, ref2=ref2, name=name)
        self.max_relative_gain_getter = max_relative_gain_getter

    def compare(
        self, n_uniques_factual: int, n_uniques_target: int
    ) -> Tuple[bool, Optional[str]]:
        if n_uniques_target == 0:
            return False, "Target table empty."
        if n_uniques_factual < n_uniques_target:
            return True, None
        relative_gain = (n_uniques_factual - n_uniques_target) / n_uniques_target
        assertion_text = (
            f"{self.ref.get_string()} has {relative_gain} of "
            f"#uniques of {self.ref2.get_string()}. It was only "
            f"allowed to increase {self.max_relative_gain} . "
            f"{self.condition_string}"
        )
        result = relative_gain <= self.max_relative_gain
        return result, assertion_text

    def test(self, engine: sa.engine.Engine) -> TestResult:
        self.max_relative_gain = self.max_relative_gain_getter(engine)
        return super().test(engine)


class CategoricalBoundConstraint(Constraint):
    """
    `CategoricalBoundConstraint` is a constraint class that checks if the share of specific values
    in a column falls within predefined bounds. It compares the actual distribution of values in a
    `DataSource` column with a target distribution, supplied as a dictionary.

    Example use cases include testing for consistency in columns with expected categorical values
    or ensuring that the distribution of values in a column adheres to a certain criterion.

    Parameters
    ----------
    ref : DataReference
        A reference to the column in the data source.
    distribution : Dict[T, Tuple[float, float]]
        A dictionary with unique values as keys and tuples of minimum and maximum allowed shares as values.
    default_bounds : Tuple[float, float], optional, default=(0, 0)
        A tuple specifying the minimum and maximum bounds for values not explicitly outlined in the target
        distribution dictionary.
    name : Optional[str], default=None
        An optional name for the constraint.
    max_relative_violations : float, optional, default=0
        A tolerance threshold (0 to 1) for the proportion of elements in the data that can violate the
        bound constraints without triggering the constraint violation.
    """

    def __init__(
        self,
        ref: DataReference,
        distribution: Dict[T, Tuple[float, float]],
        default_bounds: Tuple[float, float] = (0, 0),
        name: Optional[str] = None,
        max_relative_violations: float = 0,
        **kwargs,
    ):
        self.default_bounds = default_bounds
        self.max_relative_violations = max_relative_violations
        super().__init__(ref, ref_value=distribution, name=name, **kwargs)

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[Counter, OptionalSelections]:
        return db_access.get_uniques(engine, ref)

    def compare(
        self,
        factual: Counter,
        target: Dict[T, Tuple[float, float]],
    ) -> Tuple[bool, Optional[str]]:
        # TODO: use .total() of Counter as soon as we can assume Python 3.10
        total = sum(factual.values())
        all_variants = factual.keys() | target.keys()
        min_counts = Counter(
            {k: target.get(k, self.default_bounds)[0] * total for k in all_variants}
        )
        max_counts = Counter(
            {k: target.get(k, self.default_bounds)[1] * total for k in all_variants}
        )

        violations = (factual - max_counts) + (min_counts - factual)

        if (
            # TODO: use .total() of Counter as soon as we can assume Python 3.10
            relative_violations := (sum(violations.values()) / total)
        ) > self.max_relative_violations:
            assertion_text = (
                f"{self.ref.get_string()} has {relative_violations * 100}% > "
                f"{self.max_relative_violations * 100}% of element(s) violating the bound constraints:\n"
            )

            for variant in violations:
                actual_share = factual[variant] / total
                target_share = target.get(variant, self.default_bounds)
                min_required = min_counts[variant]
                max_required = max_counts[variant]

                assertion_text += (
                    f"'{variant}' with a share of {actual_share * 100}% "
                    f"({factual[variant]} out of {total}) "
                    f"while a share between {target_share[0] * 100}% ({ceil(min_required)}) "
                    f"and {target_share[1] * 100}% ({floor(max_required)}) "
                    f"is required\n"
                )

            assertion_text += f"{self.condition_string}"
            return False, assertion_text
        return True, None
