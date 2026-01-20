from __future__ import annotations

import abc
import warnings
from collections import Counter
from collections.abc import Callable, Collection
from itertools import zip_longest
from math import ceil, floor

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from ..utils import OutputProcessor, filternull_element, output_processor_limit
from .base import _T, Constraint, TestResult, _OptionalSelections, _ToleranceGetter


def _is_superset(
    values1: Collection[_T], values2: Collection[_T]
) -> tuple[bool, set[_T]]:
    """Check whether values1 is a superset of values2."""
    remainder = set(values2) - set(values1)
    return len(remainder) == 0, remainder


def _is_subset(
    values1: Collection[_T], values2: Collection[_T]
) -> tuple[bool, set[_T]]:
    """Check whether values1 is a subset of values2."""
    remainder = set(values1) - set(values2)
    return len(remainder) == 0, remainder


def _subset_violation_counts(
    values1: Collection[_T], counts: list[int], values2: Collection[_T]
) -> tuple[bool, dict[_T | int, int]]:
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

    Null values in the columns ``columns`` are ignored. To assert the non-existence of them use
    the [add_null_absence_constraint][datajudge.requirements.WithinRequirement.add_null_absence_constraint] helper method
    for ``WithinRequirement``.
    By default, the null filtering does not trigger if multiple columns are fetched at once.
    It can be configured in more detail by supplying a custom ``filter_func`` function.
    Some exemplary implementations are available as [filternull_element][datajudge.utils.filternull_element],
    [filternull_never][datajudge.utils.filternull_never], [filternull_element_or_tuple_all][datajudge.utils.filternull_element_or_tuple_all],
    [filternull_element_or_tuple_any][datajudge.utils.filternull_element_or_tuple_any].
    Passing ``None`` as the argument is equivalent to [filternull_element][datajudge.utils.filternull_element] but triggers a warning.
    The current default of [filternull_element][datajudge.utils.filternull_element]
    Cause (possibly often unintended) changes in behavior when the users adds a second column
    (filtering no longer can trigger at all).
    The default will be changed to [filternull_element_or_tuple_all][datajudge.utils.filternull_element_or_tuple_all] in future versions.
    To silence the warning, set ``filter_func`` explicitly.

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

    Furthermore, the `max_relative_violations` parameter can be used to set a tolerance
    threshold for the proportion of elements in the data that can violate the constraint
    (default: 0).
    Setting this argument is currently not supported for `UniquesEquality`.

    For `UniquesSubset`, by default,
    the number of occurrences affects the computed fraction of violations.
    To disable this weighting, set `compare_distinct=True`.
    This argument does not have an effect on the test results for other `Uniques` constraints,
    or if `max_relative_violations` is 0.

    By default, the assertion messages make use of sets,
    thus, they may differ from run to run despite the exact same situation being present,
    and can have an arbitrary length.
    To enforce a reproducible, limited output via (e.g.) sorting and slicing,
    set `output_processors` to a callable or a list of callables. By default, only the first 100 elements are displayed ([output_processor_limit][datajudge.utils.output_processor_limit]).

    Each callable takes in two collections, and returns modified (e.g. sorted) versions of them.
    In most cases, the second argument is simply None,
    but for `UniquesSubset` it is the counts of each of the elements.
    The suggested functions are [output_processor_sort][datajudge.utils.output_processor_sort] and [output_processor_limit][datajudge.utils.output_processor_limit]
    - see their respective docstrings for details.

    One use is of this constraint is to test for consistency in columns with expected
    categorical values.
    """

    def __init__(
        self,
        ref: DataReference,
        name: str | None = None,
        cache_size=None,
        output_processors: OutputProcessor
        | list[OutputProcessor]
        | None = output_processor_limit,
        *,
        ref2: DataReference | None = None,
        uniques: Collection | None = None,
        filter_func: Callable[[list[_T]], list[_T]] | None = None,
        map_func: Callable[[_T], _T] | None = None,
        reduce_func: Callable[[Collection], Collection] | None = None,
        max_relative_violations=0,
        compare_distinct=False,
    ):
        ref_value: tuple[Collection, list] | None
        ref_value = (uniques, []) if uniques else None
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=ref_value,
            name=name,
            cache_size=cache_size,
            output_processors=output_processors,
        )

        if filter_func is None:
            warnings.warn("Using deprecated default null filter function.")
            filter_func = filternull_element

        self._filter_func = filter_func
        self._local_func = map_func
        self._global_func = reduce_func
        self._max_relative_violations = max_relative_violations
        self._compare_distinct = compare_distinct

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[tuple[list, list[int]], _OptionalSelections]:
        uniques, selection = db_access.get_uniques(engine, ref)
        values = list(uniques.keys())
        values = self._filter_func(values)
        counts = [uniques[value] for value in values]
        if self._local_func:
            values = list(map(self._local_func, values))
        if self._global_func:
            values = list(self._global_func(values))
            if not isinstance(values, Collection):
                raise ValueError(
                    "The return value from `reduce_func` is not a Collection."
                )
        return (values, counts), selection


class UniquesEquality(Uniques):
    def __init__(self, args, name: str | None = None, cache_size=None, **kwargs):
        if kwargs.get("max_relative_violations"):
            raise RuntimeError(
                "max_relative_violations is not supported for UniquesEquality."
            )
        if kwargs.get("compare_distinct"):
            raise RuntimeError("compare_distinct is not supported for UniquesEquality.")
        super().__init__(args, name=name, **kwargs)

    def _compare(
        self,
        value_factual: tuple[list[_T], list[int]],
        value_target: tuple[Collection[_T], list[int]],
    ) -> tuple[bool, str | None]:
        factual_values_list, _ = value_factual
        factual_values = set(factual_values_list)
        target_values_list, _ = value_target
        target_values = set(target_values_list)
        is_subset, excess_values = _is_subset(factual_values, target_values)
        is_superset, lacking_values = _is_superset(factual_values, target_values)
        if not is_subset and not is_superset:
            assertion_text = (
                f"{self._ref} doesn't have the element(s) "
                f"'{self._apply_output_formatting(lacking_values)}' and has the excess element(s) "
                f"'{self._apply_output_formatting(excess_values)}' when compared with the reference values. "
                f"{self._condition_string}"
            )
            return False, assertion_text
        if not is_subset:
            assertion_text = (
                f"{self._ref} has the excess element(s) "
                f"'{self._apply_output_formatting(excess_values)}' when compared with the reference values. "
                f"{self._condition_string}"
            )
            return False, assertion_text
        if not is_superset:
            assertion_text = (
                f"{self._ref} doesn't have the element(s) "
                f"'{self._apply_output_formatting(lacking_values)}' when compared with the reference values. "
                f"{self._condition_string}"
            )
            return False, assertion_text
        return True, None


class UniquesSubset(Uniques):
    def _compare(
        self,
        value_factual: tuple[list[_T], list[int]],
        value_target: tuple[Collection[_T], list[int]],
    ) -> tuple[bool, str | None]:
        factual_values, factual_counts = value_factual
        target_values, _ = value_target

        is_subset, remainder = _subset_violation_counts(
            factual_values, factual_counts, target_values
        )
        if not self._compare_distinct:
            n_rows = sum(factual_counts)
            n_violations = sum(remainder.values())
        else:
            n_rows = len(factual_values)
            n_violations = len(remainder)

        if (
            n_rows > 0
            and (relative_violations := (n_violations / n_rows))
            > self._max_relative_violations
        ):
            output_elemes, output_counts = (
                list(remainder.keys()),
                list(remainder.values()),
            )
            if self._output_processors is not None:
                for output_processor in self._output_processors:
                    output_elemes, output_counts = output_processor(
                        output_elemes, output_counts
                    )

            assertion_text = (
                f"{self._ref} has a fraction of {relative_violations} > "
                f"{self._max_relative_violations} {'DISTINCT ' if self._compare_distinct else ''}values ({n_violations} / {n_rows}) not being an element of "
                f"'{self._apply_output_formatting(set(target_values))}'. It has excess elements "
                f"'{output_elemes}' "
                f"with counts {output_counts}."
                f"{self._condition_string}"
            )
            return False, assertion_text
        return True, None


class UniquesSuperset(Uniques):
    def __init__(self, args, name: str | None = None, cache_size=None, **kwargs):
        if kwargs.get("compare_distinct"):
            raise RuntimeError("compare_distinct is not supported for UniquesSuperset.")
        super().__init__(args, name=name, **kwargs)

    def _compare(
        self,
        value_factual: tuple[list[_T], list[int]],
        value_target: tuple[Collection[_T], list[int]],
    ) -> tuple[bool, str | None]:
        factual_values, _ = value_factual
        target_values, _ = value_target
        is_superset, remainder = _is_superset(factual_values, target_values)
        if (
            len(factual_values) > 0
            and (
                relative_violations := (
                    (n_violations := (len(remainder))) / (n_rows := len(target_values))
                )
            )
            > self._max_relative_violations
        ):
            assertion_text = (
                f"{self._ref} has a fraction of "
                f"{relative_violations} > {self._max_relative_violations} ({n_violations} / {n_rows}) "
                f"lacking unique values of '{self._apply_output_formatting(set(target_values))}'. It "
                f"doesn't have the unique value(s) '{self._apply_output_formatting(list(remainder))}'."
                f"{self._condition_string}"
            )
            return False, assertion_text
        return True, None


class NUniques(Constraint, abc.ABC):
    def __init__(
        self,
        ref: DataReference,
        *,
        ref2: DataReference | None = None,
        n_uniques: int | None = None,
        name: str | None = None,
        cache_size=None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=n_uniques,
            name=name,
            cache_size=cache_size,
        )

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[int, _OptionalSelections]:
        return db_access.get_unique_count(engine, ref)


class NUniquesEquality(NUniques):
    def _compare(
        self, value_factual: int, value_target: int
    ) -> tuple[bool, str | None]:
        result = value_factual == value_target
        assertion_text = (
            f"{self._ref} has {value_factual} "
            f"unique(s) instead of {self._target_prefix}"
            f"{value_target}. "
            f"{self._condition_string}"
        )
        return result, assertion_text


class NUniquesMaxLoss(NUniques):
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

    def _compare(
        self, value_factual: int, value_target: int
    ) -> tuple[bool, str | None]:
        if value_target == 0 or value_factual > value_target:
            return True, None
        relative_loss = (value_target - value_factual) / value_target
        assertion_text = (
            f"{self._ref} has lost {relative_loss} "
            f"of #uniques of table {self._ref2}. It "
            f"was only allowed to decrease "
            f"{self.max_relative_loss}. "
            f"{self._condition_string}"
        )
        result = relative_loss <= self.max_relative_loss
        return result, assertion_text

    def test(self, engine: sa.engine.Engine) -> TestResult:
        self.max_relative_loss = self.max_relative_loss_getter(engine)
        return super().test(engine)


class NUniquesMaxGain(NUniques):
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

    def _compare(
        self, value_factual: int, value_target: int
    ) -> tuple[bool, str | None]:
        if value_target == 0:
            return False, "Target table empty."
        if value_factual < value_target:
            return True, None
        relative_gain = (value_factual - value_target) / value_target
        assertion_text = (
            f"{self._ref} has {relative_gain} of "
            f"#uniques of {self._ref2}. It was only "
            f"allowed to increase {self._max_relative_gain} . "
            f"{self._condition_string}"
        )
        result = relative_gain <= self._max_relative_gain
        return result, assertion_text

    def test(self, engine: sa.engine.Engine) -> TestResult:
        self._max_relative_gain = self._max_relative_gain_getter(engine)
        return super().test(engine)


class CategoricalBoundConstraint(Constraint):
    """Constraint that checks if the share of specific values in a column falls within predefined bounds.

    It compares the actual distribution of values in a
    `DataSource` column with a target distribution, supplied as a dictionary.

    Example use cases include testing for consistency in columns with expected categorical values
    or ensuring that the distribution of values in a column adheres to a certain criterion.

    Parameters
    ----------
    ref : DataReference
        A reference to the column in the data source.
    distribution : dict[_T, tuple[float, float]]
        A dictionary with unique values as keys and tuples of minimum and maximum allowed shares as values.
    default_bounds : tuple[float, float], optional, default=(0, 0)
        A tuple specifying the minimum and maximum bounds for values not explicitly outlined in the target
        distribution dictionary.
    name : str | None, default=None
        An optional name for the constraint.
    max_relative_violations : float, optional, default=0
        A tolerance threshold (0 to 1) for the proportion of elements in the data that can violate the
        bound constraints without triggering the constraint violation.
    """

    def __init__(
        self,
        ref: DataReference,
        distribution: dict[_T, tuple[float, float]],
        default_bounds: tuple[float, float] = (0, 0),
        name: str | None = None,
        cache_size=None,
        max_relative_violations: float = 0,
        **kwargs,
    ):
        self._default_bounds = default_bounds
        self._max_relative_violations = max_relative_violations
        super().__init__(
            ref,
            ref_value=distribution,
            name=name,
            cache_size=cache_size,
            **kwargs,
        )

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[Counter, _OptionalSelections]:
        return db_access.get_uniques(engine, ref)

    def _compare(
        self,
        value_factual: Counter,
        value_target: dict[_T, tuple[float, float]],
    ) -> tuple[bool, str | None]:
        total = value_factual.total()
        all_variants = value_factual.keys() | value_target.keys()
        min_counts = Counter(
            {
                k: value_target.get(k, self._default_bounds)[0] * total
                for k in all_variants
            }
        )
        max_counts = Counter(
            {
                k: value_target.get(k, self._default_bounds)[1] * total
                for k in all_variants
            }
        )

        violations = (value_factual - max_counts) + (min_counts - value_factual)

        if (
            relative_violations := violations.total() / total
        ) > self._max_relative_violations:
            assertion_text = (
                f"{self._ref} has {relative_violations * 100}% > "
                f"{self._max_relative_violations * 100}% of element(s) violating the bound constraints:\n"
            )

            for variant in violations:
                actual_share = value_factual[variant] / total
                target_share = value_target.get(variant, self._default_bounds)
                min_required = min_counts[variant]
                max_required = max_counts[variant]

                assertion_text += (
                    f"'{variant}' with a share of {actual_share * 100}% "
                    f"({value_factual[variant]} out of {total}) "
                    f"while a share between {target_share[0] * 100}% ({ceil(min_required)}) "
                    f"and {target_share[1] * 100}% ({floor(max_required)}) "
                    f"is required\n"
                )

            assertion_text += f"{self._condition_string}"
            return False, assertion_text
        return True, None
