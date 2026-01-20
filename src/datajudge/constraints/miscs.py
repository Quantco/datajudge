from __future__ import annotations

import warnings

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, TestResult, _format_sample, _OptionalSelections


class PrimaryKeyDefinition(Constraint):
    def __init__(
        self,
        ref,
        primary_keys: list[str],
        name: str | None = None,
        cache_size=None,
    ):
        super().__init__(ref, ref_value=set(primary_keys), name=name)

    def _retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> tuple[set[str], _OptionalSelections]:
        values, selections = db_access.get_primary_keys(engine, self._ref)
        return set(values), selections

    # Note: Exact equality!
    def _compare(
        self, value_factual: set[str], value_target: set[str]
    ) -> tuple[bool, str | None]:
        assertion_message = ""
        result = True
        # If both are true, just report one.
        if len(value_factual.difference(value_target)) > 0:
            example_key = next(iter(value_factual.difference(value_target)))
            assertion_message = (
                f"{self._ref} incorrectly includes {example_key} as primary key."
            )
            result = False
        if len(value_target.difference(value_factual)) > 0:
            example_key = next(iter(value_target.difference(value_factual)))
            assertion_message = (
                f"{self._ref} doesn't include {example_key} as primary key."
            )
            result = False
        return result, assertion_message


class Uniqueness(Constraint):
    # In contrast to PrimaryKeyDefinition, it is hardly imaginable to have
    # this constraint be used between tables.
    def __init__(
        self,
        ref: DataReference,
        max_duplicate_fraction: float = 0,
        max_absolute_n_duplicates: int = 0,
        infer_pk_columns: bool = False,
        name: str | None = None,
        cache_size=None,
    ):
        if max_duplicate_fraction != 0 and max_absolute_n_duplicates != 0:
            raise ValueError(
                """Uniqueness constraint was attempted to be constructed
                with both a relative and an absolute tolerance. Only use one
                of both at a time."""
            )
        if max_duplicate_fraction != 0:
            ref_value = ("relative", max_duplicate_fraction)
        elif max_absolute_n_duplicates != 0:
            ref_value = ("absolute", max_absolute_n_duplicates)
        else:
            ref_value = ("relative", 0)

        self.infer_pk_columns = infer_pk_columns
        super().__init__(ref, ref_value=ref_value, name=name, cache_size=cache_size)

    def test(self, engine: sa.engine.Engine) -> TestResult:
        if self.infer_pk_columns and db_access.is_bigquery(engine):
            raise NotImplementedError("No primary key concept in BigQuery")

        # only check for primary keys when actually defined
        # otherwise default back to searching the whole table
        if self.infer_pk_columns and (
            pk_columns := db_access.get_primary_keys(engine, self._ref)[0]
        ):
            self._ref.columns = pk_columns
            if not pk_columns:  # there were no primary keys found
                warnings.warn(
                    f"""No primary keys found in {self._ref}.
                    Uniqueness will be tested for all columns."""
                )

        unique_count, unique_selections = db_access.get_unique_count(engine, self._ref)
        row_count, row_selections = db_access.get_row_count(engine, self._ref)
        self.factual_selections = row_selections
        self.target_selections = unique_selections
        if row_count == 0:
            return TestResult(True, "No occurrences.")

        tolerance_kind, tolerance_value = self._ref_value

        if tolerance_kind == "relative":
            result = unique_count >= row_count * (1 - tolerance_value)
        elif tolerance_kind == "absolute":
            result = unique_count >= row_count - tolerance_value
        else:
            raise ValueError(
                "Given tolerance is neither relative nor absolute: {tolerance_kind}."
            )
        if result:
            return TestResult.success()
        sample, _ = db_access.get_duplicate_sample(engine, self._ref)
        sample_string = _format_sample(sample, self._ref)
        assertion_text = (
            f"{self._ref} has {row_count} rows > {unique_count} "
            f"uniques. This surpasses the max_duplicate_fraction of "
            f"{self._ref_value}. An example tuple breaking the "
            f"uniqueness condition is: {sample_string}."
        )
        return TestResult.failure(assertion_text)


class FunctionalDependency(Constraint):
    def __init__(self, ref: DataReference, key_columns: list[str], **kwargs):
        super().__init__(ref, ref_value=object(), **kwargs)
        self.key_columns = key_columns

    def test(self, engine: sa.engine.Engine) -> TestResult:
        violations, _ = db_access.get_functional_dependency_violations(
            engine, self._ref, self.key_columns
        )
        if not violations:
            return TestResult.success()

        assertion_text = (
            f"{self._ref} has violations of functional dependence (in total {len(violations)} rows):\n"
            + "\n".join(
                [
                    f"{violation}"
                    for violation in self._apply_output_formatting(
                        [tuple(elem) for elem in violations]
                    )
                ]
            )
        )
        return TestResult.failure(assertion_text)


class MaxNullFraction(Constraint):
    def __init__(
        self,
        ref,
        *,
        ref2: DataReference | None = None,
        max_null_fraction: float | None = None,
        max_relative_deviation: float = 0,
        name: str | None = None,
        cache_size=None,
    ):
        super().__init__(
            ref,
            ref2=ref2,
            ref_value=max_null_fraction,
            name=name,
            cache_size=cache_size,
        )
        if max_null_fraction is not None and not (0 <= max_null_fraction <= 1):
            raise ValueError(
                f"max_null_fraction was expected to lie within [0, 1] but is "
                f"{max_null_fraction}."
            )
        if max_relative_deviation < 0:
            raise ValueError(
                f"{max_relative_deviation} is negative even though it needs to be positive."
            )
        self.max_relative_deviation = max_relative_deviation

    def _retrieve(self, engine: sa.engine.Engine, ref: DataReference):
        return db_access.get_missing_fraction(engine=engine, ref=ref)

    def _compare(
        self, value_factual: float, value_target: float
    ) -> tuple[bool, str | None]:
        threshold = value_target * (1 + self.max_relative_deviation)
        result = value_factual <= threshold
        assertion_text = (
            f"{value_factual} of {self._ref} values are NULL "
            f"while only {self._target_prefix}{threshold} were allowed to be NULL."
        )
        return result, assertion_text
