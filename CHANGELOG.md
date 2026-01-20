# Changelog

## 1.13.0 - 2026.01.21

- Deprecate `sqlalchemy` <2.0.0.

## 1.12.0 - 2026.01.12

- Drop support impala as a backend.
- Add support for DuckDB as a backend.

## 1.11.0 - 2025.12.17

- Drop support for Python 3.8 and Python 3.9.

## 1.10.0 - 2025.02.13

- Address deprecation warnings from `sqlalchemy`.
- Provide more type hints.

## 1.9.3 - 2025.01.13

**Bug fixes**

- Fix a bug in
  [`datajudge.WithinRequirement.add_date_no_overlap_constraint`][datajudge.requirements.WithinRequirement.add_date_no_overlap_constraint] and
  [`datajudge.WithinRequirement.add_date_no_overlap_2d_constraint`][datajudge.requirements.WithinRequirement.add_date_no_overlap_2d_constraint] and
  [`datajudge.WithinRequirement.add_numeric_no_overlap_constraint`][datajudge.requirements.WithinRequirement.add_numeric_no_overlap_constraint] in
  which some overlaps were not detected due to equality of their
  leftmost bounds.

## 1.9.2 - 2024.09.05

**Bug fixes**

- Fix a bug in [`datajudge.constraints.numeric.NumericPercentile`][datajudge.constraints.numeric.NumericPercentile] which
  could lead to off-by-one errors in retrieving a percentile value.

## 1.9.0 - 2024.06.25

**New features**

- Add styling for assertion messages. See `assertion-message-styling`
  for more information.
- Add `output_processors` and `filter_func` parameters to
  [`datajudge.WithinRequiremen.tadd_uniques_equality_constraint`][datajudge.requirements.WithinRequirement.add_uniques_equality_constraint],
  [`datajudge.WithinRequirement.add_uniques_superset_constraint`][datajudge.requirements.WithinRequirement.add_uniques_superset_constraint]
  and
  [`datajudge.WithinRequirement.add_uniques_subset_constraint`][datajudge.requirements.WithinRequirement.add_uniques_subset_constraint].
- Add `output_processors`, `filter_func` and `compare_distinct`
  parameters to
  [`datajudge.BetweenRequirement.add_uniques_equality_constraint`][datajudge.requirements.BetweenRequirement.add_uniques_equality_constraint],
  [`datajudge.BetweenRequirement.add_uniques_superset_constraint`][datajudge.requirements.BetweenRequirement.add_uniques_superset_constraint]
  and
  [`datajudge.BetweenRequirement.add_uniques_subset_constraint`][datajudge.requirements.BetweenRequirement.add_uniques_subset_constraint].
- Add `output_processors` parameter to
  [`datajudge.WithinRequirement.add_functional_dependency_constraint`][datajudge.requirements.WithinRequirement.add_functional_dependency_constraint].

**Other changes**

- Provide a `py.typed` file.
- Remove usage of `pkg_resources`.

## 1.8.0 - 2023.06.16

**New features**

- Implement
  [`datajudge.WithinRequirement.add_functional_dependency_constraint`][datajudge.requirements.WithinRequirement.add_functional_dependency_constraint].

**Other changes**

- Improve error message when a `DataReference` is constructed
  with a single column name instead of specifying a list of columns.

## 1.7.0 - 2023.05.11

**New features**

- Implement
  [`datajudge.WithinRequirement.add_categorical_bound_constraint`][datajudge.requirements.WithinRequirement.add_categorical_bound_constraint].
- Extended [`datajudge.WithinRequirement.add_column_type_constraint`][datajudge.requirements.WithinRequirement.add_column_type_constraint] to
  support column type specification using string format,
  backend-specific SQLAlchemy types, and SQLAlchemy's generic types.
- Implement [`datajudge.WithinRequirement.add_numeric_no_gap_constraint`][datajudge.requirements.WithinRequirement.add_numeric_no_gap_constraint],
  [`datajudge.WithinRequirement.add_numeric_no_overlap_constraint`][datajudge.requirements.WithinRequirement.add_numeric_no_overlap_constraint],

## 1.6.0 - 2023.04.12

**Other changes**

- Ensure compatibility with `sqlalchemy` \>= 2.0.

## 1.5.0 - 2023.03.14

**New features**

- Implement
  [`datajudge.BetweenRequirement.add_max_null_fraction_constraint`][datajudge.requirements.BetweenRequirement.add_max_null_fraction_constraint] and
  [`datajudge.WithinRequirement.add_max_null_fraction_constraint`][datajudge.requirements.WithinRequirement.add_max_null_fraction_constraint].
- Implement
  [`datajudge.BetweenRequirement.add_numeric_percentile_constraint`][datajudge.requirements.BetweenRequirement.add_numeric_percentile_constraint] and
  [`datajudge.WithinRequirement.add_numeric_percentile_constraint`][datajudge.requirements.WithinRequirement.add_numeric_percentile_constraint].

## 1.4.0 - 2023.02.24

**New features**

- Add partial and experimental support for db2 as a backend.

## 1.3.0 - 2023.01.17

**New features**

- Implement [`datajudge.BetweenRequirement.add_column_type_constraint`][datajudge.requirements.BetweenRequirement.add_column_type_constraint].
  Previously, only the `WithinRequirement` method existed.
- Implemented an option `infer_pk` to automatically retrieve and primary
  key definition as part of
  [`datajudge.WithinRequirement.add_uniqueness_constraint`][datajudge.requirements.WithinRequirement.add_uniqueness_constraint].
- Added a `name` parameter to all `add_x_constraint` methods of
  `WithinRequirement` and `BetweenRequirement`. This will give pytest
  test a custom name.
- Added preliminary support for Impala.

**Other changes**

- Improve assertion error for
  [`datajudge.BetweenRequirement.add_row_matching_equality_constraint`][datajudge.requirements.BetweenRequirement.add_row_matching_equality_constraint].

## 1.2.0 - 2022.10.21

**New features**

- Implemented specification of number of counterexamples in
  [`datajudge.WithinRequirement.add_varchar_regex_constraint`][datajudge.requirements.WithinRequirement.add_varchar_regex_constraint].
- Implemented in-database regex matching for some dialects via
  `computation_in_db` parameter in
  [`datajudge.WithinRequirement.add_varchar_regex_constraint`][datajudge.requirements.WithinRequirement.add_varchar_regex_constraint].
- Added support for BigQuery backends.

**Bug fix**

- Snowflake-sqlalchemy version 1.4.0 introduced an unexpected change in
  behaviour. This problem is resolved by pinning it to the previous
  version, 1.3.4.

## 1.1.1 - 2022.06.30

**New: SQL implementation for KS-test**

- The Kolgomorov Smirnov test is now implemented in pure SQL, shifting
  the computation to the database engine, improving performance
  tremendously.

## 1.1.0 - 2022.06.01

**New feature: Statistical Tests**

- Implemented a new constraint
  [`datajudge.constraints.stats.KolmogorovSmirnov2Sample`][datajudge.constraints.stats.KolmogorovSmirnov2Sample] for
  [`datajudge.BetweenRequirement`][datajudge.requirements.BetweenRequirement] that performs a [Kolmogorov Smirnov
  Test](https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test)
  between two data sources.

## 1.0.1 - 2022.05.24

**Bug fix:**

- The method `is_deprecated` of [`datajudge.Condition`][datajudge.Condition] was called
  despite not existing.
