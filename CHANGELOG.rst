.. Versioning follows semantic versioning, see also
   https://semver.org/spec/v2.0.0.html. The most important bits are:
   * Update the major if you break the public API
   * Update the minor if you add new functionality
   * Update the patch if you fixed a bug

Changelog
=========

1.7.0 - 2023.05.11
------------------

**New features**

- Implement :meth:`datajudge.WithinRequirement.add_value_distribution_constraint`.
- Extended :meth:`datajudge.WithinRequirement.add_column_type_constraint` to support column type specification using string format, backend-specific SQLAlchemy types, and SQLAlchemy's generic types.
- Implement :meth:`datajudge.WithinRequirement.add_numeric_no_gap_constraint`, :meth:`datajudge.WithinRequirement.add_numeric_no_overlap_constraint`,

1.6.0 - 2022.04.12
------------------

**Other changes**

- Ensure compatibility with ``sqlalchemy`` >= 2.0.


1.5.0 - 2022.03.14
------------------

**New features**

- Implement :meth:`datajudge.BetweenRequirement.add_max_null_fraction_constraint` and
  :meth:`datajudge.WithinRequirement.add_max_null_fraction_constraint`.
- Implement :meth:`datajudge.BetweenRequirement.add_numeric_percentile_constraint` and
  :meth:`datajudge.WithinRequirement.add_numeric_percentile_constraint`.


1.4.0 - 2022.02.24
------------------

**New features**

- Add partial and experimental support for db2 as a backend.


1.3.0 - 2022.01.17
------------------

**New features**

- Implement :meth:`~datajudge.BetweenRequirement.add_column_type_constraint`. Previously, only the ``WithinRequirement`` method existed.
- Implemented an option ``infer_pk`` to automatically retrieve and primary key definition as part of :meth:`datajudge.WithinRequirement.add_uniqueness_constraint`.
- Added a ``name`` parameter to all ``add_x_constraint`` methods of ``WithinRequirement`` and ``BetweenRequirement``. This will give pytest test a custom name.
- Added preliminary support for Impala.

**Other changes**

- Improve assertion error for :meth:`~datajudge.WithinRequirement.add_row_matching_equality_constraint`.


1.2.0 - 2022.10.21
------------------

**New features**

- Implemented specification of number of counterexamples in :meth:`~datajudge.WithinRequirement.add_varchar_regex_constraint`.
- Implemented in-database regex matching for some dialects via ``computation_in_db`` parameter in :meth:`~datajudge.WithinRequirement.add_varchar_regex_constraint`.
- Added support for BigQuery backends.

**Bug fix**

- Snowflake-sqlalchemy version 1.4.0 introduced an unexpected change in behaviour. This problem is resolved by pinning it to the previous version, 1.3.4.


1.1.1 - 2022.06.30
------------------

**New: SQL implementation for KS-test**

- The Kolgomorov Smirnov test is now implemented in pure SQL, shifting the computation to the database engine, improving performance tremendously.

1.1.0 - 2022.06.01
------------------

**New feature: Statistical Tests**

- Implemented a new constraint :class:`~datajudge.constraints.stats.KolmogorovSmirnov2Sample` for :class:`~datajudge.BetweenRequirement` that performs a `Kolmogorov Smirnov Test <https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test>`_ between two data sources.

1.0.1 - 2022.05.24
------------------

**Bug fix:**

- The method :meth:`is_deprecated` of :class:`~datajudge.Condition` was called despite not existing.

