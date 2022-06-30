.. Versioning follows semantic versioning, see also
   https://semver.org/spec/v2.0.0.html. The most important bits are:
   * Update the major if you break the public API
   * Update the minor if you add new functionality
   * Update the patch if you fixed a bug

Changelog
=========

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

