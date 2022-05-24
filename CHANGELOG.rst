.. Versioning follows semantic versioning, see also
   https://semver.org/spec/v2.0.0.html. The most important bits are:
   * Update the major if you break the public API
   * Update the minor if you add new functionality
   * Update the patch if you fixed a bug

Changelog
=========

1.0.1 - 2022.05.24
------------------

**Bug fix:**

- The method :meth:`is_deprecated` of :class:`~datajudge.Condition` was called despite not existing.

