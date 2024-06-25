from typing import Iterable

import pytest
from packaging.version import Version

from .constraints.base import Constraint
from .db_access import apply_patches
from .formatter import AnsiColorFormatter, Formatter
from .requirements import Requirement


def get_formatter(pytestconfig):
    color = pytestconfig.getoption("color")

    if color == "yes" or color == "auto":
        # before pytest-html < 4.0.0
        # styling in assertion messages was not formatted correctly
        # so in this case we use the default formatter
        # in pytest-html >= 4.0.0 the styling gets stripped or
        # translated to ANSI codes, depending if ansi2html is installed
        if pytestconfig.getoption("htmlpath", False):
            try:
                import pytest_html

                if Version(pytest_html.__version__).major >= 4:
                    return AnsiColorFormatter()
            finally:
                return Formatter()

        return AnsiColorFormatter()
    else:
        return Formatter()


def collect_data_tests(requirements: Iterable[Requirement]):
    """Make a Pytest test case that checks all `requirements`.

    Returns a function named `test_constraint` that is parametrized over all
    constraints in `requirements`. The function requires a `datajudge_engine`
    fixture that is a SQLAlchemy engine to be available.
    """
    all_constraints = [
        constraint for requirement in requirements for constraint in requirement
    ]

    @pytest.mark.parametrize(
        "constraint", all_constraints, ids=Constraint.get_description
    )
    def test_constraint(constraint, datajudge_engine, pytestconfig):
        # apply patches that fix sqlalchemy issues
        formatter = get_formatter(pytestconfig)
        apply_patches(datajudge_engine)
        test_result = constraint.test(datajudge_engine)
        assert test_result.outcome, test_result.formatted_failure_message(formatter)

    return test_constraint
