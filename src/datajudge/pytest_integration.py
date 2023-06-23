from typing import Iterable

import pytest

from .constraints.base import Constraint
from .db_access import apply_patches
from .formatter import AnsiColorFormatter, Formatter, HtmlFormatter
from .requirements import Requirement


@pytest.fixture(scope="session")
def formatter(pytestconfig):
    color = pytestconfig.getoption("color")
    is_html = pytestconfig.getoption("htmlpath") is not None

    if not is_html and (color == "yes" or color == "auto"):
        return AnsiColorFormatter()
    elif is_html and (color == "yes" or color == "auto"):
        return HtmlFormatter()
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
    def test_constraint(constraint, datajudge_engine, formatter):
        # apply patches that fix sqlalchemy issues
        apply_patches(datajudge_engine)
        test_result = constraint.test(datajudge_engine)
        assert test_result.outcome, test_result.formatted_failure_message(formatter)

    return test_constraint
