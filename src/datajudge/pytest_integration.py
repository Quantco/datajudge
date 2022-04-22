from typing import Iterable

import pytest

from .constraints.base import Constraint
from .requirements import Requirement


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
    def test_constraint(constraint, datajudge_engine):
        test_result = constraint.test(datajudge_engine)
        assert test_result.outcome, test_result.failure_message

    return test_constraint
