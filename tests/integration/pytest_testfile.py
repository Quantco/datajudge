import pytest
import sqlalchemy as sa

from datajudge.constraints.base import Constraint, TestResult
from datajudge.db_access import DataReference
from datajudge.pytest_integration import collect_data_tests
from datajudge.requirements import WithinRequirement

example_req = WithinRequirement.from_table(
    db_name="main", schema_name="main", table_name="companies"
)

example_req.add_column_existence_constraint(columns=["name"])


class StylizedTestConstraint(Constraint):
    def test(self, engine: sa.engine.Engine) -> TestResult:
        return TestResult.failure(
            "This is a [numDiff]stylized[/numDiff] failure message"
        )


@pytest.fixture()
def datajudge_engine():
    eng = sa.create_engine("sqlite://")
    eng.connect().execute(
        sa.text(
            "CREATE TABLE companies (id INTEGER PRIMARY KEY, name TEXT, num_employees INTEGER)"
        )
    )
    return eng


ref = DataReference(
    data_source=example_req.data_source,
    columns=["example"],
    condition=None,
)

example_req.append(StylizedTestConstraint(ref, ref_value=object()))

tests = collect_data_tests([example_req])
