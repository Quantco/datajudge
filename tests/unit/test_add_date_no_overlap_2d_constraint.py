import pytest
import sqlalchemy as sa

from datajudge.pytest_integration import collect_data_tests
from datajudge.requirements import WithinRequirement


@pytest.fixture(scope="module")
def datajudge_engine():
    eng = sa.create_engine("sqlite://")
    conn = eng.connect()

    statements = [
        """CREATE TABLE IF NOT EXISTS valid_dates (
            id INTEGER,
            start1 DATE,
            end1 DATE,
            start2 DATE,
            end2 DATE
        )""",
        """CREATE TABLE IF NOT EXISTS invalid_dates (
            id INTEGER,
            start1 DATE,
            end1 DATE,
            start2 DATE,
            end2 DATE
        )""",
        """INSERT INTO valid_dates (id, start1, end1, start2, end2) VALUES
            (1, '2024-01-01', '2024-01-02', '2024-02-01', '2024-02-02'),
            (1, '2024-01-03', '2024-01-04', '2024-02-03', '2024-02-04')""",
        """INSERT INTO invalid_dates (id, start1, end1, start2, end2) VALUES
            (1, '2024-01-01', '2024-01-03', '2024-02-01', '2024-02-03'),
            (1, '2024-01-01', '2024-01-03', '2024-02-01', '2024-02-03')""",
    ]

    # Execute statements one at a time
    for statement in statements:
        conn.execute(sa.text(statement))
    conn.commit()

    return eng


requirements = []
for table_name in ["valid_dates", "invalid_dates"]:
    requirement = WithinRequirement.from_table("main", "main", table_name)
    requirement.add_date_no_overlap_2d_constraint(
        key_columns=["id"],
        start_column1="start1",
        end_column1="end1",
        start_column2="start2",
        end_column2="end2",
        end_included=False,
    )
    requirements.append(requirement)

test_constraints = collect_data_tests(requirements)
