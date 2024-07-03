import inspect

from datajudge import WithinRequirement
from datajudge.requirements import BetweenRequirement


def test_all_requirements_new_kwargs():
    prices_req = WithinRequirement.from_table(
        db_name="example", schema_name="schema", table_name="prices"
    )

    prices_req_between = BetweenRequirement.from_tables(
        "example",
        "schema1",
        "table1",
        "example",
        "schema2",
        "table2",
    )

    for req in [prices_req, prices_req_between]:
        for add_func_name in dir(req):
            if not add_func_name.startswith("add_"):
                continue
            add_func = getattr(req, add_func_name)
            sig = inspect.signature(add_func)
            assert "cache_size" in sig.parameters, (
                add_func,
                sig.parameters,
                sig,
            )
