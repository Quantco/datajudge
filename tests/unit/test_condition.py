import pytest

from datajudge.db_access import Condition


def test_creation():
    c = Condition(["col1", "col2"], ["1", "2"])
    assert c.operators == ["=", "="]


def test_equality():
    c1 = Condition(["col1", "col2"], ["1", "2"])
    c2 = Condition(["col1", "col2"], ["1", "2"])

    assert c1 == c2


def test_inequality():
    c1 = Condition(["col1", "col2"], ["1", "2"])
    c2 = Condition(["col1", "col3"], ["1", "3"])

    assert c1 != c2


@pytest.mark.parametrize("column", [None, [], ["col1"]])
@pytest.mark.parametrize("value", [None, []])
def test_raises_on_invalid_input(column, value):
    # Not adding the case of non empty `value` since when matched with a non empty `column` the test will fail
    # because it's indeed a correct combination. Due to the symmetry of `column` and `value` I deem this as
    # relatively safe.
    # Furthermore, in principle this tests check if the validation works as intended. In any case, due to the
    # type annotations, at least in IDEs trying to input these pathological corner-cases, will raise alarms.
    with pytest.raises(ValueError):
        Condition(column, value)


def test_atomic_str():
    c1_str = "col1 = 1"
    c1 = Condition(raw_string=c1_str)
    assert str(c1) == c1_str


def test_composite_str():
    c1_str = "col1 = 1"
    c2_str = "col2 > 2"
    c3_str = "col3 IS NOT NULL"
    c1 = Condition(raw_string=c1_str)
    c2 = Condition(raw_string=c2_str)
    c3 = Condition(raw_string=c3_str)
    c4 = Condition(conditions=[c1, c2, c3], reduction_operator="and")
    assert str(c4) == f"({c1_str}) and ({c2_str}) and ({c3_str})"


def test_nested_composite_str():
    c1_str = "col1 = 1"
    c2_str = "col2 > 2"
    c3_str = "col3 IS NOT NULL"
    c5_str = "col5 = 42"
    c1 = Condition(raw_string=c1_str)
    c2 = Condition(raw_string=c2_str)
    c3 = Condition(raw_string=c3_str)
    c4 = Condition(conditions=[c1, c2, c3], reduction_operator="and")
    c5 = Condition(raw_string=c5_str)
    c6 = Condition(conditions=[c4, c5], reduction_operator="or")
    assert str(c6) == f"(({c1_str}) and ({c2_str}) and ({c3_str})) or ({c5_str})"


def test_condition_missing_both_arguments():
    with pytest.raises(ValueError):
        Condition()


@pytest.mark.parametrize("raw_string", [None, "col1 = 1"])
def test_condition_conditions_empty_list(raw_string):
    with pytest.raises(ValueError):
        Condition(raw_string=raw_string, conditions=[])


def test_condition_having_both_arguments():
    c1 = Condition(raw_string="col1 = 1")
    c2 = Condition(raw_string="col2 > 2")
    with pytest.raises(ValueError):
        Condition(raw_string="col3 IS NOT NULL", conditions=[c1, c2])


@pytest.mark.parametrize("reduction_operator", [None, "", "XOR", "AND"])
def test_condition_composite_inappropriate_reduction_operator(reduction_operator):
    c1 = Condition(raw_string="col1 = 1")
    c2 = Condition(raw_string="col2 > 2")
    with pytest.raises(ValueError):
        Condition(conditions=[c1, c2], reduction_operator=reduction_operator)
