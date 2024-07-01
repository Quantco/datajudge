import pytest

from datajudge.utils import (
    format_difference,
    output_processor_limit,
    output_processor_sort,
    sort_tuple_none_aware,
)


@pytest.mark.parametrize(
    "n1, n2",
    [
        (123, 123.0),
        (122593859432347, 122593859432347 // 1000),  # one group less
        (1.2, 1234567),
        (1.2, 1.3),
    ],
)
@pytest.mark.xfail
def test_print_diff_color(n1, n2):
    format_n1, format_n2 = format_difference(n1, n2)

    assert True, f"{format_n1} vs {format_n2}"


@pytest.mark.parametrize(
    "n1, n2, sep_decimal, expected_n1, expected_n2",
    [
        (123, 123.0, False, "123", "123[numDiff].0[/numDiff]"),
        (
            122593859432,
            122593859432347,
            False,
            "122593859432",
            "122593859432[numDiff]347[/numDiff]",
        ),
        (
            122593859432,
            122593859432347,
            True,
            "122,593,859,432",
            "122,593,859,432[numDiff],347[/numDiff]",
        ),
        (0, 0, False, "0", "0"),
        (1, 2, False, "[numDiff]1[/numDiff]", "[numDiff]2[/numDiff]"),
        (
            123456789,
            987654321,
            False,
            "[numDiff]123456789[/numDiff]",
            "[numDiff]987654321[/numDiff]",
        ),
    ],
)
def test_diff_color(n1, n2, sep_decimal, expected_n1, expected_n2):
    assert format_difference(n1, n2, decimal_separator=sep_decimal) == (
        expected_n1,
        expected_n2,
    )


@pytest.mark.parametrize(
    "input_main, input_counts, output_main, output_counts",
    [
        (
            [5, None, -2, 42, 1337, 42, 42, -2, None, None],
            None,
            [-2, -2, None, None, None, 5, 42, 42, 42, 1337],
            None,
        ),
        (
            {5, None, -2, 42, 1337, 42, 42, -2, None, None},
            None,
            [-2, None, 5, 42, 1337],
            None,
        ),
        (
            [5, None, -2, 42, 1337, 42, 42, -2, None, None],
            [0, 42, 1, 3, 1, 2, 2, 0, 0, -99],
            [None, 42, 42, 42, -2, 1337, -2, None, 5, None],
            [42, 3, 2, 2, 1, 1, 0, 0, 0, -99],
        ),
        (
            [(5, 5), (1, None), (1, -2), (3, 42), (4, 1337)],
            None,
            [(1, -2), (1, None), (3, 42), (4, 1337), (5, 5)],
            None,
        ),
        (
            [(5, 5), (1, None), (1, -2), (3, 42), (4, 1337)],
            [0, 0, 0, 0, 1],
            [(4, 1337), (1, -2), (1, None), (3, 42), (5, 5)],
            [1, 0, 0, 0, 0],
        ),
        (
            [
                [5, 5],
                [1, 5],
                [1, -2],
                [3, 42],
                [4, 1337],
            ],
            None,
            [[1, -2], [1, 5], [3, 42], [4, 1337], [5, 5]],
            None,
        ),
    ],
)
def test_output_processor_sort(input_main, input_counts, output_main, output_counts):
    input_main_copy = input_main.copy()
    input_counts_copy = input_counts.copy() if input_counts is not None else None
    assert output_processor_sort(input_main, input_counts) == (
        output_main,
        output_counts,
    )
    assert input_main == input_main_copy
    assert input_counts == input_counts_copy


@pytest.mark.parametrize(
    "input_main, input_counts, output_main, output_counts, error",
    [
        (
            [
                [5, 5],
                [1, None],
                [1, -2],
                [3, 42],
                [4, 1337],
            ],
            None,
            None,
            None,
            TypeError,
        ),
        (
            [5, None, -2, 42, 1337, 42, 42, -2, None, None],
            [0, 42],
            None,
            None,
            ValueError,
        ),
    ],
)
def test_output_processor_sort_error(
    input_main, input_counts, output_main, output_counts, error
):
    with pytest.raises(error):
        output_processor_sort(input_main, input_counts)


def test_output_processor_limit_defaults():
    input_main = list(range(12345))
    input_counts = None

    input_main_copy = input_main.copy()
    input_counts_copy = None
    assert output_processor_limit(input_main, input_counts) == (
        list(range(100))
        + ["<SHORTENED OUTPUT, displaying the first 100 / 12345 elements above>"],
        None,
    )
    assert input_main == input_main_copy  # verify inputs are not modified
    assert input_counts == input_counts_copy


def test_output_processor_limit_custom():
    input_main = list(range(12345))
    input_counts = None

    input_main_copy = input_main.copy()
    input_counts_copy = None
    assert output_processor_limit(input_main, input_counts, limit=42) == (
        list(range(42))
        + ["<SHORTENED OUTPUT, displaying the first 42 / 12345 elements above>"],
        None,
    )
    assert input_main == input_main_copy  # verify inputs are not modified
    assert input_counts == input_counts_copy


def test_output_processor_limit_withcounts():
    input_main = list(range(12345))
    input_counts = list(range(1, 12345 + 1))

    input_main_copy = input_main.copy()
    input_counts_copy = input_counts.copy() if input_counts is not None else None
    assert output_processor_limit(input_main, input_counts, limit=42) == (
        list(range(42))
        + ["<SHORTENED OUTPUT, displaying the first 42 / 12345 elements above>"],
        list(range(1, 42 + 1))
        + ["<SHORTENED OUTPUT, displaying the first 42 / 12345 counts above>"],
    )
    assert input_main == input_main_copy  # verify inputs are not modified
    assert input_counts == input_counts_copy


class CustomObject:
    def __init__(self, value=42):
        self.value = value

    def __eq__(self, other):
        return self.value == other.value

    def __lt__(self, other):
        return self.value < other.value

    def __repr__(self):
        return f"CustomObject({self.value})"


@pytest.mark.parametrize(
    "input_main, output_main",
    [
        (
            [
                (5, -3, 42),
                (None, None, None),
                (3, 5, 42),
                (None, None, None),
                (3, 5, 42),
                (3, 5, -5),
                (-3, 5, 42),
                (None, None, -1),
                (0, 0, -1),  # this must occur inbetween the (None, None, -1) tuples
                # since sorted(...) is stable
                (0, 0, -2),
                (0, 0, 2),
                (None, None, -1),
                (None, 3, 42),
            ],
            [
                (-3, 5, 42),
                (0, 0, -2),
                (None, None, -1),
                (0, 0, -1),
                (None, None, -1),
                (None, None, None),
                (None, None, None),
                (0, 0, 2),
                (None, 3, 42),
                (3, 5, -5),
                (3, 5, 42),
                (3, 5, 42),
                (5, -3, 42),
            ],
        ),
        (
            [
                (5, 3.14, None, None, None),
                (None, None, "abc", CustomObject(13), None),
                (-3, -3.14, "äöü", CustomObject(1337), None),
            ],
            [
                (
                    -3,
                    -3.14,
                    "äöü",
                    CustomObject(1337),
                    None,
                ),
                (
                    None,
                    None,
                    "abc",
                    CustomObject(13),
                    None,
                ),
                (
                    5,
                    3.14,
                    None,
                    None,
                    None,
                ),
            ],
        ),
        (
            [(3.14,), (None,), (-1,)],
            [(-1,), (None,), (3.14,)],
        ),
        (
            [(None,), ("ÄÖÜ",), ("abc",)],
            [(None,), ("abc",), ("ÄÖÜ",)],
        ),
        (
            [(None,), (CustomObject(13),), (CustomObject(1337),)],
            [(CustomObject(13),), (None,), (CustomObject(1337),)],
        ),
        (
            [(None, 5), (None, -2), (None, None)],
            [(None, -2), (None, None), (None, 5)],
        ),
    ],
)
def test_sort_tuple_none_aware(input_main, output_main):
    input_main_copy = input_main.copy()
    assert sort_tuple_none_aware(input_main) == output_main
    assert input_main == input_main_copy

    assert sort_tuple_none_aware(input_main, ascending=False) == output_main[::-1]
    assert input_main == input_main_copy


@pytest.mark.parametrize(
    "input_main, output_main, error",
    [
        (
            [
                (5, -3, 42),
                [None, None, None],
            ],
            None,
            ValueError,
        ),
        (
            [
                (5, -3, 42),
                (None, None),
            ],
            None,
            ValueError,
        ),
    ],
)
def test_sort_tuple_none_error(input_main, output_main, error):
    with pytest.raises(error):
        sort_tuple_none_aware(input_main)
