import pytest

from datajudge.utils import format_difference


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
