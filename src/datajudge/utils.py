from typing import Tuple, Union

from colorama import just_fix_windows_console

just_fix_windows_console()


def _fmt_diff_part(s, d):
    return f"[numDiff]{s[d:]}[/numDiff]" if d < len(s) else ""


def format_difference(
    n1: Union[float, int], n2: Union[float, int], decimal_separator: bool = True
) -> Tuple[str, str]:
    """
    Given two numbers, n1 and n2, return a tuple of two strings,
    each representing one of the input numbers with the differing part highlighted.
    Highlighting is done using BBCode-like tags, which are replaced by the formatter.

    Examples:
        123, 123.0
        -> 123, 123[numDiff].0[/numDiff]
        122593859432, 122593859432347
        -> 122593859432, 122593859432[numDiff]347[/numDiff]

    Args:
    - n1: The first number to compare.
    - n2: The second number to compare.
    - decimal_separator: Whether to separate the decimal part of the numbers with commas.

    Returns:
    - A tuple of two strings, each representing one of the input numbers with the differing part highlighted.
    """
    if decimal_separator:
        s1, s2 = f"{n1:,}", f"{n2:,}"
    else:
        s1, s2 = str(n1), str(n2)

    min_len = min(len(s1), len(s2))
    diff_idx = next(
        (i for i in range(min_len) if s1[i] != s2[i]),
        min_len,
    )

    return (
        f"{s1[:diff_idx]}{_fmt_diff_part(s1, diff_idx)}",
        f"{s2[:diff_idx]}{_fmt_diff_part(s2, diff_idx)}",
    )
