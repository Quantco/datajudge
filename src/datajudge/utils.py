from typing import Tuple, Union

from colorama import just_fix_windows_console

just_fix_windows_console()


def diff_color(n1: Union[float, int], n2: Union[float, int]) -> Tuple[str, str]:
    """
    Given two numbers, returns a tuple of strings where the numbers are colored based on their difference.
    Examples:
        123, 123.0      -> 123, 123[.0] # the part in squared brackets is colored
        122593859432, 122593859432347 -> 122,593,859,432 and 122,593,859,432[,347]
    """
    s1, s2 = f"{n1:,}", f"{n2:,}"

    min_len = min(len(s1), len(s2))
    dif_idx = next(
        (i for i in range(min_len) if s1[i] != s2[i]),
        min_len,
    )

    return (
        f"[numDiff]{s1[:dif_idx]}[/numDiff]{s1[dif_idx:]}",
        f"[numDiff]{s2[:dif_idx]}[/numDiff]{s2[dif_idx:]}",
    )
