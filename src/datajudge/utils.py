from typing import Collection, List, Optional, Protocol, Tuple, Union


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


class OutputProcessor(Protocol):
    def __call__(  # noqa: E704
        self,
        collection: Collection,
        counts: Optional[Collection] = None,
    ) -> Collection: ...


def output_processor_sort(
    collection: Collection, counts: Optional[Collection] = None
) -> Tuple[Collection, Optional[Collection]]:
    """
    Sorts a collection of tuple elements in descending order of their counts,
    and for ties, makes use of the ascending order of the elements themselves.

    If the first element is not instanceof tuple,
    each element will be transparently packaged into a 1-tuple for processing;
    this process is not visible to the caller.

    Handles ``None`` values as described in ``sort_tuple_none_aware``.
    """
    collection = list(collection)
    if not isinstance(collection[0], tuple):
        # package into a 1 tuple and pass into the method again
        packaged_list = [(elem,) for elem in collection]
        res_main, res_counts = output_processor_sort(packaged_list, counts)
        return [elem[0] for elem in res_main], res_counts

    if counts is None:
        return sort_tuple_none_aware(collection), counts

    if len(collection) != len(counts):
        raise ValueError("collection and counts must have the same length")

    if len(collection) <= 1:
        return collection, counts  # empty or 1 element lists are always sorted

    lst = sort_tuple_none_aware(
        [(-count, *elem) for count, elem in zip(counts, collection)]
    )
    return [elem[1:] for elem in lst], [-elem[0] for elem in lst]


def output_processor_limit(
    collection: Collection, counts: Optional[Collection] = None, limit: int = 100
) -> Tuple[Collection, Optional[Collection]]:
    """
    Limits the collection to the first ``limit`` elements.
    If the list was shortened,
    will add a ``limit+1``-th string element,
    informing the user of the truncation.
    The default limit of ``100`` can be adjusted using ``functools.partial``
    """
    collection = list(collection)

    ret_collection = collection[:limit]
    ret_counts = None if counts is None else list(counts)[:limit]
    if len(collection) > limit:
        ret_collection.append(
            f"<SHORTENED OUTPUT, displaying the first {limit} / {len(collection)} elements above>"
        )
        if ret_counts is not None:
            ret_counts.append(
                f"<SHORTENED OUTPUT, displaying the first {limit} / {len(collection)} counts above>"
            )

    return ret_collection, ret_counts


def filternull_element(values: List) -> List:
    return [value for value in values if value is not None]


def filternull_never(values: List) -> List:
    return values


def filternull_element_or_tuple_all(values: List) -> List:
    return [
        value
        for value in values
        if value is not None
        and not (isinstance(value, tuple) and all(x is None for x in value))
    ]


def filternull_element_or_tuple_any(values: List) -> List:
    return [
        value
        for value in values
        if value is not None
        and not (isinstance(value, tuple) and any(x is None for x in value))
    ]


def sort_tuple_none_aware(
    collection: Collection[Tuple], ascending=True
) -> Collection[Tuple]:
    """
    Stable sort of a collection of tuples.
    Each tuple in the collection must have the same length,
    since they are treated as rows in a table,
    with ``elem[0]`` being the first column,
    ``elem[1]`` the second, etc. for each ``elem`` in ``collection``.
    For sorting, ``None`` is considered the same as the default value of the respective column's type.

    ints and floats ``int()`` and ``float()`` yield ``0`` and ``0.0`` respectively; for strings, ``str()`` yields ``''``.
    The constructor is determined by calling ``type`` on the first non-``None`` element of the respective column.

    Validates that all elements in collection are tuples and that all tuples have the same length.
    """
    lst = list(collection)

    if len(lst) <= 1:
        return lst  # empty or 1 element lists are always sorted

    if not all(isinstance(elem, tuple) and len(elem) == len(lst[0]) for elem in lst):
        raise ValueError("all elements must be tuples and have the same length")

    dtypes_each_tupleelement: List[Optional[type]] = [None] * len(lst[0])
    for dtypeidx in range(len(dtypes_each_tupleelement)):
        for elem in lst:
            if elem[dtypeidx] is not None:
                dtypes_each_tupleelement[dtypeidx] = type(elem[dtypeidx])
                break
        else:
            # if all entries are None, just use a constant int() == 0
            dtypes_each_tupleelement[dtypeidx] = int

    def replace_None_with_default(elem):  # noqa: N802
        return tuple(
            ((dtype() if dtype else None) if subelem is None else subelem)
            for dtype, subelem in zip(dtypes_each_tupleelement, elem)
        )

    return sorted(
        lst, key=lambda elem: replace_None_with_default(elem), reverse=not ascending
    )
