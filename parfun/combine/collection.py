"""
A collection of pre-define APIs to help users combine collection data, like list, array, tuple
"""

import logging
from itertools import chain, tee
from typing import Iterable, List, Tuple, TypeVar

ListValue = TypeVar("ListValue")


def list_concat(values: Iterable[List[ListValue]]) -> List[ListValue]:
    """
    Chains a collection of lists in a single list.

    .. code:: python

        list_concat([[1,2], [3], [4, 5]]) # [1, 2, 3, 4, 5]

    """
    return list(chain.from_iterable(values))


def lists_concat(values: Iterable[List[ListValue]]) -> List[ListValue]:
    logging.warning(
        f"`{lists_concat.__name__}` will be removed in a future version, use `{list_concat.__name__}` instead."
    )

    return list_concat(values)


def concat_lists(values: Iterable[List[ListValue]]) -> List[ListValue]:
    logging.warning(
        f"`{concat_lists.__name__}` will be removed in a future version, use `{list_concat.__name__}` instead."
    )

    return list_concat(values)


def unzip(iterable: Iterable[Tuple]) -> Tuple[Iterable]:
    """
    Opposite of zip().

    .. code:: python

        ls_1 = [1, 2, 3]
        ls_2 = [2, 4, 6]

        zipped = zip(ls_1, ls_2) # [(1, 2), (2, 4), (3, 6)]

        ls_1_out, ls_2_out = unzip(zipped)
        print(ls_1_out) # [1, 2, 3]
        print(ls_2_out) # [2, 4, 6]

    """

    logging.warning(f"`{unzip.__name__}` will be removed in a future version.")

    # Fetches the first item to deduce the number of nested values.
    it = iter(iterable)
    try:
        first_values = next(it)
        n_values = len(first_values)
    except StopIteration:
        return ()

    def tupled_generator():
        yield first_values
        while True:
            try:
                yield next(it)
            except StopIteration:
                return

    teed_iterators = tee(tupled_generator(), n_values)

    # Captures the i variable as an function argument, as variables are captured by reference in Python's closures.
    def map_function(i_local):
        return lambda v: v[i_local]

    return tuple(map(map_function(i), gen) for i, gen in enumerate(teed_iterators))
