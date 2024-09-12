"""
A collection of pre-define APIs to help users combine collection data, like list, array, tuple
"""

from itertools import chain
from typing import Iterable, List, TypeVar

ListValue = TypeVar("ListValue")


def list_concat(values: Iterable[List[ListValue]]) -> List[ListValue]:
    """
    Chains a collection of lists in a single list.

    .. code:: python

        list_concat([[1,2], [3], [4, 5]]) # [1, 2, 3, 4, 5]

    """
    return list(chain.from_iterable(values))
