"""
A collection of pre-define APIs to help users partition collection data, like list, array, tuple
"""

from typing import Iterable, Tuple

from parfun.partition.object import PartitionGenerator, PartitionType


def list_by_chunk(*iterables: Iterable[PartitionType]) -> PartitionGenerator[Tuple[Iterable[PartitionType], ...]]:
    """
    Partition one or multiple iterables by chunks of identical sizes.

    .. code:: python

        ls_1 = [1, 2, 3, 4]
        ls_2 = [1, 4, 9, 16]

        with_partition_size(list_by_chunk, ls_1, ls_2, partition_size=2))
        # [((1, 2), (1, 4)), ((3, 4), (9, 16))]

    """

    chunk_size = yield None

    i = 0
    partition = []

    for tuple_item in zip(*iterables):
        if i < chunk_size:
            partition.append(tuple_item)
            i += 1

        if i == chunk_size:
            chunk_size = yield chunk_size, tuple(zip(*partition))
            i = 0
            partition = []

    if partition:
        yield len(partition), tuple(zip(*partition))
