from typing import Callable, Generator, Optional, Tuple, Union

from parfun.object import PartitionType

SimplePartitionIterator = Generator[PartitionType, None, None]

SmartPartitionGenerator = Generator[Optional[Tuple[int, PartitionType]], int, None]

PartitionGenerator = Union[SimplePartitionIterator[PartitionType], SmartPartitionGenerator[PartitionType]]
"""
All partitioning functions must return a Python generator of this type.

There are two ways of writing a partitioning functions:

* Use regular Python generators (prefered) or iterators, returning partitioned values:

.. code:: python

    def partition_list_by_chunks(values: List): PartitionGenerator[List]:
        PARTITION_SIZE = len(values) / 100

        for begin in range(0, len(values), PARTITION_SIZE)):
            yield values[begin:begin + PARTITION_SIZE]


* Use partition size aware Python generators, or smart generators. These are more complex but more efficient. Partition
  size aware generators must get a suggested partition size through the return value of the ``yield`` statement, and
  yield partition sizes with its partitioned values:

.. code:: python

    def partition_list_by_chunks(values: List, constant: int) -> PartitionGenerator[Tuple[List, int]]:
        # A first empty call to `yield` is required to obtain the first requested partition size
        requested_partition_size = yield None

        begin = 0
        while begin < len(values):
            end = min(len(values), begin + requested_partition_size)

            partition_size = end - begin
            partition = (values[begin:end], a)

            # Yield the actual partition along its size, and obtain the requested size for the next partition.
            requested_partition_size = yield partition_size, partition

            begin = end

"""

PartitionFunction = Callable[..., PartitionGenerator[PartitionType]]
