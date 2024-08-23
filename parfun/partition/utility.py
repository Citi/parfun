from typing import Callable, Generator, Optional, Union, cast

from parfun.object import PartitionType
from parfun.partition.object import PartitionGenerator, SmartPartitionGenerator, SimplePartitionIterator


def with_partition_size(
    generator: PartitionGenerator[PartitionType], partition_size: Union[int, Callable[[], int]] = 1
) -> Generator[PartitionType, None, None]:
    """
    Runs a partitioning generator without requiring the partition size estimator.

    This function uses the provided partition size input to feed the partitioning generator through Python's
    :py:meth:`generator.send` method, simulating the parallel function's behaviour.

    .. code:: python

        # Runs the `df_by_row` partitioning function with a random partition size generator.
        with_partition_size(
            df_by_row(df_1, df_2),
            partition_size=lambda: random.randint(1, 10)
        )

    :param partitions_with: the partitioning generator to execute
    :param partition_size: a constant partition size, or a function generating partition sizes
    """

    try:
        first_value = cast(Optional[PartitionType], next(generator))

        if first_value is not None:
            # This is a regular generator
            simple_generator = cast(SimplePartitionIterator[PartitionType], generator)

            yield first_value
            yield from simple_generator
        else:
            smart_generator = cast(SmartPartitionGenerator[PartitionType], generator)

            while True:
                if isinstance(partition_size, int):
                    current_partition_size = partition_size
                else:
                    assert callable(partition_size)
                    current_partition_size = partition_size()

                value = smart_generator.send(current_partition_size)

                if value is None or len(value) != 2:
                    raise ValueError("partition generator should yield a partition with its size.")

                yield value[1]
    except StopIteration:
        return
