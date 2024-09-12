"""
A decorator that helps users run their functions in parallel.
"""

import importlib
from functools import wraps
from typing import Callable, Iterable, Optional, Tuple, Union

from parfun.kernel.function_signature import NamedArguments
from parfun.kernel.parallel_function import ParallelFunction
from parfun.object import FunctionInputType, FunctionOutputType
from parfun.partition.object import PartitionGenerator
from parfun.partition_size_estimator.linear_regression_estimator import LinearRegessionEstimator
from parfun.partition_size_estimator.mixins import PartitionSizeEstimator


def parfun(
    split: Callable[[NamedArguments], Tuple[NamedArguments, PartitionGenerator[NamedArguments]]],
    combine_with: Callable[[Iterable[FunctionOutputType]], FunctionOutputType],
    initial_partition_size: Optional[Union[int, Callable[[FunctionInputType], int]]] = None,
    fixed_partition_size: Optional[Union[int, Callable[[FunctionInputType], int]]] = None,
    profile: bool = False,
    trace_export: Optional[str] = None,
    partition_size_estimator_factory: Callable[[], PartitionSizeEstimator] = LinearRegessionEstimator,
) -> Callable:
    """
    Returns a function decorator that automatically parallelizes a function.

    .. code:: python

        @parfun(
            split=per_argument(
                values=lists_by_chunk,
            ),
            combine_with=lists_concat
        )
        def multiply_by_constant(values: Iterable[int], constant: int):
            return [v * constant for v in values]

        # This would be functionally equivalent to running the function inside a single for loop:

        results = []
        for partition in lists_by_chunk(values):
            results.append(multiply_by_constant(partition, constant))

        return combine_with(results)

    :param split:
        Partition the data based on the provided partitioning function.

        See :py:mod:`~parfun.partition.api` for the list of predefined partitioning functions.

    :param combine_with: aggregates the results by running the function.
    :type combine_with: Callable
    :param initial_partition_size:
        Overrides the first estimate from the partition size estimator.

        If the value is a callable, the function will be provided with the input to be partitioned and shall return the
        initial partition size to use.

    :type initial_partition_size: int | Callable[[PartitionType], int] | None
    :param fixed_partition_size:
        Uses a constant partition size and do not run the partition size estimator.

        If the value is a callable, the function will be provided with the input to be partitioned and shall return the
        partition size to use.
    :type fixed_partition_size: int | Callable[[PartitionType], int] | None
    :param profile: if true, prints additional debugging information about the parallelization overhead.
    :type profile: bool
    :param trace_export: if defined, will export the execution time to the provided CSV file's path.
    :type trace_export: str
    :param partition_size_estimator_factory: the partition size estimator class to use
    :type partition_size_estimator_factory: Callable[[], PartitionSizeEstimator]

    :return: a decorated function
    :rtype: Callable
    """

    def decorator(function: Callable[[FunctionInputType], FunctionOutputType]):
        # init a ParallelFunction object to handle parallel computations automatically
        parallel_function = ParallelFunction(
            function=function,
            function_name=function.__name__,
            split=split,
            combine_with=combine_with,
            initial_partition_size=initial_partition_size,
            fixed_partition_size=fixed_partition_size,
            profile=profile,
            trace_export=trace_export,
            partition_size_estimator_factory=partition_size_estimator_factory,
        )

        @wraps(function)
        def wrapped(*args, **kwargs):
            # Remark: we cannot decorate `parallel_function` with `wraps` directly as it's not a regular function.
            return parallel_function(*args, **kwargs)

        # Renames the original function as "_{function_name}_sequential" and adds it to the same module.
        # This is required as Pickle requires all serialized functions to be accessible from a qualified module, which
        # will not be the case for the original function as it gets overridden by the decorator.
        module = importlib.import_module(function.__module__)
        name = f"_{function.__name__}_sequential"
        parent_qualname, parent_separator, old_qualname = function.__qualname__.rpartition(".")
        qualname = f"{parent_qualname}{parent_separator}_{old_qualname}_sequential"
        setattr(module, name, function)
        getattr(module, name).__name__ = name
        getattr(module, name).__qualname__ = qualname

        return wrapped

    return decorator
