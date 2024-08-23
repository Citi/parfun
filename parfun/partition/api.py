from collections import OrderedDict
from itertools import chain
from typing import Callable, Tuple, Union

from parfun.kernel.function_signature import NamedArguments
from parfun.partition.object import PartitionFunction, PartitionGenerator
from parfun.partition.primitives import partition_map, partition_zip


def per_argument(
    **partition_arg_with: PartitionFunction,
) -> Callable[[NamedArguments], Tuple[NamedArguments, PartitionGenerator[NamedArguments]]]:
    """
    Applies multiple partitioning functions simultaneously on different function arguments, similarly to Python's
    :py:func:`zip`.

    .. code:: python

        @parfun(
            split=per_argument(
                df=df_by_row,
                xs=list_by_chunk,
            )
        )
        def func(df: pd.DataFrame, xs: List, constant: int):
            ...

    """

    partition_arg_names = set(partition_arg_with.keys())

    def partitioning_function(named_args: NamedArguments) -> Tuple[NamedArguments, PartitionGenerator[NamedArguments]]:
        # Applies all partition functions simultaneously using `partition_zip()`, and then rebuilds the `NamedArguments`
        # object with the partitioned values.

        partitioned_args, non_partitioned_args = named_args.split(partition_arg_names)

        def reassign_partitioned_arguments(*partitioned_values) -> NamedArguments:
            changes = dict(zip(partition_arg_names, [v[0] for v in partitioned_values]))
            return partitioned_args.reassigned(**changes)

        partitioned_arg_generators = [
            partition_arg_with[arg_name](partitioned_args[arg_name]) for arg_name in partition_arg_names
        ]

        zipped = partition_zip(*partitioned_arg_generators)

        generator = partition_map(reassign_partitioned_arguments, zipped)  # type: ignore[type-var]

        return non_partitioned_args, generator

    return partitioning_function


def multiple_arguments(
    partition_on: Union[Tuple[str, ...], str], partition_with: PartitionFunction
) -> Callable[[NamedArguments], Tuple[NamedArguments, PartitionGenerator[NamedArguments]]]:
    """
    Applies a single partitioning function to multiple arguments.

    .. code:: python

        @parfun(
            split=multiple_arguments(
                ("df_1", "df_2"),
                df_by_group(by=["year", "month"]),
            )
        )
        def func(df_1: pd.DataFrame, df_2: pd.DataFrame, constant: int):
            ...

    """

    if isinstance(partition_on, str):
        partition_on = (partition_on,)

    if not isinstance(partition_on, tuple) or not all(isinstance(i, str) for i in partition_on):
        raise ValueError(f"`partition_on` must be str or tuple of string, but got: {partition_on}.")

    if len(partition_on) == 0:
        raise ValueError("empty `partition_on` value.")

    def partitioning_function(named_args: NamedArguments) -> Tuple[NamedArguments, PartitionGenerator]:
        # Applies the partitioning function to the selected parameters, and then rebuilds the `NamedArguments` object
        # with these partitioned values.

        partitioned_args, non_partitioned_args = named_args.split(set(partition_on))

        arg_values = [partitioned_args[a] for a in partition_on]

        generator = partition_map(
            lambda *partitioned_values: partitioned_args.reassigned(**dict(zip(partition_on, partitioned_values))),
            partition_with(*arg_values),
        )  # type: ignore[type-var]

        return non_partitioned_args, generator

    return partitioning_function


def all_arguments(
    partition_with: PartitionFunction,
) -> Callable[[NamedArguments], Tuple[NamedArguments, PartitionGenerator[NamedArguments]]]:
    """
    Applies a single partitioning function to all arguments.

    .. code:: python

        @parfun(
            split=all_arguments(df_by_group(by=["year", "month"])
        )
        def func(df_1: pd.DataFrame, df_2: pd.DataFrame):
            ...

    """

    def partitioning_function(named_args: NamedArguments) -> Tuple[NamedArguments, PartitionGenerator]:
        # Applies the partition function to the named positional parameters first, then keyword, then variable args,
        # then rebuilds the partitioned NamedArgument object in the same order.

        def reassign_all_arguments(*partitioned_values) -> NamedArguments:
            n_args = len(named_args.args)
            n_kwargs = len(named_args.kwargs)

            args = OrderedDict(zip(named_args.args.keys(), partitioned_values[:n_args]))
            kwargs = dict(zip(named_args.kwargs.keys(), partitioned_values[n_args : n_args + n_kwargs]))
            var_args = partitioned_values[n_args + n_kwargs :]

            return NamedArguments(args=args, kwargs=kwargs, var_args=var_args)

        arg_values = chain(named_args.args.values(), named_args.kwargs.values(), named_args.var_args)

        return NamedArguments(), partition_map(reassign_all_arguments, partition_with(*arg_values))

    return partitioning_function
