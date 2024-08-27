from typing import Any, Callable, Dict, Sequence, Tuple, Union

from parfun.partition.object import PartitionFunction, PartitionGenerator
from parfun.partition.primitives import partition_flatmap, partition_map


def partition_nested(
    *columns_partitions: Tuple[Union[Tuple[str, ...], str], PartitionFunction[Tuple]]
) -> Callable[..., PartitionGenerator[Dict[str, Any]]]:
    """
    Creates a new partitioning function from a collection of nested partitioning functions that are individually applied
    to some of the input arguments.

    .. code: python

        # Applies `df_by_row` on `col_1` and `col_2`, then `df_by_group` on `col_3`, and finally
        # `df_by_row` on the previously partitioned `col_3`.
        partition_columns(
            (("col_1", "col_2"), df_by_row),
            ("col_3", df_by_group(by="year")),
            ("col_3", df_by_row),
        )

    """

    # Validates the input.

    if len(columns_partitions) < 1:
        raise ValueError("empty partition generator collection.")

    for columns, _ in columns_partitions:
        if not isinstance(columns, (str, tuple)):
            raise ValueError("column values should be either strings or tuples of strings.")

        if isinstance(columns, str):
            columns = (columns,)

    # Builds the generator from the nested-most partition using partition_flatmap calls.

    return _partition_nested_build_generator(columns_partitions)


def _partition_nested_build_generator(
    columns_partitions: Sequence[Tuple[Union[Tuple[str, ...], str], Callable[..., PartitionGenerator[Tuple]]]]
) -> Callable[..., PartitionGenerator[Dict[str, Any]]]:
    assert len(columns_partitions) >= 1

    current_arg_names, partition_function = columns_partitions[0]
    remaining_columns_partitions = columns_partitions[1:]

    if isinstance(current_arg_names, str):
        current_arg_names = (current_arg_names,)

    def generator(**kwargs) -> PartitionGenerator[Dict[str, Any]]:
        missing_args = [p for p in current_arg_names if p not in kwargs]
        if len(missing_args) > 0:
            missing_arg_str = ", ".join(missing_args)
            raise ValueError(f"missing partition argument(s): {missing_arg_str}")

        current_args = (kwargs[p] for p in current_arg_names)
        current_generator = partition_function(*current_args)

        if len(columns_partitions) > 1:
            return partition_flatmap(
                lambda *partitioned_values: _partition_nested_build_generator(remaining_columns_partitions)(
                    **_updated_partitioned_kwargs(kwargs, current_arg_names, partitioned_values)
                ),
                current_generator,
            )  # type: ignore[type-var, return-value]
        else:
            return partition_map(
                lambda *partitioned_values: _updated_partitioned_kwargs(kwargs, current_arg_names, partitioned_values),
                current_generator,
            )  # type: ignore[type-var, return-value]

    return generator


def _updated_partitioned_kwargs(
    kwargs: Dict[str, Any], arg_names: Tuple[str, ...], partitioned_values: Any
) -> Dict[str, Any]:
    return {**kwargs, **dict(zip(arg_names, partitioned_values))}
