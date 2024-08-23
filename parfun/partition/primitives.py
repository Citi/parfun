from typing import Callable, Optional, Sequence, Tuple, TypeVar, cast

from parfun.partition.object import PartitionGenerator, PartitionType, SmartPartitionGenerator, SimplePartitionIterator

InputPartitionType = TypeVar("InputPartitionType", bound=Tuple)
OutputPartitionType = TypeVar("OutputPartitionType", bound=Tuple)


def partition_map(
    func: Callable[..., OutputPartitionType], generator: PartitionGenerator[InputPartitionType]
) -> PartitionGenerator[OutputPartitionType]:
    """
    Same as Python's built-in ``map()``, but works on partition generators.

    .. code:: python

        partition_map(
            lambda partition_df: partition_df * 2,
            df_by_row(df)
        )

    If the generator is a regular Python generator, the function returns a regular generator. Otherwise, it returns a
    smart generator.
    """

    try:
        first_value = cast(Optional[InputPartitionType], next(generator))

        if first_value is not None:
            # This is a regular generator
            simple_generator = cast(SimplePartitionIterator[InputPartitionType], generator)

            yield func(*first_value)

            while True:
                yield func(*next(simple_generator))
        else:
            smart_generator = cast(SmartPartitionGenerator[InputPartitionType], generator)

            requested_partition_size = yield None

            while True:
                value = smart_generator.send(requested_partition_size)
                _validate_smart_partition_value(value)

                partition_size, partition = value

                requested_partition_size = yield partition_size, func(*partition)
    except StopIteration:
        return


def partition_unit(partition_size: int, partition: PartitionType) -> PartitionGenerator[PartitionType]:
    """Creates a generator returning a single partition."""

    _ = yield None
    yield partition_size, partition


def partition_flatmap(
    func: Callable[[InputPartitionType], PartitionGenerator[OutputPartitionType]],
    generator: PartitionGenerator[InputPartitionType],
) -> PartitionGenerator[OutputPartitionType]:
    """
    Allows allows the nesting of ``PartitionGenerator``s, similarly to nested for loops:

    .. code:: python

        partition_flatmap(
            lambda partition_df: df_by_row(*partition_df),
            df_by_group(by="year")(df)
        )

    Returns a regular Python generator iff the parent and iterated generators are regular Python generators. Otherwise,
    it returns a smart generator.
    """

    try:
        first_value = cast(Optional[InputPartitionType], next(generator))
    except StopIteration:
        return

    if first_value is not None:
        # The parent generator is a regular generator
        simple_generator = cast(SimplePartitionIterator[InputPartitionType], generator)
        yield from _partition_flatmap_regular_generator(func, first_value, simple_generator)
    else:
        smart_generator = cast(SmartPartitionGenerator[InputPartitionType], generator)
        yield from _partition_flatmap_smart_generator(func, smart_generator)


def _partition_flatmap_regular_generator(
    func: Callable[[InputPartitionType], PartitionGenerator[OutputPartitionType]],
    first_value: InputPartitionType,
    generator: SimplePartitionIterator[InputPartitionType],
) -> PartitionGenerator[OutputPartitionType]:
    """
    `partition_flatmap()` specialisation for parent generators that are regular Python generators.

    The function returns a smart generator iff the iterated function return smart generators, otherwise it returns a
    regular Python generator.
    """

    def iterate_nested_generator(
        nested_generator: PartitionGenerator[OutputPartitionType], requested_partition_size: Optional[int] = None
    ):
        try:
            first_value = cast(Optional[OutputPartitionType], next(nested_generator))

            if first_value is not None:
                # This is a regular generator
                nested_simple_generator = cast(SimplePartitionIterator[OutputPartitionType], nested_generator)

                if requested_partition_size is not None:
                    raise ValueError(
                        "`partition_flatmap()` doesn't support mixing smart and regular generators in applied function."
                    )

                yield first_value
                yield from nested_simple_generator
            else:
                nested_smart_generator = cast(SmartPartitionGenerator[OutputPartitionType], nested_generator)

                if requested_partition_size is None:  # First nested call value.
                    requested_partition_size = yield None

                while True:
                    value = nested_smart_generator.send(requested_partition_size)
                    _validate_smart_partition_value(value)

                    partition_size, partition = value

                    requested_partition_size = yield partition_size, func(*partition)
        except StopIteration:
            return requested_partition_size

    requested_partition_size = None
    value = first_value

    try:
        while True:
            requested_partition_size = yield from iterate_nested_generator(func(*value), requested_partition_size)
            value = next(generator)
    except StopIteration:
        return


def _partition_flatmap_smart_generator(
    func: Callable[[InputPartitionType], PartitionGenerator[OutputPartitionType]],
    generator: SmartPartitionGenerator[InputPartitionType],
) -> SmartPartitionGenerator[OutputPartitionType]:
    """
    `partition_flatmap()` specialisation for parent generators that are smart generators.

    The function always returns a smart generator.
    """

    def iterate_nested_generator(
        nested_generator: PartitionGenerator[OutputPartitionType],
        requested_partition_size: int,
        parent_partition_size: int,
    ):
        total_size = 0

        try:
            nested_value = next(nested_generator)

            if nested_value is not None:
                # This is a regular nested generator
                nested_simple_generator = cast(SimplePartitionIterator[OutputPartitionType], nested_generator)

                while True:
                    total_size += 1

                    requested_partition_size = yield parent_partition_size, nested_value
                    nested_value = next(nested_simple_generator)
            else:
                # This is a smart nested generator
                nested_smart_generator = cast(SmartPartitionGenerator[OutputPartitionType], nested_generator)

                while True:
                    nested_requested_partition_size = max(1, round(requested_partition_size / parent_partition_size))

                    nested_value = nested_smart_generator.send(nested_requested_partition_size)
                    _validate_smart_partition_value(nested_value)

                    nested_partition_size, nested_partition = nested_value

                    total_size += nested_partition_size

                    requested_partition_size = yield parent_partition_size * nested_partition_size, nested_partition
        except StopIteration:
            return total_size, requested_partition_size

    # Keep track of the nested total size of the previous iteration of the nested function, so that we can
    # estimate the optimal partition size to propagate to the parent's generator.
    total_nested_size = 0
    n_nested = 0

    requested_partition_size = yield None
    parent_requested_partition_size = 1

    try:
        while True:
            value = generator.send(parent_requested_partition_size)
            _validate_smart_partition_value(value)

            parent_partition_size, partition = value

            nested_size, requested_partition_size = yield from iterate_nested_generator(
                func(*partition), requested_partition_size, parent_partition_size
            )

            total_nested_size += nested_size
            n_nested += 1

            avg_nested_size = total_nested_size / n_nested
            parent_requested_partition_size = max(1, round(requested_partition_size / avg_nested_size))
    except StopIteration:
        return


def partition_zip(*generators: PartitionGenerator) -> PartitionGenerator[Tuple]:
    """
    Same as Python's built-in ``zip()``, but works on ``PartitionGenerator``s.
    """

    if len(generators) < 1:
        raise ValueError("at least one partition generator required.")

    try:
        # Detects which generators are partition-size aware

        is_smart = []
        first_values = []

        for generator in generators:
            first_value = next(generator)

            is_smart.append(first_value is None)
            first_values.append(first_value)

        has_smart = any(is_smart)

        # If at least one of the generator is partition-size aware (smart), yields a partition-size aware generator.

        if has_smart:
            requested_partition_size = yield None
        else:
            requested_partition_size = None

        # Collects the first values of the smart generators (we already have the non-smart first values).

        partition_size = None
        for i, generator in enumerate(generators):
            if not is_smart[i]:
                continue

            value = cast(SmartPartitionGenerator, generator).send(requested_partition_size)
            _validate_partition_zip_smart_partition_value(value, partition_size)
            partition_size, first_values[i] = value

        if has_smart:
            requested_partition_size = yield partition_size, tuple(first_values)
        else:
            yield tuple(first_values)

        # Iterates through the next values until one generator finishes.

        while True:
            values = []
            partition_size = None

            for i, generator in enumerate(generators):
                if is_smart[i]:
                    value = cast(SmartPartitionGenerator, generator).send(requested_partition_size)
                    _validate_partition_zip_smart_partition_value(value, partition_size)
                    partition_size, partition = value
                else:
                    partition = next(generator)

                values.append(partition)

            if has_smart:
                requested_partition_size = yield partition_size, tuple(values)
            else:
                yield tuple(values)
    except StopIteration:
        return


def _validate_partition_zip_smart_partition_value(
    partition_value: Tuple[int, PartitionType], partition_size: Optional[int]
):
    """
    Validates the smart partition value iterated by ``partition_zip()``, and validates that its size matches the other
    concurrent partitions' size (``partition_size``).
    """

    _validate_smart_partition_value(partition_value)

    current_partition_size, partition = partition_value

    if partition_size is not None and partition_size != current_partition_size:
        raise ValueError("all smart partition generators should yield identically sized partitions.")

    return current_partition_size, partition


def _validate_smart_partition_value(value):
    if not isinstance(value, Sequence) or len(value) != 2:
        raise ValueError("partition generator should yield a partition with its size.")

    partition_size, _ = value

    if not isinstance(partition_size, int) or partition_size < 1:
        raise ValueError("partition generator should return a strictly positive partition size.")
