import collections
import logging
from inspect import Parameter, currentframe
from itertools import chain
from typing import Callable, Deque, Generator, Iterable, Optional, Tuple, Union

import attrs

from parfun.backend.mixins import BackendEngine, ProfiledFuture
from parfun.entry_point import get_parallel_backend, set_parallel_backend_context
from parfun.kernel.function_signature import FunctionSignature, NamedArguments
from parfun.object import FunctionInputType, FunctionOutputType, PartitionType
from parfun.partition.object import PartitionGenerator
from parfun.partition_size_estimator.linear_regression_estimator import LinearRegessionEstimator
from parfun.partition_size_estimator.mixins import PartitionSizeEstimator
from parfun.profiler.functions import (
    export_task_trace, print_profile_trace, timed_combine_with, timed_function, timed_partition,
)
from parfun.profiler.object import PartitionedTaskTrace, TraceTime


@attrs.define
class ParallelFunction:
    """Wraps a function so that it executes in parallel using a map-reduce/scatter-gather approach.

    See the `@parallel()` decorator for a more user-friendly interface.
    """

    function: Callable[[FunctionInputType], FunctionOutputType] = attrs.field()

    function_name: str = attrs.field()

    split: Callable[[NamedArguments], Tuple[NamedArguments, PartitionGenerator[NamedArguments]]] = attrs.field()

    combine_with: Callable[[Iterable[FunctionOutputType]], FunctionOutputType] = attrs.field()

    initial_partition_size: Optional[Union[int, Callable[[FunctionInputType], int]]] = attrs.field(default=None)
    fixed_partition_size: Optional[Union[int, Callable[[FunctionInputType], int]]] = attrs.field(default=None)

    profile: bool = attrs.field(default=None)
    trace_export: Optional[str] = attrs.field(default=None)

    partition_size_estimator_factory: Callable[[], PartitionSizeEstimator] = attrs.field(
        default=LinearRegessionEstimator
    )

    _partition_size_estimator: Optional[PartitionSizeEstimator] = attrs.field(init=False, default=None)
    _function_signature: FunctionSignature = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        self._function_signature = FunctionSignature.from_function(self.function)

        if self.initial_partition_size is not None and self.fixed_partition_size is not None:
            raise ValueError("`initial_partition_size` and `fixed_partition_size` cannot be set simultaneously.")

        if self.fixed_partition_size is None:
            self._partition_size_estimator = self.partition_size_estimator_factory()

        self._validate_function_signature()

    @initial_partition_size.validator
    @fixed_partition_size.validator
    def _partition_size_validator(self, attribute, value):
        if value is not None and not isinstance(value, int) and not callable(value):
            raise ValueError(f"`{attribute.name}` should be either an integer, a callable or `None`.")

    def _validate_function_signature(self):
        if self._function_signature.has_var_arg or self._function_signature.has_var_kwarg:
            return

        if any(arg.kind == Parameter.POSITIONAL_ONLY for arg in self._function_signature.args.values()):
            raise ValueError("parfun toolkit does not support positional only parameters yet.")

    def __call__(self, *args, **kwargs) -> FunctionOutputType:
        current_backend = get_parallel_backend()
        allows_nested_tasks = current_backend is not None and current_backend.allows_nested_tasks()

        # Note: is_nested_parallelism check should appears before any backend check, as unsupported nested function
        # calls will have an empty backend setup.
        if is_nested_parallelism() and not allows_nested_tasks:
            logging.debug(
                f"backend does not support nested parallelism. Running {self.function.__name__} sequentially."
            )
            return self.function(*args, **kwargs)

        if current_backend is None:
            logging.warning(f"no parallel backend engine set, run `{self.function_name}(...)` sequentially.")
            return self.function(*args, **kwargs)

        # Assign a name to each argument based on the decorated function's signature.

        named_args = self._function_signature.assign(args, kwargs)

        # Initialize the partition generator

        non_partitioned_args, partition_generator = self.split(named_args)

        initial_partition_size, fixed_partition_size = self._get_user_partition_sizes(args, kwargs)

        partitions = timed_partition(
            partition_generator, self._partition_size_estimator, initial_partition_size, fixed_partition_size
        )

        # Execute the function

        if allows_nested_tasks:
            nested_backend = current_backend
        else:
            nested_backend = None

        results = run_function_on_partitions(
            self.function,
            non_partitioned_args,
            partitions,
            current_backend,
            nested_backend,
        )

        # Combine the results

        combined_result, task_trace = timed_combine_with(self.combine_with, self._partition_size_estimator, results)

        if self.profile:
            print_profile_trace(self.function, self.function_name, self._partition_size_estimator, task_trace)

        if self.trace_export:
            export_task_trace(self.trace_export, task_trace)

        logging.info(
            f"Run `{self.function_name}(...)` with {task_trace.partition_count} of "
            f"sub-tasks using backend {current_backend.__class__} successfully"
        )

        return combined_result

    def _get_user_partition_sizes(self, args, kwargs) -> Tuple[Optional[int], Optional[int]]:
        """Returns the initial partition size and fixed partition size for the calling function arguments."""

        if callable(self.initial_partition_size):
            initial_partition_size = self.initial_partition_size(*args, **kwargs)
        else:
            initial_partition_size = self.initial_partition_size

        if callable(self.fixed_partition_size):
            fixed_partition_size = self.fixed_partition_size(*args, **kwargs)
        else:
            fixed_partition_size = self.fixed_partition_size

        return initial_partition_size, fixed_partition_size


def is_nested_parallelism():
    """Returns True if there is any call to `_apply_function()` in the current call stack."""

    frame = currentframe()
    while frame is not None:
        if frame.f_code.co_name == run_function_in_worker.__name__ and frame.f_code.co_filename == __file__:
            return True
        frame = frame.f_back
    return False


def run_function_on_partitions(
    function: Callable[[PartitionType], FunctionOutputType],
    non_partitioned_args: NamedArguments,
    partitions: Generator[Tuple[NamedArguments, PartitionedTaskTrace], None, None],
    backend: BackendEngine,
    nested_backend: Optional[BackendEngine],
) -> Generator[Tuple[FunctionOutputType, TraceTime], None, None]:
    """
    Applies the provided function on all non-partitioned and partitioned arguments using the provided backend.
    """

    # First, tries to get the first 2 partitions. If we get less than 2, we run the function sequentially to avoid
    # any parallelism overhead.

    iterator = iter(partitions)

    first_values = []
    try:
        first_values.append(next(iterator))
        first_values.append(next(iterator))
    except StopIteration:
        # Less than 2 values, run these sequentially and return
        assert len(first_values) <= 2

        for partitioned_args in first_values:
            yield timed_function(
                run_function_in_worker,
                function,
                non_partitioned_args,
                partitioned_args,
                backend=None,
            )

        return

    # At least two values, submits these and the rest to the backend.

    with backend.session() as session:
        preloaded_non_partitioned_args = session.preload_value(non_partitioned_args)

        # We take care of futures.pop() no longer required futures' references as we yield them, to avoid keeping no
        # longer used results. Not doing this will prevent the Python's GC to free the yielded results once these have
        # been processed by the consuming function.
        futures: Deque[ProfiledFuture] = collections.deque()

        try:
            for partitioned_args in chain(first_values, iterator):
                futures.append(
                    session.submit(
                        run_function_in_worker,
                        function,
                        preloaded_non_partitioned_args,
                        partitioned_args,
                        nested_backend,
                    )
                )

                # Yields any finished future from the head of the queue.
                while len(futures) > 0 and futures[0].done():
                    yield futures.popleft().result_and_duration()

            # Yields the remaining results.
            while len(futures) > 0:
                yield futures.popleft().result_and_duration()
        finally:
            # If any failure, cancels all unfinished tasks.
            for future in futures:
                future.cancel()


def run_function_in_worker(
    function: Callable[[PartitionType], FunctionOutputType],
    non_partitioned_args: NamedArguments,
    partition: Tuple[NamedArguments, PartitionedTaskTrace],
    backend: Optional[BackendEngine] = None,
) -> Tuple[FunctionOutputType, PartitionedTaskTrace]:
    """
    Runs the function with the partitioned object, setting up the expected worker environment.

    :param non_partitioned_args: the function arguments that are identical for every function call.
    :param partition: the partitioned arguments and the associated partition task trace.
    :param backend: if not None, setup this backend before executing the function.

    :returns the function's output and the original partition task trace.
    """

    partitioned_args, trace = partition

    merged_args = non_partitioned_args.merge(partitioned_args)
    assert len(non_partitioned_args.var_args) == 0

    args, kwargs = merged_args.as_args_kwargs()

    if backend is not None:
        with set_parallel_backend_context(backend):
            result = function(*args, **kwargs)
    else:
        result = function(*args, **kwargs)

    return result, trace
