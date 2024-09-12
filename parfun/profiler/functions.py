import csv
import datetime
import inspect
import sys
import time
from contextlib import contextmanager
from typing import Any, Callable, Generator, Iterable, Optional, TextIO, Tuple

from parfun.kernel.function_signature import NamedArguments
from parfun.object import FunctionOutputType
from parfun.partition.object import PartitionGenerator
from parfun.partition_size_estimator.mixins import PartitionSizeEstimator
from parfun.profiler.object import PartitionedTaskTrace, ProfileDuration, TaskTrace, TraceTime


@contextmanager
def profile(timer_function: Callable[[], TraceTime] = time.process_time_ns):
    """
    Provides a Python ``with`` context that measures the execution time of the enclosing block.

    .. code:: python

        with profile() as duration:
            some_heavy_task()

        print(f"Task duration: {duration.value}ns")

    """

    starts_at = timer_function()

    profile_duration = ProfileDuration()
    yield profile_duration

    profile_duration.value = timer_function() - starts_at


def timed_function(fn: Callable, *args, **kwargs) -> Tuple[TraceTime, Any]:
    """
    Runs the provided function with the specified args, and returns its execution CPU time and its returned value.
    """

    with profile() as duration:
        result = fn(*args, **kwargs)

    return duration.value, result


def timed_partition(
    generator: PartitionGenerator[NamedArguments],
    partition_size_estimator: Optional[PartitionSizeEstimator],
    initial_partition_size: Optional[int],
    fixed_partition_size: Optional[int],
) -> Generator[Tuple[NamedArguments, PartitionedTaskTrace], None, None]:
    """
    Wraps the ``partition_generator`` with performance timers.

    :returns the wrapped generator.
    """

    if initial_partition_size is not None and fixed_partition_size is not None:
        raise ValueError("`initial_partition_size` and `fixed_partition_size` cannot be set simultaneously.")

    if partition_size_estimator is not None and fixed_partition_size is not None:
        raise ValueError("`partition_size_estimator` and `fixed_partition_size` cannot be set simultaneously.")

    if partition_size_estimator is None and fixed_partition_size is None:
        raise ValueError("either `partition_size_estimator` or `fixed_partition_size` must be set.")

    if not inspect.isgenerator(generator):
        raise TypeError(f"partition functions must be generators, got {type(generator).__name__}")

    try:
        with profile() as first_value_duration:
            first_value = next(generator)

        if first_value is not None:
            # This is a regular generator. Iterates without relying on the partition size estimator.

            trace = PartitionedTaskTrace(
                partition_size_estimate=None, partition_size=1, partition_duration=first_value_duration.value
            )
            yield first_value, trace

            while True:
                with profile() as partition_duration:
                    partition = next(generator)

                trace = PartitionedTaskTrace(
                    partition_size_estimate=None, partition_size=1, partition_duration=partition_duration.value
                )
                yield partition, trace
        else:
            # This is a smart generator. Iterates while running the partition size estimator.

            if initial_partition_size is not None or fixed_partition_size is not None:
                requested_partition_size = initial_partition_size or fixed_partition_size
                partition_size_estimate = None
            else:
                assert partition_size_estimator is not None
                partition_size_estimate = partition_size_estimator.estimate()
                requested_partition_size = partition_size_estimate.value

            assert requested_partition_size is not None

            while True:
                with profile() as partition_duration:
                    partition_size, partition = generator.send(requested_partition_size)

                trace = PartitionedTaskTrace(
                    partition_size_estimate=partition_size_estimate,
                    partition_size=partition_size,
                    partition_duration=partition_duration.value,
                )
                yield partition, trace

                if fixed_partition_size is None:
                    assert partition_size_estimator is not None
                    partition_size_estimate = partition_size_estimator.estimate()
                    requested_partition_size = partition_size_estimate.value
                else:
                    requested_partition_size = fixed_partition_size

    except StopIteration:
        return


def timed_combine_with(
    combine_with: Callable[[Iterable[FunctionOutputType]], FunctionOutputType],
    partition_size_estimator: Optional[PartitionSizeEstimator],
    results: Iterable[Tuple[Tuple[FunctionOutputType, PartitionedTaskTrace], TraceTime]],
) -> Tuple[FunctionOutputType, TaskTrace]:
    """
    Wraps the ``combine_with`` function with performance timers.

    :param combine_with: the combining function.
    :param partition_size_estimator: forward execution feedback to the estimator is not ``None``.
    :param results: the partitioned execution result and their associated partitioning and execution times.

    :returns the combined output, and the task execution time measurements.
    """

    trace = TaskTrace()

    def timed_combine_generator():
        for result_with_trace, task_duration in results:
            result, partition_trace = result_with_trace

            partition_trace.task_duration = task_duration

            with profile() as combine_duration:
                yield result

            partition_trace.combine_duration = combine_duration.value

            if partition_size_estimator is not None:
                partition_size_estimator.add_partition_trace(partition_trace)

            trace.partition_traces.append(partition_trace)

    result = combine_with(timed_combine_generator())

    return result, trace


def print_profile_trace(
    function: Callable,
    function_name: Optional[str],
    partition_size_estimator: Optional[PartitionSizeEstimator],
    task_trace: TaskTrace,
    file: TextIO = sys.stderr,
) -> None:
    """Prints an human-readable summary of the task's execution times."""

    def print_to_file(value: str):
        print(value, file=file)

    partition_timedelta = datetime.timedelta(microseconds=task_trace.total_partition_duration / 1000)
    partition_average_timedelta = partition_timedelta / task_trace.partition_count
    function_timedelta = datetime.timedelta(microseconds=task_trace.total_task_duration / 1000)
    function_timedeltas = [
        datetime.timedelta(microseconds=partition_trace.task_duration / 1000)
        for partition_trace in task_trace.partition_traces
    ]
    combine_timedelta = datetime.timedelta(microseconds=task_trace.total_combine_duration / 1000)

    total_cpu_duration = partition_timedelta + function_timedelta + combine_timedelta

    parallel_overhead = partition_timedelta + combine_timedelta

    if function_name is not None:
        print_to_file(f"{function_name}()")
    else:
        print_to_file(f"{function.__name__}()")

    print_to_file(f"\ttotal CPU execution time: {total_cpu_duration}.")

    # Execution stats

    min_compute = min(function_timedeltas)
    max_compute = max(function_timedeltas)

    print_to_file(f"\tcompute time: {function_timedelta} ({function_timedelta / total_cpu_duration * 100:.2f}%)")
    print_to_file(f"\t\tmin.: {min_compute}")
    print_to_file(f"\t\tmax.: {max_compute}")
    print_to_file(f"\t\tavg.: {function_timedelta / task_trace.partition_count}")

    # Partitioning / Combining stats

    print_to_file(
        f"\ttotal parallel overhead: {parallel_overhead} ({parallel_overhead / total_cpu_duration * 100:.2f}%)"
    )
    print_to_file(
        f"\t\ttotal partitioning: {partition_timedelta} ({partition_timedelta / total_cpu_duration * 100:.2f}%)"
    )
    print_to_file(f"\t\taverage partitioning: {partition_average_timedelta}")
    print_to_file(f"\t\ttotal combining: {combine_timedelta} ({combine_timedelta / total_cpu_duration * 100:.2f}%)")

    theoretical_speedup = total_cpu_duration / max(parallel_overhead, max_compute)
    print_to_file(f"\tmaximum speedup (theoretical): {theoretical_speedup:.2f}x")

    # Partition size estimator state
    print_to_file(f"\ttotal partition count: {task_trace.partition_count}")

    if partition_size_estimator is not None:
        estimator_state = partition_size_estimator.state()
        print_to_file(f"\t\testimator state: {estimator_state.value}")

        estimate = partition_size_estimator.estimate(dry_run=True).value
        print_to_file(f"\t\testimated partition size: {estimate}")


def export_task_trace(file_path: str, task_trace: TaskTrace) -> None:
    """Exports the task trace as a CSV file."""

    def export_partitioned_task_trace(trace: PartitionedTaskTrace) -> Tuple:
        if trace.partition_size_estimate is not None:
            requested_partition_size = trace.partition_size_estimate.value
        else:
            requested_partition_size = None

        return (
            requested_partition_size,
            trace.partition_size,
            trace.partition_duration,
            trace.task_duration,
            trace.combine_duration,
        )

    with open(file_path, "w") as file:
        writer = csv.writer(file)
        writer.writerow(
            (
                "requested_partition_size",
                "partition_size",
                "partition_duration",
                "function_duration",
                "combine_duration",
            )
        )
        writer.writerows(export_partitioned_task_trace(trace) for trace in task_trace.partition_traces)
