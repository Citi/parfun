import logging
import os
import time
import timeit
import unittest
from typing import Iterable, List, Tuple

try:
    import pandas as pd
except ImportError:
    raise ImportError("Pandas dependency missing. Use `pip install 'parfun[pandas]'` to install Pandas.")

from parfun.combine.collection import list_concat
from parfun.combine.dataframe import df_concat
from parfun.decorators import parfun
from parfun.entry_point import (
    BACKEND_REGISTRY,
    get_parallel_backend,
    set_parallel_backend,
    set_parallel_backend_context,
)
from parfun.partition.api import multiple_arguments, per_argument
from parfun.partition.collection import list_by_chunk
from parfun.partition.dataframe import df_by_row
from parfun.partition.object import PartitionGenerator
from tests.backend.utility import warmup_workers
from tests.test_helpers import find_nth_prime, random_df

_DELAY = 0.25


class TestDecorators(unittest.TestCase):
    N_WORKERS = 4

    def setUp(self) -> None:
        set_parallel_backend("local_multiprocessing", max_workers=TestDecorators.N_WORKERS)
        warmup_workers(get_parallel_backend(), TestDecorators.N_WORKERS)

    def test_parallel(self):
        """Makes sure the decorator provides some speedup on CPU intensive computations."""

        input_df = random_df(rows=1000, columns=10, low=45, high=50)

        def sequential_compute():
            return _find_all_nth_primes.__wrapped__(input_df)

        def parallel_compute():
            return _find_all_nth_primes(input_df)

        self.assertTrue(sequential_compute().equals(parallel_compute()))

        N_PROBES = 3

        sequential_duration = timeit.timeit(sequential_compute, number=N_PROBES) / N_PROBES
        parallel_duration = timeit.timeit(parallel_compute, number=N_PROBES) / N_PROBES

        speedup = sequential_duration / parallel_duration
        logging.info(
            f"_find_all_nth_primes(): sequential: {sequential_duration:0.4f}s - "
            f"parallel: {parallel_duration:0.4f}s - "
            f"speedup: {speedup:0.4f}x"
        )

        self.assertGreater(speedup, 1)

    def test_parallel_multiple_arguments(self):
        """Makes sure the decorator works with multiple input parameters."""

        col = [1] * 500
        constant = 10

        def sequential_compute():
            return _sum_horizontally.__wrapped__(col, col, col, constant)

        def parallel_compute():
            return _sum_horizontally(col, col, col, constant)

        self.assertEqual(sequential_compute(), parallel_compute())

        sequential_duration = timeit.timeit(sequential_compute, number=1)
        parallel_duration = timeit.timeit(parallel_compute, number=1)

        speedup = sequential_duration / parallel_duration
        logging.info(
            f"_sum_horizontally(): sequential: {sequential_duration:0.4f}s - "
            f"parallel: {parallel_duration:0.4f}s - "
            f"speedup: {speedup:0.4f}x"
        )

        self.assertGreater(speedup, 1)

    def test_parallel_concurrent_generator(self):
        """Makes sure the scheduler runs the partition and combine generators concurrently with the worker
        computations."""

        input_data = [12, 344, 65, 19, 12, 176, 12]

        _delayed_sum(input_data)

        duration = timeit.timeit(lambda: _delayed_sum(input_data), number=1)

        expected_duration = (
            len(input_data) * _DELAY * 2
        )  # * 2 as both the partition and combine generators are delayed.

        self.assertGreater(duration, expected_duration)
        self.assertAlmostEqual(duration, expected_duration, delta=expected_duration * 0.2)  # within 20% of expected

    @unittest.skipUnless("scaler_local" in BACKEND_REGISTRY, "Scaler backend not installed")
    def test_parallel_nested_calls(self):
        """Makes sure that the decorators handles nested parallel function calls."""

        PARENT_INPUT_SIZE = 10
        CHILD_INPUT_SIZE = 10

        INPUT_DATA = list(range(0, PARENT_INPUT_SIZE))

        with set_parallel_backend_context("local_multiprocessing", max_workers=2):
            # When the backend does not support nested tasks, the child tasks should execute on the same process.

            self.assertFalse(get_parallel_backend().allows_nested_tasks())

            pids = _nested_parent_function(INPUT_DATA, CHILD_INPUT_SIZE)

            self.assertEqual(len(pids), len(INPUT_DATA) * CHILD_INPUT_SIZE)

            for parent_pid, child_pid in pids:
                self.assertEqual(parent_pid, child_pid)

        with set_parallel_backend_context("scaler_local", n_workers=2, per_worker_queue_size=10):
            # When the backend supports nested tasks, the child tasks should execute on different processes.

            self.assertTrue(get_parallel_backend().allows_nested_tasks())

            pids = _nested_parent_function(INPUT_DATA, CHILD_INPUT_SIZE)

            self.assertEqual(len(pids), len(INPUT_DATA) * CHILD_INPUT_SIZE)

            for parent_pid, child_pid in pids:
                self.assertNotEqual(parent_pid, child_pid)

    def test_parallel_list_by_chunk(self):
        a = [1, 2, 3, 4]
        b = [2, 3, 4, 5]
        constant_df = pd.DataFrame([[3], [3]])

        results = _calculate_some_df(a, b, constant_df)
        expected = pd.DataFrame([[6], [6], [8], [8], [10], [10], [12], [12]])
        self.assertTrue(results.equals(expected))

    def test_fixed_partition_size(self):
        xs = list(range(0, 100))

        _fixed_partition_size(xs)  # 100 values, multiple of 10.

        with self.assertRaises(ValueError):
            _fixed_partition_size(xs[1:])  # 99 values, not a multiple of 10.

    def test_per_argument(self):
        N = 100

        xs = list(range(0, N))
        df = pd.DataFrame({"x": [1 for _ in range(0, N)], "y": xs})

        sequential = _per_argument_sum.__wrapped__(xs, df)
        parallel = _per_argument_sum(xs, df)

        self.assertTrue(sequential.equals(parallel))


@parfun(split=multiple_arguments(("col1", "col2", "col3"), list_by_chunk), combine_with=sum, fixed_partition_size=100)
def _sum_horizontally(col1: Iterable[int], col2: Iterable[int], col3: Iterable[int], constant: int) -> int:
    result = 0
    for i in zip(col1, col2, col3):
        time.sleep(0.01)
        result += sum(i) * constant
    return result


@parfun(split=per_argument(values=df_by_row), combine_with=df_concat)
def _find_all_nth_primes(values: pd.DataFrame) -> pd.DataFrame:
    return values.apply(lambda series: series.apply(find_nth_prime))


@parfun(split=multiple_arguments(("a", "b"), list_by_chunk), combine_with=df_concat)
def _calculate_some_df(a: List[int], b: List[float], constant_df: pd.DataFrame) -> pd.DataFrame:
    list_of_df = []
    for i, j in zip(a, b):
        list_of_df.append(constant_df + i + j)

    return pd.concat(list_of_df)


def _delayed_partition(values: Iterable[float]) -> PartitionGenerator[Tuple[List[float]]]:
    yield None
    for i, v in enumerate(values):
        logging.debug(f"starts generating partition #{i}")
        time.sleep(_DELAY)
        logging.debug(f"finished generating partition #{i}")
        yield 1, ([v],)


def _delayed_combine(values: Iterable[float]) -> float:
    result = 0.0
    for i, v in enumerate(values):
        logging.debug(f"starts combining partition #{i}")
        time.sleep(_DELAY)
        result += v
        logging.debug(f"finished combining partition #{i}")
    return result


@parfun(split=per_argument(values=_delayed_partition), combine_with=_delayed_combine)
def _delayed_sum(values: Iterable[float]) -> float:
    logging.debug("start delayed sum")
    result = sum(values)
    logging.debug("finished delayed sum")
    return result


@parfun(split=per_argument(values=list_by_chunk), combine_with=list_concat)
def _nested_parent_function(values: List[int], child_input_size: int) -> List[Tuple[int, int]]:
    parent_pid = os.getpid()
    child_input = [parent_pid for _ in range(0, child_input_size)]

    return list_concat(_nested_child_function(child_input) for _ in values)


@parfun(split=per_argument(parent_pids=list_by_chunk), combine_with=list_concat)
def _nested_child_function(parent_pids: List[int]) -> List[Tuple[int, int]]:
    child_pid = os.getpid()
    return [(parent_pid, child_pid) for parent_pid in parent_pids]


@parfun(split=per_argument(values=list_by_chunk), combine_with=list_concat, fixed_partition_size=10)
def _fixed_partition_size(values: List) -> List:
    if len(values) != 10:
        raise ValueError("invalid fixed partition size.")
    return values


@parfun(split=per_argument(a=list_by_chunk, b=df_by_row), combine_with=df_concat)
def _per_argument_sum(a: List, b: pd.DataFrame) -> pd.DataFrame:
    """Multiples the dataframe values by the corresponding list items."""

    if len(a) != b.shape[0]:
        raise ValueError("length of `a` should matches number `b` rows.")

    result = b.copy()

    for i, v in enumerate(a):
        result.iloc[i, :] *= v

    return result


if __name__ == "__main__":
    unittest.main()
