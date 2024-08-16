import logging
import unittest
from typing import Callable, Generator, Iterable, Tuple

from parfun.decorators import parfun
from parfun.entry_point import set_parallel_backend


def factorial_partition_function(
    start: int, end: int, chunk_size: int = 5
) -> Generator[Tuple[int, Tuple[int, ...]], int, None]:
    """
        A python generator to partition a range between
        [start] and [end] using a chunk size of [chunk_size]
    Return:
        Tuple[int, Tuple[int, ...],
        e.g, (chunk_size, (start, end))
    """
    while start <= end:
        new_end = min(start + chunk_size - 1, end)
        yield new_end - start + 1, (start, new_end)
        start = new_end + 1


def factorial_combine_function(values: Iterable[int]) -> int:
    from functools import reduce

    return reduce(lambda x, y: x * y, values, 1)


@parfun(
    partition_on=("start", "end"), partition_with=factorial_partition_function, combine_with=factorial_combine_function
)
def factorial_computing_using_loop(start: int, end: int, callback: Callable = lambda x: x) -> int:
    """
    Get factorial computation between [start] and [end]
    e.g.,
       >> factorial_computing_using_loop(1, 5) = 1*2*3*4*5 = 120
    """
    m = 1
    for i in range(start, end + 1):
        m = m * i
    return callback(m)


def factorial_computing_using_multiprocess(start: int, end: int) -> int:
    from multiprocessing import Pool

    num_workers = 5
    # [(1, 9), (10, 18), (19, 27), (28, 36), (37, 45), (46, 50)]
    partition_data = [data for _, data in factorial_partition_function(start, end, (end - start) // num_workers)]
    with Pool(num_workers) as pool:
        results = pool.starmap(factorial_computing_using_loop, partition_data)
    return factorial_combine_function(results)


@parfun(
    partition_on=("start", "end"), partition_with=factorial_partition_function, combine_with=factorial_combine_function
)
def factorial_computing_using_recursive(start: int, end: int) -> int:
    if end == start:
        return start
    else:
        return end * factorial_computing_using_recursive(start, end - 1)


def callback_func(x):
    return x * 2


class TestFactorialComputation(unittest.TestCase):
    def test_loop_using_none_backend(self):
        set_parallel_backend("none")
        logging.info(f"Result: {factorial_computing_using_loop(1, 10)}")

    def test_loop_using_single_process_backend(self):
        set_parallel_backend("local_single_process")
        logging.info(f"Result: {factorial_computing_using_loop(1, 10)}")

    def test_loop_using_multiprocess_backend(self):
        set_parallel_backend("local_multiprocessing")
        logging.info(f"Result: {factorial_computing_using_loop(1, 10)}")
