import time
import unittest

from parfun.entry_point import get_parallel_backend, set_parallel_backend
from parfun.functions import parallel_map, parallel_starmap, parallel_timed_map
from tests.backend.utility import warmup_workers

DELAY = 50_000_000  # 50 ms


def func(i: int) -> int:
    return i * i


def delayed_func(i: int) -> int:
    begins_at = time.process_time_ns()

    while time.process_time_ns() - begins_at < DELAY:
        ...

    return func(i)


class TestFunctions(unittest.TestCase):
    N_WORKERS = 4

    def setUp(self) -> None:
        set_parallel_backend("local_multiprocessing", max_workers=TestFunctions.N_WORKERS)

        warmup_workers(get_parallel_backend(), TestFunctions.N_WORKERS)

    def test_parallel_timed_map(self):
        N = 10

        values = list(parallel_timed_map(delayed_func, range(0, N)))

        for i, value in enumerate(values):
            self.assertEqual(value[0], func(i))
            self.assertAlmostEqual(value[1], DELAY, delta=DELAY * 0.15)  # 15% tolerance

    def test_parallel_map(self):
        N = 10
        self.assertListEqual(list(parallel_map(func, range(0, N))), [func(i) for i in range(0, N)])

    def test_parallel_map_laziness(self):
        """`parallel_map()` should consume input and produce output concurrently."""

        N = 10

        set_parallel_backend("local_single_process")

        previous_task = None

        def generator():
            nonlocal previous_task

            for i in range(0, N):
                previous_task = "generator"
                yield i
                self.assertEqual(previous_task, "consumer")

        def consumer(iterable):
            nonlocal previous_task

            total = 0
            for i in iterable:
                self.assertEqual(previous_task, "generator")
                total += i
                previous_task = "consumer"

            return total

        self.assertEqual(consumer(parallel_map(func, generator())), sum(func(i) for i in range(0, N)))

    def test_parallel_starmap(self):
        self.assertListEqual(
            list(parallel_starmap(func, ((i,) for i in range(0, 10)))), [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
        )


if __name__ == "__main__":
    unittest.main()
