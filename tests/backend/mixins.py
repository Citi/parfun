import abc
import math
import time
import timeit
import warnings
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import Deque

from parfun.backend.mixins import BackendEngine, ProfiledFuture
from parfun.functions import parallel_map
from parfun.profiler.functions import profile

from tests.backend.utility import failure_task, nested_task, no_op_task, sleep


class BackendEngineTestCase(metaclass=abc.ABCMeta):
    """
    Validates the requirements of the ``BackendEngine`` interface.
    """

    # Remark: the class cannot be an instance of TestCase as unittest will try to instance it and execute it as a test
    # case.

    @abc.abstractmethod
    def backend(self) -> BackendEngine:
        raise NotImplementedError()

    @abc.abstractmethod
    def n_workers(self) -> int:
        raise NotImplementedError()

    def test_is_blocking(self):
        """Tests the backend `submit()` method and checks that it's blocking when exceeding `max_concurrency` concurrent
        tasks."""

        N_TASKS = self.n_workers() * 4
        DELAY = 0.5

        n_concurrent_tasks = 0
        futures = []

        def future_callback(_):
            nonlocal n_concurrent_tasks
            n_concurrent_tasks -= 1

        with self.backend().session() as session:
            for _ in range(0, N_TASKS):
                n_concurrent_tasks += 1
                future = session.submit(sleep, DELAY)

                future.add_done_callback(future_callback)

                futures.append(future)

                self.assertLessEqual(n_concurrent_tasks, self.n_workers())  # type: ignore[attr-defined]

            for future in futures:
                future.result()

        self.assertEqual(n_concurrent_tasks, 0)  # type: ignore[attr-defined]

    def test_is_backend_handling_exceptions(self):
        """Tests if the the backend correctly reports the exceptions to the returned value."""

        N_TASKS = 10

        futures = []

        with self.backend().session() as session:
            for i in range(0, N_TASKS):
                must_fail = i == (N_TASKS - 1)
                futures.append(session.submit(failure_task, must_fail=must_fail))

            for i, future in enumerate(futures):
                must_fail = i == (N_TASKS - 1)

                if must_fail:
                    self.assertRaises(Exception, future.result)  # type: ignore[attr-defined]
                else:
                    future.result()

    def test_is_backend_providing_speedup(self):
        N_TASKS = 8
        MAX_OVERHEAD = 0.2  # 20%
        TASK_DURATION = 10

        if self.n_workers() > 0:
            min_expected_duration = (N_TASKS * TASK_DURATION) / float(self.n_workers())
        else:
            min_expected_duration = N_TASKS * TASK_DURATION

        with self.backend().session() as session:
            duration = timeit.timeit(
                lambda: list(parallel_map(sleep, (TASK_DURATION for _ in range(0, N_TASKS)), backend_session=session)),
                number=1,
            )

        self.assertAlmostEqual(  # type: ignore[attr-defined]
            duration, min_expected_duration, delta=min_expected_duration * MAX_OVERHEAD
        )
        self.assertGreaterEqual(duration, min_expected_duration)  # type: ignore[attr-defined]

    def test_task_duration(self):
        """
        Checks if the measured ``submit()`` task duration matches the returned backend's value.
        """

        TOLERANCE = 0.25  # 25%
        ITERATIONS = 1000

        # Measures the total task duration and compares it to the task duration returned by the backend's interface.
        #
        # We take two time measurements, one for the current process (only CPU time), and one for all the concurrent
        # processes (wall time).

        futures: Deque[ProfiledFuture] = deque()
        total_task_duration = 0

        with profile() as process_time, profile(
            time.perf_counter_ns
        ) as eslaped_time, self.backend().session() as session:
            i = 0
            current_concurrency = 0
            while i < ITERATIONS:
                while current_concurrency <= self.n_workers():
                    futures.append(session.submit(no_op_task))
                    current_concurrency += 1
                    i += 1

                # Waits for oldest task to finish.
                future = futures.popleft()
                total_task_duration += future.duration()

                current_concurrency -= 1

            # Waits for all futures to finish.
            total_task_duration += sum(f.duration() for f in futures)

        measured_duration = process_time.value + eslaped_time.value * self.n_workers()

        delta = measured_duration * TOLERANCE

        if abs(measured_duration - total_task_duration) < delta:
            warnings.warn(f"Expected execution duration of {total_task_duration} ns, measured {measured_duration} ns.")

    def test_supports_nested_tasks(self):
        """Validates that the backend supports nested tasks if it reports it."""

        if not self.backend().allows_nested_tasks():
            return

        with self.backend().session() as session:
            self.assertEqual(  # type: ignore[attr-defined]
                session.submit(nested_task, self.backend(), must_fail=False).result(), None
            )

            with self.assertRaises(Exception):  # type: ignore[attr-defined]
                # Must propagate inner tasks' exceptions.
                session.submit(nested_task, self.backend(), must_fail=True).result()

    def test_is_threadsafe(self):
        """Validates that the backend can be used from multiple concurrent threads."""

        N_THREADS = 20
        N_TASKS = N_THREADS * 10

        def threaded_backend_session(arg):
            try:
                with self.backend().session() as session:
                    return session.submit(math.pow, 2, 3).result() == math.pow(2, 3)
            except Exception:
                return False

        with ThreadPoolExecutor(max_workers=N_THREADS) as thread_executor:
            results = thread_executor.map(threaded_backend_session, [()] * N_TASKS)

            self.assertTrue(all(results))  # type: ignore[attr-defined]
