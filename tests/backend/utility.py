import time
import timeit
import unittest
from collections import deque
from concurrent.futures import wait

from parfun.backend.mixins import BackendEngine
from parfun.functions import parallel_map
from parfun.profiler.functions import profile
from parfun.profiler.object import TraceTime


# default time.sleep has no signatures, which will break scaler
def sleep(x):
    time.sleep(x)
    return


def is_backend_blocking(test_case: unittest.TestCase, backend: BackendEngine, max_concurrency: int = 1):
    """Tests the backend `submit()` method and checks that it's blocking when exceeding `max_concurrency` concurrent
    tasks."""

    N_TASKS = max_concurrency * 4
    DELAY = 0.5

    n_concurrent_tasks = 0
    futures = []

    def future_callback(_):
        nonlocal n_concurrent_tasks
        n_concurrent_tasks -= 1

    with backend.session() as session:
        for _ in range(0, N_TASKS):
            n_concurrent_tasks += 1
            future = session.submit(sleep, DELAY)

            future.add_done_callback(future_callback)

            futures.append(future)

            test_case.assertLessEqual(n_concurrent_tasks, max_concurrency)

        for future in futures:
            future.result()

    test_case.assertEqual(n_concurrent_tasks, 0)


def is_backend_handling_exceptions(test_case: unittest.TestCase, backend: BackendEngine):
    """Tests if the the backend correctly reports the exceptions to the returned value."""

    N_TASKS = 10

    futures = []

    with backend.session() as session:
        for i in range(0, N_TASKS):
            must_fail = i == (N_TASKS - 1)
            futures.append(session.submit(_failure_task, must_fail=must_fail))

        for i, future in enumerate(futures):
            must_fail = i == (N_TASKS - 1)

            if must_fail:
                test_case.assertRaises(Exception, future.result)
            else:
                future.result()


def is_backend_providing_speedup(
    test_case: unittest.TestCase,
    backend: BackendEngine,
    n_tasks: int = 8,
    n_workers: int = 2,
    max_overhead: float = 0.2,  # 20%
    task_duration: float = 10,
):
    if n_workers > 0:
        min_expected_duration = (n_tasks * task_duration) / float(n_workers)
    else:
        min_expected_duration = n_tasks * task_duration

    with backend.session() as session:
        duration = timeit.timeit(
            lambda: list(parallel_map(sleep, (task_duration for _ in range(0, n_tasks)), backend_session=session)),
            number=1,
        )

    test_case.assertAlmostEqual(duration, min_expected_duration, delta=min_expected_duration * max_overhead)
    test_case.assertGreaterEqual(duration, min_expected_duration)


def is_task_duration_correct(
    test_case: unittest.TestCase,
    backend: BackendEngine,
    concurrency: int = 0,
    tolerance: float = 0.25,  # 25%
    iterations: int = 1000,
):
    """
    Checks if the measured ``submit()`` task duration matches the returned backend's value.

    :param concurrency the concurrent processes. If zero, assumes no worker processes and the backend executes in the
    current thread.
    :param tolerance the test tolerance as a fraction of the backend's overhead (default: 25%).
    """

    # Measures the total task duration and compares it to the task duration returned by the backend's interface.
    #
    # We take two time measurements, one for the current process (only CPU time), and one for all the concurrent
    # processes (wall time).

    futures = deque()
    total_task_duration = 0

    with profile() as process_time, profile(time.perf_counter_ns) as eslaped_time, backend.session() as session:
        i = 0
        current_concurrency = 0
        while i < iterations:
            while current_concurrency <= concurrency:
                futures.append(session.submit(_no_op_task))
                current_concurrency += 1
                i += 1

            # Waits for oldest task to finish.
            future = futures.popleft()
            total_task_duration += future.duration()

            current_concurrency -= 1

        # Waits for all futures to finish.
        total_task_duration += sum(f.duration() for f in futures)

    measured_duration = process_time.value + eslaped_time.value * concurrency

    delta = measured_duration * tolerance

    test_case.assertAlmostEqual(measured_duration, total_task_duration, delta=delta)


def is_backend_supporting_nested_tasks(test_case: unittest.TestCase, backend: BackendEngine):
    """Validates that the backend supports nested tasks if it reports it."""

    if not backend.allows_nested_tasks():
        return

    with backend.session() as session:
        test_case.assertEqual(session.submit(_nested_task, backend, must_fail=False).result(), None)

        with test_case.assertRaises(Exception):
            # Must propagate inner tasks' exceptions.
            session.submit(_nested_task, backend, must_fail=True).result()


def warmup_workers(backend: BackendEngine, n_workers: int):
    """Makes sure the backend's workers are fully initialized by submitting a single task."""

    with backend.session() as session:
        wait([session.submit(_no_op_task) for _ in range(0, n_workers)])


def _no_op_task(duration: TraceTime = 1_000_000) -> None:
    starts_at = time.process_time_ns()

    while time.process_time_ns() - starts_at < duration:
        ...

    return None


def _failure_task(must_fail: bool, duration: TraceTime = 1_000_000):
    """Raises an exception if `must_fail` is True."""

    _no_op_task(duration)

    if must_fail:
        raise Exception("_failure_task: task failure.")


def _nested_task(backend: BackendEngine, must_fail: bool, duration: TraceTime = 1_000_000):
    with backend.session() as session:
        return session.submit(_failure_task, must_fail, duration).result()
