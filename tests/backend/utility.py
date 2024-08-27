import time
from concurrent.futures import wait

from parfun.backend.mixins import BackendEngine
from parfun.profiler.object import TraceTime


def failure_task(must_fail: bool, duration: TraceTime = 1_000_000):
    """Raises an exception if `must_fail` is True."""

    no_op_task(duration)

    if must_fail:
        raise Exception("_failure_task: task failure.")


def nested_task(backend: BackendEngine, must_fail: bool, duration: TraceTime = 1_000_000):
    with backend.session() as session:
        return session.submit(failure_task, must_fail, duration).result()


def no_op_task(duration: TraceTime = 1_000_000) -> None:
    starts_at = time.process_time_ns()

    while time.process_time_ns() - starts_at < duration:
        ...

    return None


def sleep(x):
    """Built-in time.sleep has no signatures, which will break scaler"""
    time.sleep(x)
    return


def warmup_workers(backend: BackendEngine, n_workers: int):
    """Makes sure the backend's workers are fully initialized by submitting a single task."""

    with backend.session() as session:
        wait([session.submit(no_op_task) for _ in range(0, n_workers)])
