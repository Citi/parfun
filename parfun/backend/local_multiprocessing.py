import multiprocessing
from concurrent.futures import Executor, Future, ProcessPoolExecutor, ThreadPoolExecutor
from threading import BoundedSemaphore

import attrs
import psutil
from attrs.validators import instance_of

from parfun.backend.mixins import BackendEngine, BackendSession
from parfun.backend.profiled_future import ProfiledFuture
from parfun.profiler.functions import profile, timed_function


class LocalMultiprocessingSession(BackendSession):
    # Additional constant scheduling overhead that cannot be accounted for when measuring the task execution duration.
    CONSTANT_SCHEDULING_OVERHEAD = 1_500_000  # 1.5ms

    def __init__(self, underlying_executor: Executor):
        self._underlying_executor = underlying_executor
        self._concurrent_task_guard = BoundedSemaphore(underlying_executor._max_workers)  # type: ignore[attr-defined]

    def __enter__(self) -> "LocalMultiprocessingSession":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        return None

    def submit(self, fn, *args, **kwargs) -> ProfiledFuture:
        with profile() as submit_duration:
            future = ProfiledFuture()

            self._concurrent_task_guard.acquire()

            underlying_future = self._underlying_executor.submit(timed_function, fn, *args, **kwargs)

        def on_done_callback(underlying_future: Future):
            assert submit_duration.value is not None

            if underlying_future.cancelled():
                future.cancel()
                return

            with profile() as release_duration:
                exception = underlying_future.exception()

                if exception is None:
                    function_duration, result = underlying_future.result()
                else:
                    function_duration = 0
                    result = None

                self._concurrent_task_guard.release()

            task_duration = (
                self.CONSTANT_SCHEDULING_OVERHEAD + submit_duration.value + function_duration + release_duration.value
            )

            if exception is None:
                future.set_result(result, duration=task_duration)
            else:
                future.set_exception(exception, duration=task_duration)

        underlying_future.add_done_callback(on_done_callback)

        return future


@attrs.define(init=False)
class LocalMultiprocessingBackend(BackendEngine):
    """
    Concurrent engine that uses Python builtin :mod:`multiprocessing` module.
    """

    _underlying_executor: Executor = attrs.field(validator=instance_of(Executor), init=False)
    _concurrent_task_guard: BoundedSemaphore = attrs.field(validator=instance_of(BoundedSemaphore), init=False)

    def __init__(self, max_workers: int = psutil.cpu_count(logical=False) - 1, is_process: bool = True, **kwargs):
        if is_process:
            self._underlying_executor = ProcessPoolExecutor(
                max_workers=max_workers, mp_context=multiprocessing.get_context("spawn"), **kwargs
            )
        else:
            self._underlying_executor = ThreadPoolExecutor(max_workers=max_workers, **kwargs)

    def session(self) -> LocalMultiprocessingSession:
        return LocalMultiprocessingSession(self._underlying_executor)

    def shutdown(self, wait=True):
        self._underlying_executor.shutdown(wait=wait)

    def allows_nested_tasks(self) -> bool:
        return False
