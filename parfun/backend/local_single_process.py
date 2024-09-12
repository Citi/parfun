from typing import Callable

from parfun.backend.mixins import BackendEngine, BackendSession
from parfun.backend.profiled_future import ProfiledFuture
from parfun.profiler.functions import profile


class LocalSingleProcessSession(BackendSession):
    # Additional constant scheduling overhead that cannot be accounted for when measuring the task execution duration.
    CONSTANT_SCHEDULING_OVERHEAD = 5_000  # 5 us

    def __enter__(self) -> "LocalSingleProcessSession":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        return None

    def submit(self, fn: Callable, *args, **kwargs) -> ProfiledFuture:
        with profile() as function_duration:
            future = ProfiledFuture()

            try:
                result = fn(*args, **kwargs)
                exception = None
            except Exception as e:
                exception = e
                result = None

        task_duration = self.CONSTANT_SCHEDULING_OVERHEAD + function_duration.value

        if exception is None:
            future.set_result(result, duration=task_duration)
        else:
            future.set_exception(exception, duration=task_duration)

        return future


class LocalSingleProcessBackend(BackendEngine):
    def session(self) -> BackendSession:
        return LocalSingleProcessSession()

    def shutdown(self):
        pass

    def allows_nested_tasks(self) -> bool:
        return False
