import abc
from contextlib import contextmanager
from threading import BoundedSemaphore
from typing import Generator, Optional

try:
    from dask.distributed import Client, Future, LocalCluster, worker_client
    from dask.distributed.client import ClientExecutor
except ImportError:
    raise ImportError("Dask dependencies missing. Use `pip install 'parfun[dask]'` to install Dask dependencies.")

import psutil

from parfun.backend.mixins import BackendEngine, BackendSession
from parfun.backend.profiled_future import ProfiledFuture
from parfun.profiler.functions import profile, timed_function


class DaskSession(BackendSession):
    # Additional constant scheduling overhead that cannot be accounted for when measuring the task execution duration.
    CONSTANT_SCHEDULING_OVERHEAD = 20_000_000  # 20ms

    def __init__(self, engine: "DaskBaseBackend", n_workers: int):
        self._engine = engine
        self._concurrent_task_guard = BoundedSemaphore(n_workers)

    def __enter__(self) -> "DaskSession":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        return None

    def submit(self, fn, *args, **kwargs) -> Optional[ProfiledFuture]:
        with profile() as submit_duration:
            future = ProfiledFuture()

            acquired = self._concurrent_task_guard.acquire()
            if not acquired:
                return None

            with self._engine.executor() as executor:  # type: ignore[var-annotated]
                underlying_future = executor.submit(timed_function, fn, *args, **kwargs)

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


class DaskBaseBackend(BackendEngine, metaclass=abc.ABCMeta):
    def __init__(self, n_workers: int) -> None:
        self._n_workers = n_workers

    def session(self) -> DaskSession:
        return DaskSession(self, self._n_workers)

    @abc.abstractmethod
    @contextmanager
    def executor(self) -> Generator[ClientExecutor, None, None]:
        raise NotImplementedError

    def allows_nested_tasks(self) -> bool:
        return False


class DaskRemoteClusterBackend(DaskBaseBackend):
    """Connects to a previously instantiated Dask instance as a backend engine."""

    def __init__(self, scheduler_address: str):
        self._client = Client(address=scheduler_address)

        n_workers = len(self._client.scheduler_info()["workers"])
        super().__init__(n_workers)

        self._executor = self._client.get_executor()

    @contextmanager
    def executor(self) -> Generator[ClientExecutor, None, None]:
        yield self._executor

    def get_scheduler_address(self) -> str:
        return self._client.cluster.scheduler_address

    def disconnect(self, wait: bool = True):
        self._executor.shutdown(wait=wait)
        self._client.close()

    def shutdown(self):
        pass


class DaskLocalClusterBackend(DaskRemoteClusterBackend):
    """Creates a Dask cluster on the local machine and uses it as a backend engine."""

    def __init__(
        self, n_workers: int = psutil.cpu_count(logical=False) - 1, dashboard_address=":33333", memory_limit="100GB"
    ):
        self._cluster = LocalCluster(
            n_workers=n_workers, threads_per_worker=1, dashboard_address=dashboard_address, memory_limit=memory_limit
        )
        super().__init__(self._cluster.scheduler_address)

    def shutdown(self):
        self._cluster.close()


class DaskCurrentBackend(DaskBaseBackend):
    """
    Uses the current Dask worker context to deduce the backend instance.

    This backend should be used by Dask's worker tasks that desire to access the underlying backend instance.
    """

    def __init__(self, n_workers: int) -> None:
        super().__init__(n_workers)

    @contextmanager
    def executor(self) -> Generator[ClientExecutor, None, None]:
        with worker_client() as client:
            yield client.get_executor()

    def shutdown(self):
        pass
