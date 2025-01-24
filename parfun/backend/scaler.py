import inspect
import itertools
import threading
from collections import deque
from threading import BoundedSemaphore
from typing import Any, Deque, Dict, Optional, Set

try:
    from scaler import Client, SchedulerClusterCombo
    from scaler.client.future import ScalerFuture
    from scaler.client.object_reference import ObjectReference
except ImportError:
    raise ImportError("Scaler dependency missing. Use `pip install 'parfun[scaler]'` to install Scaler.")

import psutil

from parfun.backend.mixins import BackendEngine, BackendSession
from parfun.backend.profiled_future import ProfiledFuture
from parfun.backend.utility import get_available_tcp_port
from parfun.profiler.functions import profile


class ScalerSession(BackendSession):
    # Additional constant scheduling overhead that cannot be accounted for when measuring the task execution duration.
    CONSTANT_SCHEDULING_OVERHEAD = 8_000_000  # 8ms

    def __init__(self, client_pool: "ScalerClientPool", n_workers: int):
        self._concurrent_task_guard = BoundedSemaphore(n_workers)
        self._client_poll = client_pool
        self._client = client_pool.acquire()

    def __enter__(self) -> "ScalerSession":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._client.clear()
        self._client_poll.release(self._client)

    def preload_value(self, value: Any) -> ObjectReference:
        return self._client.send_object(value)

    def submit(self, fn, *args, **kwargs) -> Optional[ProfiledFuture]:
        with profile() as submit_duration:
            future = ProfiledFuture()

            acquired = self._concurrent_task_guard.acquire()
            if not acquired:
                return None

            underlying_future = self._client.submit(fn, *args, **kwargs)

        def on_done_callback(underlying_future: ScalerFuture):
            assert submit_duration.value is not None

            if underlying_future.cancelled():
                future.cancel()
                return

            with profile() as release_duration:
                exception = underlying_future.exception()

                if exception is None:
                    result = underlying_future.result()
                    function_duration = int(underlying_future.profiling_info().cpu_time_s * 1_000_000_000)
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


class ScalerClientPool:
    def __init__(self, scheduler_address: str, client_kwargs: Dict, max_unused_clients: int = 1):
        self._scheduler_address = scheduler_address
        self._client_kwargs = client_kwargs
        self._max_unused_clients = max_unused_clients

        self._lock = threading.Lock()
        self._assigned_clients: Dict[bytes, Client] = {}
        self._unassigned_clients: Deque[Client] = deque()  # a FIFO poll of up to `max_unused_clients`.

    def acquire(self) -> Client:
        with self._lock:
            if len(self._unassigned_clients) > 0:
                client = self._unassigned_clients.popleft()
            else:
                client = Client(address=self._scheduler_address, profiling=True, **self._client_kwargs)

            self._assigned_clients[client.identity] = client

            return client

    def release(self, client: Client) -> None:
        with self._lock:
            self._assigned_clients.pop(client.identity)

            if len(self._unassigned_clients) < self._max_unused_clients:
                self._unassigned_clients.append(client)
            else:
                client.disconnect()

    def disconnect_all(self) -> None:
        with self._lock:
            for client in itertools.chain(self._unassigned_clients, self._assigned_clients.values()):
                client.disconnect()

            self._unassigned_clients.clear()
            self._assigned_clients.clear()


class ScalerRemoteBackend(BackendEngine):
    """Connects to a previously instantiated Scaler instance as a backend engine."""

    def __init__(
        self,
        scheduler_address: str,
        n_workers: int = psutil.cpu_count(logical=False) - 1,
        allows_nested_tasks: bool = True,
        **client_kwargs,
    ):
        self.__setstate__(
            {
                "scheduler_address": scheduler_address,
                "n_workers": n_workers,
                "allows_nested_tasks": allows_nested_tasks,
                "client_kwargs": client_kwargs,
            }
        )

    def __getstate__(self) -> dict:
        return {
            "scheduler_address": self._scheduler_address,
            "n_workers": self._n_workers,
            "allows_nested_tasks": self._allows_nested_tasks,
            "client_kwargs": self._client_kwargs,
        }

    def __setstate__(self, state: dict) -> None:
        self._scheduler_address = state["scheduler_address"]
        self._n_workers = state["n_workers"]
        self._allows_nested_tasks = state["allows_nested_tasks"]
        self._client_kwargs = state["client_kwargs"]

        self._client_pool = ScalerClientPool(
            scheduler_address=self._scheduler_address, client_kwargs=self._client_kwargs
        )

    def session(self) -> ScalerSession:
        return ScalerSession(self._client_pool, self._n_workers)

    def get_scheduler_address(self) -> str:
        return self._scheduler_address

    def disconnect(self):
        return self._client_pool.disconnect_all()

    def shutdown(self):
        return self._client_pool.disconnect_all()

    def allows_nested_tasks(self) -> bool:
        return self._allows_nested_tasks


class ScalerLocalBackend(ScalerRemoteBackend):
    """Creates a Scaler cluster on the local machine and uses it as a backend engine."""

    def __init__(
        self,
        per_worker_queue_size: int,
        scheduler_address: Optional[str] = None,
        n_workers: int = psutil.cpu_count(logical=False) - 1,
        allows_nested_tasks: bool = True,
        **kwargs,
    ):
        """
        :param scheduler_address the ``tcp://host:port`` tuple to use as a cluster address. If ``None``, listen to the
        local host on an available TCP port.
        """

        if scheduler_address is None:
            scheduler_port = get_available_tcp_port()
            scheduler_address = f"tcp://127.0.0.1:{scheduler_port}"

        client_kwargs = self.__get_constructor_arg_names(Client)

        super().__init__(
            scheduler_address=scheduler_address,
            allows_nested_tasks=allows_nested_tasks,
            n_workers=n_workers,
            **{kwarg: value for kwarg, value in kwargs.items() if kwarg in client_kwargs},
        )

        scheduler_cluster_combo_kwargs = self.__get_constructor_arg_names(SchedulerClusterCombo)

        self._cluster = SchedulerClusterCombo(
            address=scheduler_address,
            n_workers=n_workers,
            per_worker_queue_size=per_worker_queue_size,
            **{kwarg: value for kwarg, value in kwargs.items() if kwarg in scheduler_cluster_combo_kwargs},
        )

    def __setstate__(self, state: dict) -> None:
        super().__setstate__(state)
        self._cluster = None  # Unserialized instances have no cluster reference.

    @property
    def cluster(self) -> SchedulerClusterCombo:
        if self._cluster is None:
            raise AttributeError("cluster is undefined for serialized instances.")

        return self._cluster

    def shutdown(self):
        super().shutdown()

        if self._cluster is not None:
            self._cluster.shutdown()
            self._cluster = None

    @staticmethod
    def __get_constructor_arg_names(class_: type) -> Set:
        return set(inspect.signature(class_).parameters.keys())
