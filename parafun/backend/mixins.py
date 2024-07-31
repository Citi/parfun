import abc
import logging
from concurrent.futures import wait
from contextlib import AbstractContextManager
from typing import Any, Callable, Optional

from parafun.backend.profiled_future import ProfiledFuture


class BackendSession(AbstractContextManager, metaclass=abc.ABCMeta):
    """
    An task submitting session to a backend engine that manages the lifecycle of the task objects (preloaded values,
    argument values and future objects).
    """

    def preload_value(self, value: Any) -> Any:
        """
        Preloads a value to the backend engine.

        The returned value will be used when calling ``submit()`` instead of the original value.
        """
        # By default, does not do anything
        return value

    @abc.abstractmethod
    def submit(self, fn: Callable, *args, **kwargs) -> ProfiledFuture:
        """
        Executes an asynchronous computation.

        **Blocking if no computing resource is available**.
        """

        raise NotImplementedError()


class BackendEngine(metaclass=abc.ABCMeta):
    """
    Asynchronous task manager interface.
    """

    @abc.abstractmethod
    def session(self) -> BackendSession:
        """
        Returns a new managed session for submitting tasks.

        .. code:: python

            with backend.session() as session:
                arg_ref = session.preload_value(arg)

                future = session.submit(fn, arg_ref)

                print(future.result())

        """
        raise NotImplementedError()

    def submit(self, fn: Callable, *args, **kwargs) -> ProfiledFuture:
        logging.warning("`submit()` will be removed in a future version, use `session()` instead.")

        with self.session() as session:
            future = session.submit(fn, *args, **kwargs)
            wait([future])

        return future

    @abc.abstractmethod
    def get_scheduler_address(self) -> Optional[str]:
        raise NotImplementedError()

    @abc.abstractmethod
    def disconnect(self):
        """
        Disconnects from schedulers in backend engine
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def shutdown(self):
        """
        Shutdowns schedulers in backend engine
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def allows_nested_tasks(self) -> bool:
        """
        Indicates if Parafun can submit new tasks from other tasks.
        """
        raise NotImplementedError()
