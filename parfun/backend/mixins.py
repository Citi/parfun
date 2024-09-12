import abc
from contextlib import AbstractContextManager
from typing import Any, Callable

from parfun.backend.profiled_future import ProfiledFuture


class BackendSession(AbstractContextManager, metaclass=abc.ABCMeta):
    """
    A task submitting session to a backend engine that manages the lifecycle of the task objects (preloaded values,
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

    @abc.abstractmethod
    def shutdown(self):
        """
        Shutdowns all resources required by the backend engine.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def allows_nested_tasks(self) -> bool:
        """
        Indicates if Parfun can submit new tasks from other tasks.
        """
        raise NotImplementedError()
