import abc

from parafun.backend.mixins import BackendEngine
from tests.backend.utility import (
    is_backend_blocking,
    is_backend_handling_exceptions,
    is_backend_providing_speedup,
    is_backend_supporting_nested_tasks,
    is_task_duration_correct,
)


class BackendEngineTestCase(metaclass=abc.ABCMeta):
    """
    Validates the requirements of the ``BackendEngine`` interface.
    """

    # Remark: the class cannot be an instance of unittest.TestCase as the library will try to instance it and execute
    # it as a test case.

    @abc.abstractmethod
    def backend(self) -> BackendEngine:
        raise NotImplementedError()

    @abc.abstractmethod
    def n_workers(self) -> int:
        raise NotImplementedError()

    def test_is_blocking(self):
        is_backend_blocking(self, self.backend(), max_concurrency=self.n_workers())

    def test_is_backend_handling_exceptions(self):
        is_backend_handling_exceptions(self, self.backend())

    def test_is_backend_providing_speedup(self):
        is_backend_providing_speedup(self, self.backend(), n_workers=self.n_workers())

    def test_task_duration(self):
        is_task_duration_correct(self, self.backend(), concurrency=self.n_workers())

    def test_supports_nested_tasks(self):
        is_backend_supporting_nested_tasks(self, self.backend())
