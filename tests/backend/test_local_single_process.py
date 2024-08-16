import unittest

from parfun.backend.local_single_process import LocalSingleProcessBackend
from tests.backend.mixins import BackendEngineTestCase


class TestLocalSingleProcess(unittest.TestCase, BackendEngineTestCase):
    def setUp(self) -> None:
        self._backend = LocalSingleProcessBackend()

    def n_workers(self) -> int:
        return 0

    def backend(self) -> LocalSingleProcessBackend:
        return self._backend


if __name__ == "__main__":
    unittest.main()
