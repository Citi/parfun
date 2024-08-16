import unittest

from parfun.backend.local_multiprocessing import LocalMultiprocessingBackend
from tests.backend.mixins import BackendEngineTestCase
from tests.backend.utility import warmup_workers


class TestLocalMultiprocessingBackend(unittest.TestCase, BackendEngineTestCase):
    N_WORKERS = 4

    def setUp(self):
        self._backend = LocalMultiprocessingBackend(max_workers=TestLocalMultiprocessingBackend.N_WORKERS)
        warmup_workers(self.backend(), TestLocalMultiprocessingBackend.N_WORKERS)

    def tearDown(self) -> None:
        return self._backend.shutdown()

    def n_workers(self) -> int:
        return TestLocalMultiprocessingBackend.N_WORKERS

    def backend(self) -> LocalMultiprocessingBackend:
        return self._backend


if __name__ == "__main__":
    unittest.main()
