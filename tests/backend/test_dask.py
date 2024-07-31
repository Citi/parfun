import unittest

from parafun.backend.dask import DaskLocalClusterBackend, DaskRemoteClusterBackend
from tests.backend.mixins import BackendEngineTestCase
from tests.backend.utility import warmup_workers


class TestDaskLocalBackend(unittest.TestCase, BackendEngineTestCase):
    N_WORKERS = 4

    def setUp(self) -> None:
        self._backend = DaskLocalClusterBackend(n_workers=TestDaskLocalBackend.N_WORKERS)

        warmup_workers(self.backend(), self.n_workers())

    def tearDown(self) -> None:
        self.backend().shutdown()

    def n_workers(self) -> int:
        return TestDaskLocalBackend.N_WORKERS

    def backend(self) -> DaskLocalClusterBackend:
        return self._backend

    def test_is_backend_dask_remote(self):
        # TestDaskLocalBackend is a special case of DaskRemoteClusterBackend, so no need to test
        # DaskRemoteClusterBackend
        self.assertIsInstance(self._backend, (TestDaskLocalBackend, DaskRemoteClusterBackend))


if __name__ == "__main__":
    unittest.main()
