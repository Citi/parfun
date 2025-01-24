import unittest

from parfun.backend.mixins import BackendEngine

try:
    from parfun.backend.scaler import ScalerLocalBackend, ScalerRemoteBackend

    scaler_installed = True
except ImportError:
    scaler_installed = False

from tests.backend.mixins import BackendEngineTestCase
from tests.backend.utility import warmup_workers


@unittest.skipUnless(scaler_installed, "Scaler backend not installed")
class TestScalerBackend(unittest.TestCase, BackendEngineTestCase):
    N_WORKERS = 4

    def setUp(self) -> None:
        self._backend = ScalerLocalBackend(n_workers=TestScalerBackend.N_WORKERS, per_worker_queue_size=1)

        warmup_workers(self._backend, self.n_workers())

    def tearDown(self) -> None:
        self.backend().shutdown()

    def n_workers(self) -> int:
        return TestScalerBackend.N_WORKERS

    def backend(self) -> BackendEngine:
        return self._backend

    def test_is_backend_scaler_remote(self):
        # ScalerLocalBackend is a special case of ScalerRemoteBackend, so no need to test ScalerRemoteBackend
        self.assertIsInstance(self._backend, (ScalerRemoteBackend, ScalerLocalBackend))


if __name__ == "__main__":
    unittest.main()
