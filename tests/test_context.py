import unittest

import parfun as pf
from parfun.backend.local_multiprocessing import LocalMultiprocessingBackend
from parfun.backend.local_single_process import LocalSingleProcessBackend


class TestContext(unittest.TestCase):
    def test_nested_context(self):
        self.assertIsNone(pf.get_parallel_backend())

        with pf.set_parallel_backend_context("local_multiprocessing"):
            self.assertIsInstance(pf.get_parallel_backend(), LocalMultiprocessingBackend)

            with pf.set_parallel_backend_context("local_single_process"):
                self.assertIsInstance(pf.get_parallel_backend(), LocalSingleProcessBackend)

            self.assertIsInstance(pf.get_parallel_backend(), LocalMultiprocessingBackend)
        self.assertIsNone(pf.get_parallel_backend())


if __name__ == "__main__":
    unittest.main()
