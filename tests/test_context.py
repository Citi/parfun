import unittest

from parfun.backend.local_multiprocessing import LocalMultiprocessingBackend
from parfun.backend.local_single_process import LocalSingleProcessBackend
from parfun.entry_point import get_parallel_backend, set_parallel_backend_context


class TestContext(unittest.TestCase):
    def test_nested_context(self):
        self.assertIsNone(get_parallel_backend())

        with set_parallel_backend_context("local_multiprocessing"):
            self.assertIsInstance(get_parallel_backend(), LocalMultiprocessingBackend)

            with set_parallel_backend_context("local_single_process"):
                self.assertIsInstance(get_parallel_backend(), LocalSingleProcessBackend)

            self.assertIsInstance(get_parallel_backend(), LocalMultiprocessingBackend)
        self.assertIsNone(get_parallel_backend())


if __name__ == "__main__":
    unittest.main()
