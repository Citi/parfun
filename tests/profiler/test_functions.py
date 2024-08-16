import time
import unittest

from parfun.profiler.functions import profile


class TestProfilerFunctions(unittest.TestCase):
    def test_profile(self):
        TEST_DURATION = 10_000_000

        with profile() as duration:
            starts_at = time.process_time_ns()
            while time.process_time_ns() - starts_at < TEST_DURATION:
                pass

            time.sleep(TEST_DURATION / 10**9)  # This should be ignored as non-process time.

        self.assertAlmostEqual(duration.value, TEST_DURATION, delta=0.1 * TEST_DURATION)  # 10% margin.


if __name__ == "__main__":
    unittest.main()
