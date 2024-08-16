import random
import unittest

from parfun.entry_point import set_parallel_backend
from parfun.partition_size_estimator.linear_regression_estimator import LinearRegessionEstimator
from parfun.profiler.object import PartitionedTaskTrace


class TestLinearRegressionEstimator(unittest.TestCase):
    def setUp(self) -> None:
        set_parallel_backend("local_multiprocessing")

    def tearDown(self) -> None:
        set_parallel_backend("none")

    def test_parallelism_efficiency(self):
        estimator = LinearRegessionEstimator()

        a = 43_000.76  # 43 us
        b = 18_700_000.233  # 18.7 ms
        sigma = 50_000  # 50 us

        # Partition size will be determinated by the task's minimal parallelism efficiency
        optimal_partition_size = b / (a * (1 - estimator.min_parallelism_efficiency))

        random_gen = random.Random(1)

        for i in range(0, 1000):
            partition_size = estimator.estimate().value

            if i < estimator.learning_sample_count:
                # Estimator still training
                self.assertIn(partition_size, estimator.learning_sample_sizes)
            else:
                # Estimate within 20% of optimum
                self.assertAlmostEqual(optimal_partition_size, partition_size, delta=optimal_partition_size * 0.20)

            trace = _fake_trace(random_gen, a, b, sigma, partition_size)
            estimator.add_partition_trace(trace)

    def test_task_duration(self):
        estimator = LinearRegessionEstimator()

        a = 22_000.76  # 22 us
        b = 5_000_000  # 5 ms
        sigma = 5_000  # 5 us

        # Partition size will be determinated by the task initialisation/scheduling overhead

        optimal_partition_size = b / (a * (1 - estimator.min_parallelism_efficiency))

        random_gen = random.Random(2)

        for i in range(0, 1000):
            partition_size = estimator.estimate().value

            if i < estimator.learning_sample_count:
                # Estimator still training
                self.assertIn(partition_size, estimator.learning_sample_sizes)
            else:
                # Estimate within 20% of optimum
                self.assertAlmostEqual(optimal_partition_size, partition_size, delta=optimal_partition_size * 0.20)

            trace = _fake_trace(random_gen, a, b, sigma, partition_size)
            estimator.add_partition_trace(trace)


def _fake_trace(
    random_gen: random.Random, a: float, b: float, sigma: float, partition_size: int
) -> PartitionedTaskTrace:
    theoritical_total_duration = a * partition_size + b
    total_duration = round(random_gen.gauss(theoritical_total_duration, sigma))

    return PartitionedTaskTrace(
        None, partition_size, round(total_duration * 0.1), round(total_duration * 0.85), round(total_duration * 0.05)
    )


if __name__ == "__main__":
    unittest.main()
