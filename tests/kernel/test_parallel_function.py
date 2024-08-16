import unittest

from parfun.entry_point import set_parallel_backend
from parfun.kernel.parallel_function import ParallelFunction


class TestParallelFunction(unittest.TestCase):
    def setUp(self) -> None:
        set_parallel_backend("local_multiprocessing")

    def test_validate_signature(self):
        # These are valid:

        ParallelFunction(
            function=lambda x, y: x + y, partition_on=("x",), partition_with=lambda x: [(x,)], combine_with=sum
        )

        ParallelFunction(
            function=lambda *args, **kwargs: tuple(),
            partition_on=("x", "y"),
            partition_with=lambda x, y: [(x, y)],
            combine_with=sum,
        )

        with self.assertRaises(ValueError):
            ParallelFunction(
                function=lambda x, y: x + y, partition_on=(), partition_with=lambda: [()], combine_with=sum
            )

        with self.assertRaises(ValueError):
            ParallelFunction(
                function=lambda x, y: x + y,
                partition_on=["x", "z"],
                partition_with=lambda x, z: [(x, z)],
                combine_with=sum,
            )


if __name__ == "__main__":
    unittest.main()
