import unittest

from parfun.entry_point import set_parallel_backend
from parfun.partition.api import all_arguments, per_argument
from parfun.kernel.parallel_function import ParallelFunction


class TestParallelFunction(unittest.TestCase):
    def setUp(self) -> None:
        set_parallel_backend("local_multiprocessing")

    def test_validate_signature(self):
        # These are valid:

        ParallelFunction(
            function=lambda x, y: x + y,  # type: ignore[misc, arg-type]
            function_name="lambda",
            split=per_argument(x=lambda x: [(x,)]),  # type: ignore[arg-type, return-value]
            combine_with=sum,
        )

        ParallelFunction(
            function=lambda *args, **kwargs: tuple(),  # type: ignore[misc, arg-type]
            function_name="lambda",
            split=all_arguments(lambda x, y: [(x, y)]),  # type: ignore[arg-type, return-value]
            combine_with=sum,
        )


if __name__ == "__main__":
    unittest.main()
