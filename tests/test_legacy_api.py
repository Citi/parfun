import unittest
from typing import List

try:
    import pandas as pd
except ImportError:
    raise ImportError("Pandas dependency missing. Use `pip install 'parfun[pandas]'` to install Pandas.")

from parfun import parfun
from parfun.combine.dataframe import df_concat
from parfun.entry_point import get_parallel_backend, set_parallel_backend, set_parallel_backend_context
from parfun.backend.local_multiprocessing import LocalMultiprocessingBackend
from parfun.backend.local_single_process import LocalSingleProcessBackend
from parfun.partition.api import all_arguments, per_argument
from parfun.partition.collection import list_by_chunk
from parfun.partition.dataframe import df_by_row
from tests.backend.utility import warmup_workers


class TestLegacyAPI(unittest.TestCase):
    N_WORKERS = 4

    def setUp(self) -> None:
        set_parallel_backend("local_multiprocessing", max_workers=TestLegacyAPI.N_WORKERS)
        warmup_workers(get_parallel_backend(), TestLegacyAPI.N_WORKERS)

    def test_collections(self):
        input_data = list(range(0, 100))

        result = parallel_sum(input_data)
        self.assertEqual(result, sum(input_data))

    def test_dataframes(self):
        dataframe = pd.DataFrame({
            "values": list(range(0, 100)),
        })
        factor = 4

        result = dataframe_multiply(dataframe, factor)
        self.assertTrue(result["values"].equals(dataframe["values"] * factor))

    def test_backend_context(self):
        self.assertIsInstance(get_parallel_backend(), LocalMultiprocessingBackend)

        with set_parallel_backend_context("local_single_process"):
            self.assertIsInstance(get_parallel_backend(), LocalSingleProcessBackend)


@parfun(
    split=all_arguments(list_by_chunk),
    combine_with=sum,
)
def parallel_sum(values: List) -> List:
    return sum(values)


@parfun(
    split=per_argument(dataframe=df_by_row),
    combine_with=df_concat,
)
def dataframe_multiply(dataframe: pd.DataFrame, factor: float) -> pd.DataFrame:
    return dataframe * factor


if __name__ == "__main__":
    unittest.main()
