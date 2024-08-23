import math
import unittest
from typing import List, cast

try:
    import pandas as pd
except ImportError:
    raise ImportError("Pandas dependency missing. Use `pip install 'parfun[pandas]'` to install Pandas.")

from parfun.partition.dataframe import df_by_group, df_by_row
from parfun.partition.object import SmartPartitionGenerator
from parfun.partition.utility import with_partition_size
from tests.test_helpers import random_df


class TestPartitionDataframe(unittest.TestCase):
    def test_df_by_row(self):
        def test_with_params(input_dfs: List[pd.DataFrame], partition_size):
            n_rows = input_dfs[0].shape[0]

            partitions = list(with_partition_size(df_by_row(*input_dfs), partition_size=partition_size))

            self.assertEqual(len(partitions), math.ceil(n_rows / partition_size))

            # Validates the partition sizes.
            for partition_dfs in partitions:
                self.assertEqual(len(partition_dfs), len(input_dfs))

                for partition_df, input_df in zip(partition_dfs, input_dfs):
                    self.assertEqual(partition_df.shape[1], input_df.shape[1])
                    self.assertTrue(partition_df.columns.equals(input_df.columns))

                    self.assertLessEqual(partition_df.shape[0], partition_size)

            # Validates the partition values
            for input_df_i, input_df in enumerate(input_dfs):
                output_df = pd.concat((partition[input_df_i] for partition in partitions))
                self.assertTrue(input_df.equals(output_df))

        test_with_params([random_df(rows=1, columns=1)], partition_size=1)
        test_with_params([random_df(rows=1, columns=13)], partition_size=1)
        test_with_params([random_df(rows=13, columns=23), random_df(rows=13, columns=3)], partition_size=3)
        test_with_params(
            [pd.DataFrame({"a": list("hello"), "b": list("world")}), random_df(rows=5, columns=3)], partition_size=2
        )

        with self.assertRaises(ValueError):
            test_with_params([random_df(rows=10, columns=23), random_df(rows=6, columns=3)], partition_size=5)

    def test_df_by_group(self):
        input_df = pd.DataFrame({"category": ["a", "a", "b", "a", "c", "c"], "values": [1, 2, 3, 4, 5, 6]})

        # Tests if the generator correctly groups the input dataframe.

        output_df = pd.concat(df for df, in with_partition_size(df_by_group(by="category")(input_df)))

        self.assertTrue(input_df.sort_values("category").equals(output_df))

        # Tests if the generator dynamically adapts to varying chunk size.

        gen = cast(SmartPartitionGenerator, df_by_group(by="category")(input_df))
        next(gen)

        partition_size, chunk = gen.send(1)
        self.assertEqual(partition_size, 3)
        self.assertTrue(input_df[input_df["category"] == "a"].equals(chunk[0]))

        partition_size, chunk = gen.send(10)
        self.assertEqual(partition_size, 3)
        self.assertTrue(input_df[(input_df["category"] == "b") | (input_df["category"] == "c")].equals(chunk[0]))

        with self.assertRaises(StopIteration):
            gen.send(10)

        # Tests if the generator works with multiple dataframes.

        input_df_2 = pd.DataFrame({"category": input_df["category"], "values_2": input_df["values"] * 2})

        output_dfs = list(
            with_partition_size(df_by_group(by="category")(input_df, input_df_2), partition_size=input_df.shape[0])
        )[0]

        # , fixed_chunk_size=input_df.shape[0], initial_chunk_size=None
        self.assertTrue(input_df.sort_values("category").equals(output_dfs[0]))
        self.assertTrue(input_df_2.sort_values("category").equals(output_dfs[1]))


if __name__ == "__main__":
    unittest.main()
