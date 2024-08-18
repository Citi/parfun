import unittest

try:
    import pandas as pd
except ImportError:
    raise ImportError("Pandas dependency missing. Use `pip install 'parfun[pandas]'` to install Pandas.")

from parfun.partition.collection import list_by_chunk
from parfun.partition.dataframe import df_by_group
from parfun.partition.nested import partition_nested
from parfun.partition.utility import with_partition_size


class TestPartitionNested(unittest.TestCase):
    def test_partition_nested(self):
        arg_1 = list(range(2010, 2014))
        arg_2 = pd.DataFrame({"industry": [1, 2, 1, 3, 1, 2], "score": [212.1, 331.1, 18.2, 98.2, 23.1, 12.3]})
        arg_3 = ["USA", "Belgium", "China", "Poland"]

        def custom_partition_generator(df: pd.DataFrame):
            for row in range(0, df.shape[0]):
                yield df.iloc[row : row + 1],

        partition_function = partition_nested(
            (("arg_1", "arg_3"), list_by_chunk),
            ("arg_2", df_by_group(by="industry")),
            ("arg_2", custom_partition_generator),
        )

        partitions = list(
            with_partition_size(partition_function(arg_1=arg_1, arg_2=arg_2, arg_3=arg_3), partition_size=1)
        )

        n_partitions = len(arg_1) * arg_2.shape[0]
        self.assertEqual(len(partitions), n_partitions)

        i = 0
        for partition_arg_1, partition_arg_3 in zip(arg_1, arg_3):
            for _, partition_arg_2 in arg_2.groupby(by="industry"):
                for i_row_arg_2 in range(0, len(partition_arg_2)):
                    row_arg_2 = partition_arg_2.iloc[i_row_arg_2 : i_row_arg_2 + 1]

                    partition = partitions[i]

                    self.assertSequenceEqual(partition["arg_1"], [partition_arg_1])
                    self.assertTrue(partition["arg_2"].equals(row_arg_2))
                    self.assertSequenceEqual(partition["arg_3"], [partition_arg_3])

                    i += 1


if __name__ == "__main__":
    unittest.main()
