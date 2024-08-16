import math
import unittest

import pandas as pd

from parfun.kernel.function_signature import NamedArguments
from parfun.partition.api import all_arguments, multiple_arguments, per_argument
from parfun.partition.collection import list_by_chunk
from parfun.partition.dataframe import df_by_group, df_by_row
from parfun.partition.utility import with_partition_size


class TestPartitionAPI(unittest.TestCase):
    def test_per_argument(self):
        N = 100
        PARTITION_SIZE = 2
        N_PARTITIONS = math.ceil(N / PARTITION_SIZE)

        def custom_chunk_generator(values):
            for i in range(0, N_PARTITIONS):
                yield values[i * PARTITION_SIZE : (i + 1) * PARTITION_SIZE],

        partitioning_function = per_argument(values=list_by_chunk, df=df_by_row, custom=custom_chunk_generator)

        xs = [x for x in range(0, N)]
        df = pd.DataFrame({"x^2": [x * x for x in xs]})

        args = NamedArguments(kwargs={"values": xs, "df": df, "custom": xs, "constant": 1})

        non_partitioned_args, partition_generator = partitioning_function(args)

        self.assertEqual(len(non_partitioned_args.keys()), 1)
        self.assertEqual(non_partitioned_args["constant"], 1)

        partitions = list(with_partition_size(partition_generator, partition_size=PARTITION_SIZE))

        self.assertEqual(len(partitions), N_PARTITIONS)

        for i, partition in enumerate(partitions):
            self.assertEqual(len(partition.keys()), 3)

            partition_xs = xs[i * 2 : i * 2 + 2]

            self.assertSequenceEqual(partition["values"], partition_xs)
            self.assertSequenceEqual(list(partition["df"]["x^2"]), [x * x for x in partition_xs])
            self.assertSequenceEqual(partition["custom"], partition_xs)

    def test_multiple_arguments(self):
        partitioning_function = multiple_arguments(("df_1", "df_2"), df_by_group(by="year"))

        df_1 = pd.DataFrame({"year": [2020, 2021, 2020, 2020, 2022], "values": range(0, 5)})

        df_2 = df_1.copy()
        df_2["values"] **= 2

        args = NamedArguments(kwargs={"df_1": df_1, "df_2": df_2, "constant": 2})

        non_partitioned_args, partition_generator = partitioning_function(args)

        self.assertEqual(len(non_partitioned_args.keys()), 1)
        self.assertEqual(non_partitioned_args["constant"], 2)

        partitions = list(with_partition_size(partition_generator, partition_size=1))

        self.assertEqual(len(partitions), df_1["year"].unique().shape[0])

        for partition in partitions:
            self.assertEqual(len(partition.keys()), 2)

            self.assertEqual(partition["df_1"].shape[0], partition["df_2"].shape[0])
            self.assertSequenceEqual(list(partition["df_1"]["values"] ** 2), list(partition["df_2"]["values"]))

    def test_all_arguments(self):
        N = 100
        PARTITION_SIZE = 3
        N_PARTITIONS = math.ceil(N / PARTITION_SIZE)

        partitioning_function = all_arguments(list_by_chunk)

        xs = list(range(0, N))
        ys = [x * x for x in xs]

        args = NamedArguments(kwargs={"xs": xs, "ys": ys})

        non_partitioned_args, partition_generator = partitioning_function(args)

        self.assertEqual(len(non_partitioned_args.keys()), 0)

        partitions = list(with_partition_size(partition_generator, partition_size=PARTITION_SIZE))

        self.assertEqual(len(partitions), N_PARTITIONS)

        for partition in partitions:
            self.assertEqual(len(partition.kwargs), 2)

            self.assertLessEqual(len(partition["xs"]), PARTITION_SIZE)

            self.assertSequenceEqual([x * x for x in partition["xs"]], partition["ys"])


if __name__ == "__main__":
    unittest.main()
