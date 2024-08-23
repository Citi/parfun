import math
import unittest
from itertools import chain, repeat
from typing import Generator, List, Tuple, cast

try:
    import pandas as pd
except ImportError:
    raise ImportError("Pandas dependency missing. Use `pip install 'parfun[pandas]'` to install Pandas.")

from parfun.partition.collection import list_by_chunk
from parfun.partition.dataframe import df_by_group, df_by_row
from parfun.partition.object import SimplePartitionIterator
from parfun.partition.primitives import partition_flatmap, partition_map, partition_zip
from parfun.partition.utility import with_partition_size


class TestPartitionPrimitives(unittest.TestCase):
    def test_partition_zip(self):
        N = 10
        PARTITION_SIZE = 3

        n_partitions = math.ceil(N / PARTITION_SIZE)

        xs = list(range(0, N))
        df = pd.DataFrame({"a": xs, "b": xs})

        gen_1 = list_by_chunk
        gen_2 = df_by_row
        gen_3 = cast(SimplePartitionIterator, repeat(math.pi))

        ys = list(with_partition_size(partition_zip(gen_1(xs), gen_2(df), gen_3), partition_size=PARTITION_SIZE))

        self.assertEqual(len(ys), n_partitions)

        for i, partition in enumerate(ys):
            self.assertEqual(len(partition), 3)
            xs_partition = partition[0][0]
            df_partition = partition[1][0]
            pi = partition[2]

            if i + 1 < n_partitions:
                expected_partition_size = PARTITION_SIZE
            else:
                expected_partition_size = N % PARTITION_SIZE

            self.assertEqual(len(xs_partition), expected_partition_size)
            self.assertEqual(len(df_partition), expected_partition_size)

            excepted_values = list(range(i * PARTITION_SIZE, i * PARTITION_SIZE + expected_partition_size))

            self.assertSequenceEqual(xs_partition, excepted_values)
            self.assertListEqual(list(df_partition["a"]), excepted_values)
            self.assertListEqual(list(df_partition["b"]), excepted_values)

            self.assertAlmostEqual(pi, math.pi)

    def test_partition_map(self):
        N = 10
        PARTITION_SIZE = 3

        n_partitions = math.ceil(N / PARTITION_SIZE)

        xs = list(range(0, N))

        def mapped_function(partition: List[int]) -> Tuple[List[int]]:
            return ([x * x for x in partition],)

        # Smart generators

        ys = list(with_partition_size(partition_map(mapped_function, list_by_chunk(xs)), partition_size=PARTITION_SIZE))
        self.assertEqual(len(ys), n_partitions)
        self.assertSequenceEqual(list(chain.from_iterable([y[0] for y in ys])), [x * x for x in xs])

        # Python generators

        def partition_generator(values: List) -> Generator[Tuple[List], None, None]:
            for x in xs:
                yield ([x],)

        ys = list(with_partition_size(partition_map(mapped_function, partition_generator(xs)), partition_size=1))
        self.assertEqual(len(ys), len(xs))
        self.assertSequenceEqual(list(chain.from_iterable([y[0] for y in ys])), [x * x for x in xs])

    def test_partition_flatmap(self):
        N_YEARS = 10
        N_DAYS = 365

        yearly_days = [(year, day) for year in range(2010, 2010 + N_YEARS) for day in range(0, N_DAYS)]
        df = pd.DataFrame({"year": (year for year, _ in yearly_days), "day": (day for _, day in yearly_days)})

        def custom_partition_by_week(df: pd.DataFrame):
            for _, partition in df.groupby(by=df["day"] // 7):
                yield partition,

        # Partition by year group, then by chunks of 7 days.
        partitions = list(
            with_partition_size(
                partition_flatmap(custom_partition_by_week, df_by_group(by="year")(df)), partition_size=7
            )
        )

        self.assertEqual(len(partitions), N_YEARS * math.ceil(N_DAYS / 7))


if __name__ == "__main__":
    unittest.main()
