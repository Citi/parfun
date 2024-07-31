import unittest

from parafun.partition.collection import list_by_chunk
from parafun.partition.utility import with_partition_size


class TestPartitionUtility(unittest.TestCase):
    def test_with_partition_size(self):
        ls_1 = range(0, 5)
        ls_2 = range(10, 15)
        values = with_partition_size(list_by_chunk(ls_1, ls_2), partition_size=2)

        self.assertListEqual(list(values), [((0, 1), (10, 11)), ((2, 3), (12, 13)), ((4,), (14,))])


if __name__ == "__main__":
    unittest.main()
