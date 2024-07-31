import unittest

from parafun.combine.collection import list_concat, unzip


class TestCombineCollection(unittest.TestCase):
    def test_list_concat(self):
        input_data = [[0, 1, 2, 3], [4, 5, 6], [7], [8, 9]]
        output_data = list_concat(input_data)
        self.assertListEqual(output_data, list(range(0, 10)))

    def test_unzip(self):
        input_data_1 = [1, 2, 3, 4]
        input_data_2 = ["a", "b", "c"]

        output_data_1, output_data_2 = unzip(zip(input_data_1, input_data_2))
        self.assertSequenceEqual(input_data_1[:3], list(output_data_1))
        self.assertSequenceEqual(input_data_2, list(output_data_2))


if __name__ == "__main__":
    unittest.main()
