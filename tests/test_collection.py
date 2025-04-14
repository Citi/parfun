import unittest

import parfun as pf


class TestCollection(unittest.TestCase):
    def test_concat(self):
        input_data = [[0, 1, 2, 3], [4, 5, 6], [7], [8, 9]]
        output_data = pf.py_list.concat(input_data)
        self.assertListEqual(output_data, list(range(0, 10)))


if __name__ == "__main__":
    unittest.main()
