"""
Shows how a Parfun function can be called from within another Parfun function.

Usage:

    $ git clone https://github.com/Citi/parfun && cd parfun
    $ python -m examples.api_usage.nested_functions
"""

import pprint
import random
from typing import List

import parfun as pf


@pf.parallel(
    split=pf.all_arguments(pf.py_list.by_chunk),
    combine_with=pf.py_list.concat,
)
def add_vectors(vec_a: List, vec_b: List) -> List:
    """Add two vectors, element-wise."""
    return [a + b for a, b in zip(vec_a, vec_b)]


@pf.parallel(
    split=pf.all_arguments(pf.py_list.by_chunk),
    combine_with=pf.py_list.concat,
)
def add_matrices(mat_a: List[List], mat_b: List[List]) -> List[List]:
    """Add two matrices, row by row."""
    return [add_vectors(vec_a, vec_b) for vec_a, vec_b in zip(mat_a, mat_b)]


if __name__ == "__main__":
    N_ROWS, N_COLS = 10, 10

    mat_a = [[random.randint(0, 99) for _ in range(0, N_COLS)] for _ in range(0, N_ROWS)]
    mat_b = [[random.randint(0, 99) for _ in range(0, N_COLS)] for _ in range(0, N_ROWS)]

    print("A =")
    pprint.pprint(mat_a)

    print("B =")
    pprint.pprint(mat_b)

    with pf.set_parallel_backend_context("local_multiprocessing"):
        result = add_matrices(mat_a, mat_b)

    print("A + B =")
    pprint.pprint(result)
