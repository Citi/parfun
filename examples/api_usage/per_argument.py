"""
Uses `per_argument` to partition the input data from multiple arguments.

Usage:

    $ git clone https://github.com/Citi/parfun && cd parfun
    $ python -m examples.api_usage.per_argument
"""

from typing import List

import pandas as pd

import parfun as pf


@pf.parallel(
    split=pf.per_argument(
        factors=pf.py_list.by_chunk,
        dataframe=pf.dataframe.by_row,
    ),
    combine_with=pf.dataframe.concat,
)
def multiply_by_row(factors: List[int], dataframe: pd.DataFrame) -> pd.DataFrame:
    assert len(factors) == len(dataframe)
    return dataframe.multiply(factors, axis=0)


if __name__ == "__main__":
    dataframe = pd.DataFrame({
        "A": [1, 2, 3],
        "B": [4, 5, 6]
    })

    factors = [10, 20, 30]

    with pf.set_parallel_backend_context("local_multiprocessing"):
        result = multiply_by_row(factors, dataframe)

    print(result)
    #     A    B
    # 0  10   40
    # 1  40  100
    # 2  90  180
