"""
Uses `per_argument` to partition the input data from multiple arguments.

Usage:

    $ git clone https://github.com/Citi/parfun && cd parfun
    $ python -m examples.api_usage.per_argument
"""

from typing import List

import pandas as pd

from parfun import parfun
from parfun.entry_point import set_parallel_backend_context

from parfun.partition.api import per_argument
from parfun.combine.dataframe import df_concat
from parfun.partition.collection import list_by_chunk
from parfun.partition.dataframe import df_by_row


@parfun(
    split=per_argument(
        factors=list_by_chunk,
        dataframe=df_by_row,
    ),
    combine_with=df_concat,
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

    with set_parallel_backend_context("local_multiprocessing"):
        result = multiply_by_row(factors, dataframe)

    print(result)
    #     A    B
    # 0  10   40
    # 1  40  100
    # 2  90  180
