"""
Uses `all_arguments` to partition all the input data of a parallel function.

Usage:

    $ git clone https://github.com/Citi/parfun && cd parfun
    $ python -m examples.api_usage.partition_size
"""

import numpy as np
import pandas as pd

import parfun as pf


# With `fixed_partition_size`, the input dataframe will always be split in chunks of 1000 rows.
@pf.parallel(
    split=pf.all_arguments(pf.dataframe.by_row),
    combine_with=sum,
    fixed_partition_size=1000,
)
def fixed_partition_size_sum(dataframe: pd.DataFrame) -> float:
    return dataframe.values.sum()


# With `initial_partition_size`, the input dataframe will be split in chunks of 1000 rows until Parfun's
# machine-learning algorithm find a better estimate.
@pf.parallel(
    split=pf.all_arguments(pf.dataframe.by_row),
    combine_with=sum,
    initial_partition_size=1000,
)
def initial_partition_size_sum(dataframe: pd.DataFrame) -> float:
    return dataframe.values.sum()


# Both `fixed_partition_size` and `initial_partition_size` can accept a callable instead of an integer value. This
# allows for partition sizes to be computed based on the input parameters.
@pf.parallel(
    split=pf.all_arguments(pf.dataframe.by_row),
    combine_with=sum,
    initial_partition_size=lambda dataframe: max(10, len(dataframe) // 4),
)
def computed_partition_size_sum(dataframe: pd.DataFrame) -> float:
    return dataframe.values.sum()


if __name__ == "__main__":
    dataframe = pd.DataFrame(
        np.random.randint(0, 100, size=(100, 3)),
        columns=["alpha", "beta", "gamma"],
    )

    with pf.set_parallel_backend_context("local_multiprocessing"):
        print(fixed_partition_size_sum(dataframe))

        print(initial_partition_size_sum(dataframe))

        print(computed_partition_size_sum(dataframe))
