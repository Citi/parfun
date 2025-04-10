"""
Demonstrates the use of the `profile` and `trace_export` parameters for profiling Parfun's performances.

Usage:

    $ git clone https://github.com/Citi/parfun && cd parfun
    $ python -m examples.api_usage.profiling
"""

from typing import List
import random

from parfun import parfun
from parfun.entry_point import set_parallel_backend_context
from parfun.partition.api import all_arguments
from parfun.partition.collection import list_by_chunk


@parfun(
    split=all_arguments(list_by_chunk),
    combine_with=sum,
    profile=True,
    trace_export="parallel_sum_trace.csv",
)
def parallel_sum(values: List) -> List:
    return sum(values)


if __name__ == "__main__":
    N_VALUES = 100_000
    values = [random.randint(0, 99) for _ in range(0, N_VALUES)]

    with set_parallel_backend_context("local_multiprocessing"):
        print("Sum =", parallel_sum(values))
