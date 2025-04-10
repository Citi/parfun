"""
Uses `all_arguments` to partition all the input data of a parallel function.

Usage:

    $ git clone https://github.com/Citi/parfun && cd parfun
    $ python -m examples.api_usage.all_arguments
"""

import pandas as pd

from parfun import parfun
from parfun.combine.dataframe import df_concat
from parfun.entry_point import set_parallel_backend_context
from parfun.partition.api import all_arguments
from parfun.partition.dataframe import df_by_group


@parfun(
    split=all_arguments(df_by_group(by=["year", "month"])),
    combine_with=df_concat,
)
def monthly_sum(sales: pd.DataFrame, costs: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(sales, costs, on=["year", "month", "day"], how="outer")

    # Group and sum by day
    grouped = merged.groupby(["year", "month", "day"], as_index=False).sum(numeric_only=True)

    return grouped


if __name__ == "__main__":
    sales = pd.DataFrame({
        "year": [2024, 2024, 2024],
        "month": [1, 1, 2],
        "day": [1, 2, 1],
        "sales": [100, 200, 150]
    })

    costs = pd.DataFrame({
        "year": [2024, 2024, 2024],
        "month": [1, 1, 2],
        "day": [1, 2, 1],
        "costs": [50, 70, 80]
    })

    with set_parallel_backend_context("local_multiprocessing"):
        result = monthly_sum(sales, costs)

    print(result)
    #     year  month  day  sales  costs
    # 0  2024      1    1    100     50
    # 1  2024      1    2    200     70
    # 2  2024      2    1    150     80
