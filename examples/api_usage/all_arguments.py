"""
Uses `all_arguments` to partition all the input data of a parallel function.

Usage:

    $ git clone https://github.com/Citi/parfun && cd parfun
    $ python -m examples.api_usage.all_arguments
"""

import pandas as pd

import parfun as pf


@pf.parallel(
    split=pf.all_arguments(pf.dataframe.by_group(by=["year", "month"])),
    combine_with=pf.dataframe.concat,
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

    with pf.set_parallel_backend_context("local_multiprocessing"):
        result = monthly_sum(sales, costs)

    print(result)
    #     year  month  day  sales  costs
    # 0  2024      1    1    100     50
    # 1  2024      1    2    200     70
    # 2  2024      2    1    150     80
