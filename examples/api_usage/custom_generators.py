"""
Shows how to use custom Python generators and functions as partitioning and combining functions.

Usage:

    $ git clone https://github.com/Citi/parfun && cd parfun
    $ python -m examples.api_usage.custom_generators
"""

from typing import Generator, Iterable, Tuple

import pandas as pd

import parfun as pf


def partition_by_day_of_week(dataframe: pd.DataFrame) -> Generator[Tuple[pd.DataFrame], None, None]:
    """Divides the computation on the "datetime" value, by day of the week (Monday, Tuesday ...)."""

    for _, partition in dataframe.groupby(dataframe["datetime"].dt.day_of_week):
        yield partition,  # Should always yield a tuple that matches the input parameters.


def combine_results(dataframes: Iterable[pd.DataFrame]) -> pd.DataFrame:
    """Collects the results by concatenating them, and make sure the values are kept sorted by date."""
    return pd.concat(dataframes).sort_values(by="datetime")


@pf.parallel(
    split=pf.all_arguments(partition_by_day_of_week),
    combine_with=combine_results,
)
def daily_mean(dataframe: pd.DataFrame) -> pd.DataFrame:
    return dataframe.groupby(dataframe["datetime"].dt.date).mean(numeric_only=True)


if __name__ == "__main__":
    dataframe = pd.DataFrame({
        # Probing times
        "datetime": pd.to_datetime([
            "2025-04-01 06:00", "2025-04-01 18:00", "2025-04-02 10:00", "2025-04-03 14:00", "2025-04-03 23:00",
            "2025-04-04 08:00", "2025-04-05 12:00", "2025-04-06 07:00", "2025-04-06 20:00", "2025-04-07 09:00",
            "2025-04-08 15:00", "2025-04-09 11:00", "2025-04-10 13:00", "2025-04-11 06:00", "2025-04-12 16:00",
            "2025-04-13 17:00", "2025-04-14 22:00", "2025-04-15 10:00", "2025-04-16 09:00", "2025-04-17 13:00",
            "2025-04-18 14:00", "2025-04-19 18:00", "2025-04-20 07:00", "2025-04-21 20:00", "2025-04-22 15:00",
        ]),
        # Temperature values (°C)
        "temperature": [
            7.2, 10.1, 9.8, 12.5, 11.7,
            8.9, 13.0, 7.5, 10.8, 9.3,
            12.1, 11.5, 13.3, 6.8, 12.7,
            13.5, 9.2, 10.0, 9.9, 11.8,
            12.4, 10.6, 7.9, 9.5, 11.6,
        ],
        # Humidity values (%)
        "humidity": [
            85, 78, 80, 75, 76,
            88, 73, 89, 77, 84,
            72, 74, 70, 90, 71,
            69, 86, 81, 83, 76,
            74, 79, 87, 82, 73,
        ]
    })

    with pf.set_parallel_backend_context("local_multiprocessing"):
        result = daily_mean(dataframe)

    print(result)
