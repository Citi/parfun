"""
Based on a portfolio of stocks, computes basic statistics.

Usage:

    $ git clone https://github.com/Citi/parfun && cd parfun
    $ python -m examples.portfolio_metrics.main
"""

from typing import List

import pandas as pd

import parfun as pf


@pf.parallel(
    split=pf.per_argument(portfolio=pf.dataframe.by_group(by="country")),
    combine_with=pf.dataframe.concat,
)
def relative_metrics(portfolio: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Computes relative metrics (difference to mean, median ...) of a dataframe, for each of the requested dataframe's
    values, grouped by country.
    """

    output = portfolio.copy()

    for country in output["country"].unique():
        for column in columns:
            values = output.loc[output["country"] == country, column]

            mean = values.mean()
            std = values.std()

            output.loc[output["country"] == country, f"{column}_diff_to_mean"] = values - mean
            output.loc[output["country"] == country, f"{column}_sq_diff_to_mean"] = (values - mean) ** 2
            output.loc[output["country"] == country, f"{column}_relative_to_mean"] = (values - mean) / std

    return output


if __name__ == "__main__":
    portfolio = pd.DataFrame({
        "company": ["Apple", "ASML", "Volkswagen", "Citigroup", "Tencent"],
        "industry": ["technology", "technology", "manufacturing", "banking", "manufacturing"],
        "country": ["US", "NL", "DE", "US", "CN"],
        "market_cap": [2828000000000, 236000000000, 55550000000, 80310000000, 345000000000],
        "revenue": [397000000000, 27180000000, 312000000000, 79840000000, 79000000000],
        "workforce": [161000, 39850, 650951, 240000, 104503]
    })

    with pf.set_parallel_backend_context("local_multiprocessing"):
        metrics = relative_metrics(portfolio, ["market_cap", "revenue"])

    print(metrics)
