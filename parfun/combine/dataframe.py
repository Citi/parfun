"""
A collection of pre-define APIs to help users combine Pandas' Dataframe data
"""

from typing import Iterable

try:
    import pandas as pd
except ImportError:
    raise ImportError("Pandas dependency missing. Use `pip install 'parfun[pandas]'` to install Pandas.")


def df_concat(dfs: Iterable[pd.DataFrame]) -> pd.DataFrame:
    """
    Similar to :py:func:`pandas.concat`.

    .. code:: python

        df_1 = pd.DataFrame([1,2,3])
        df_2 = pd.DataFrame([4,5,6])

        print(df_concat([df_1, df_2]))
        #    0
        # 0  1
        # 1  2
        # 2  3
        # 3  4
        # 4  5
        # 5  6

    """

    return pd.concat(dfs, ignore_index=True)
