"""
A collection of pre-define APIs to help users combine Pandas' Dataframe data
"""

import logging
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


def dfs_concat(dfs: Iterable[pd.DataFrame]) -> pd.DataFrame:
    logging.warning(f"`{dfs_concat.__name__}` will be removed in a future version, use `{df_concat.__name__}` instead.")

    return df_concat(dfs)


def concat_list_of_dfs(df_list: Iterable[pd.DataFrame]) -> pd.DataFrame:
    logging.warning(
        f"`{concat_list_of_dfs.__name__}` will be removed in a future version, use `{df_concat.__name__}` instead."
    )

    return df_concat(df_list)
