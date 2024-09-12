"""
A collection of pre-define APIs to help users partition dataframe data
"""

from typing import Iterable, List, Tuple

try:
    import pandas as pd
except ImportError:
    raise ImportError("Pandas dependency missing. Use `pip install 'parfun[pandas]'` to install Pandas.")

from parfun.partition.object import PartitionFunction, PartitionGenerator


def df_by_row(*dfs: pd.DataFrame) -> PartitionGenerator[Tuple[pd.DataFrame, ...]]:
    """
    Partitions one or multiple Pandas dataframes by rows.

    If multiple dataframes are given, these returned partitions will be of identical number of rows.

    .. code:: python

        df_1 = pd.DataFrame(range(0, 5))
        df_2 = df_1 ** 2

        with_partition_size(df_by_row(df_1, df_2), partition_size=2)

        #  (   0
        #   1  0
        #   2  1,
        #      0
        #   1  0
        #   2  1),
        #  (   0
        #   3  2
        #   4  3,
        #      0
        #   3  4
        #   4  9),
        #  (   0
        #   5  4,
        #       0
        #   5  16)]

    """

    __validate_dfs_parameter(*dfs)

    chunk_size = yield None

    def dfs_chunk(rng_start: int, rng_end: int) -> Tuple[pd.DataFrame, ...]:
        return tuple(df.iloc[rng_start:rng_end] for df in dfs)

    total_size = dfs[0].shape[0]
    range_start = 0
    range_end = chunk_size
    while range_end < total_size:
        chunk_size = yield chunk_size, dfs_chunk(range_start, range_end)

        range_start = range_end
        range_end += chunk_size

    if range_start < total_size:
        yield total_size - range_start, dfs_chunk(range_start, total_size)


def df_by_group(*args, **kwargs) -> PartitionFunction[pd.DataFrame]:
    """
    Partitions one or multiple Pandas dataframes by groups of identical numbers of rows, similar to
    :py:func:`pandas.DataFrame.groupby`.

    See :py:func:`pandas.DataFrame.groupby` for function parameters.

    .. code:: python

        df_1 = pd.DataFrame({"country": ["USA", "China", "Belgium"], "capital": ["Washington", "Beijing", "Brussels"]})
        df_2 = pd.DataFrame({"country": ["USA", "China", "Belgium"], "iso_code": ["US", "CN", "BE"]})

        with_partition_size(df_by_group(by="country")(df_1, df_2), partition_size=1)

        # [(   country   capital
        #   2  Belgium  Brussels,
        #      country iso_code
        #   2  Belgium       BE),
        #  (  country  capital
        #   1   China  Beijing,
        #     country iso_code
        #   1   China       CN),
        #  (  country     capital
        #   0     USA  Washington,
        #     country iso_code
        #   0     USA       US)]

    """

    def generator(*dfs: pd.DataFrame) -> PartitionGenerator[Tuple[pd.DataFrame, ...]]:
        __validate_dfs_parameter(*dfs)

        groups: Iterable[Tuple[pd.DataFrame, ...]] = zip(
            *((group for _name, group in df.groupby(*args, **kwargs)) for df in dfs)
        )

        it = iter(groups)

        chunked_group: Tuple[List[pd.DataFrame], ...] = tuple([] for _ in range(0, len(dfs)))
        chunked_group_size: int = 0

        target_chunk_size = yield None

        def concat_chunked_group_dfs(chunked_group: Tuple[List[pd.DataFrame], ...]):
            return tuple(pd.concat(chunked_dfs) for chunked_dfs in chunked_group)

        while True:
            try:
                group = next(it)
                assert isinstance(group, tuple)
                assert isinstance(group[0], pd.DataFrame)

                group_size = group[0].shape[0]

                if any(group_df.shape[0] != group_size for group_df in group[1:]):
                    raise ValueError("all dataframe group sizes should be identical.")

                chunked_group_size += group_size

                for i, group_df in enumerate(group):
                    chunked_group[i].append(group_df)

                if chunked_group_size >= target_chunk_size:
                    target_chunk_size = yield chunked_group_size, concat_chunked_group_dfs(chunked_group)

                    chunked_group = tuple([] for _ in range(0, len(dfs)))
                    chunked_group_size = 0
            except StopIteration:
                if chunked_group_size > 0:
                    yield chunked_group_size, concat_chunked_group_dfs(chunked_group)

                return

    return generator


def __validate_dfs_parameter(*dfs: pd.DataFrame) -> None:
    if len(dfs) < 1:
        raise ValueError("missing `dfs` parameter.")

    if any(not isinstance(df, pd.DataFrame) for df in dfs):
        raise ValueError("all `dfs` values should be DataFrame instances.")

    total_size = dfs[0].shape[0]
    if any(df.shape[0] != total_size for df in dfs[1:]):
        raise ValueError("all DataFrames should have the same number of rows.")
