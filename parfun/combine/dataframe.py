import warnings

from parfun.dataframe import concat


warnings.warn(
    "parfun.combine.dataframe is deprecated and will be removed in a future version, use parfun.dataframe.",
    DeprecationWarning
)

df_concat = concat

__all__ = ["df_concat"]
