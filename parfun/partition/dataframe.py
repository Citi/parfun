
import warnings

from parfun.dataframe import by_group, by_row


warnings.warn(
    "parfun.partition.dataframe is deprecated and will be removed in a future version, use parfun.dataframe.",
    DeprecationWarning
)

df_by_group = by_group

df_by_row = by_row

__all__ = ["df_by_group", "df_by_row"]
