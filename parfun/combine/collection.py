import warnings

from parfun.collection import concat


warnings.warn(
    "parfun.combine.collection is deprecated and will be removed in a future version, use parfun.collection.",
    DeprecationWarning
)

list_concat = concat

__all__ = ["list_concat"]
