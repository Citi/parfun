import warnings

from parfun.py_list import concat


warnings.warn(
    "parfun.combine.collection is deprecated and will be removed in a future version, use parfun.py_list.",
    DeprecationWarning
)

list_concat = concat

__all__ = ["list_concat"]
