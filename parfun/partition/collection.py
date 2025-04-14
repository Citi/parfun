import warnings

from parfun.py_list import by_chunk


warnings.warn(
    "parfun.partition.collection is deprecated and will be removed in a future version, use parfun.py_list.",
    DeprecationWarning
)

list_by_chunk = by_chunk

__all__ = ["list_by_chunk"]
