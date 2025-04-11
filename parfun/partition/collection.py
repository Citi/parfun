import warnings

from parfun.collection import by_chunk


warnings.warn(
    "parfun.partition.collection is deprecated and will be removed in a future version, use parfun.collection.",
    DeprecationWarning
)

list_by_chunk = by_chunk

__all__ = ["list_by_chunk"]
