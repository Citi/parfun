from parfun.decorators import parallel, parfun
from parfun.entry_point import get_parallel_backend, set_parallel_backend, set_parallel_backend_context
from parfun.partition.api import all_arguments, multiple_arguments, per_argument

from .about import __version__

assert isinstance(__version__, str)

__all__ = (
    "__version__",
    "parallel", "parfun",
    "get_parallel_backend", "set_parallel_backend", "set_parallel_backend_context",
    "all_arguments", "multiple_arguments", "per_argument",
)
