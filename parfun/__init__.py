from parfun.decorators import parfun

from .about import __version__

assert isinstance(__version__, str)

__all__ = ("__version__", "parfun")
