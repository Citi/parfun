"""
APIs to manage backends and integrate the toolkit with other projects.
"""

import argparse
import contextlib
import logging
import os
from contextvars import ContextVar, Token
from typing import Callable, Dict, Optional, Union

from parfun.backend.local_multiprocessing import LocalMultiprocessingBackend
from parfun.backend.local_single_process import LocalSingleProcessBackend
from parfun.backend.mixins import BackendEngine

_backend_engine: ContextVar[Optional[BackendEngine]] = ContextVar("_backend_engine", default=None)

BACKEND_REGISTRY: Dict[str, Callable] = {
    "none": lambda *_args, **_kwargs: None,
    "local_single_process": LocalSingleProcessBackend,
    "local_multiprocessing": LocalMultiprocessingBackend,
}

try:
    from parfun.backend.dask import DaskCurrentBackend, DaskLocalClusterBackend, DaskRemoteClusterBackend

    BACKEND_REGISTRY["dask_local"] = DaskLocalClusterBackend
    BACKEND_REGISTRY["dask_remote"] = DaskRemoteClusterBackend
    BACKEND_REGISTRY["dask_current"] = DaskCurrentBackend
except ImportError:
    logging.debug("Dask backends disabled. Use `pip install 'parfun[dask]'` to install Dask dependencies.")

try:
    from parfun.backend.scaler import ScalerLocalBackend, ScalerRemoteBackend

    BACKEND_REGISTRY["scaler_local"] = ScalerLocalBackend
    BACKEND_REGISTRY["scaler_remote"] = ScalerRemoteBackend

except ImportError:
    logging.debug("Scaler backends disabled. Use `pip install 'parfun[scaler]'` to install Scaler dependencies.")


def set_parallel_backend(backend: Union[str, BackendEngine], *args, **kwargs) -> None:
    """
    Initializes and sets the current parfun backend.

    .. code:: python

        set_parallel_backend("local_multiprocessing", max_workers=4, is_process=False)

    :param backend:
        Supported backend options:

        * ``"none"``: disable the current parallel backend.

          Functions decorated with :py:func:`~parfun.decorators.parfun` will run sequentially as if not decorated.

          Partitionning and combining functions will be ignored.

        * ``"local_single_process"``: runs the tasks inside the calling Python process.

          Functions decorated with :py:func:`~parfun.decorators.parfun` will partition the input data, and run the
          combining function on the output data, but will also execute the function inside the calling Python process.

          Mostly intended for debugging purposes.

          See :py:mod:`~parfun.backend.local_single_process.LocalSingleProcessBackend`.

        * ``"local_multiprocessing"``: runs the tasks in parallel using Python ``multiprocessing`` processes.

          See :py:mod:`~parfun.backend.local_multiprocessing.LocalMultiprocessingBackend`.

        * ``"scaler_local"``: runs the tasks in parallel using an internally managed Scaler cluster.

          See :py:mod:`~parfun.backend.scaler.ScalerLocalBackend`.

        * ``"scaler_remote"``: runs the tasks in parallel using an externally managed Dask cluster.

          See :py:mod:`~parfun.backend.scaler.ScalerRemoteBackend`.

        * ``"dask_local"``: runs the tasks in parallel using an internally managed Dask cluster.

          See :py:mod:`~parfun.backend.dask_local.DaskLocalClusterBackend`.

        * ``"dask_remote"``: runs the tasks in parallel using an externally managed Dask cluster.

          See :py:mod:`~parfun.backend.dask_remote.DaskRemoteClusterBackend`.

        * ``"dask_current"``: runs the tasks in parallel using the currently running Dask client
          (:py:func:`~distributed.get_client`).

          See :py:mod:`~parfun.backend.dask_current.DaskCurrentBackend`.

    :type backend:  Union[str, BackendEngine]

    :param args: Additional positional parameters for the backend constructor
    :param kwargs: Additional keyword parameters for the backend constructor.
    :rtype: None
    """
    _set_parallel_backend(backend, *args, **kwargs)


@contextlib.contextmanager
def set_parallel_backend_context(backend: Union[str, BackendEngine], *args, **kwargs):
    """
    Sets a new parallel backend instance in a contextlib's context.

    .. code:: python

        with set_parallel_backend_context("local_single_processing"):
            some_parallel_computation()

    :param backend: See :py:func:`set_parallel_backend`.
    :type backend: Union[str, BackendEngine]
    """
    token = _set_parallel_backend(backend, *args, **kwargs)
    try:
        yield
    finally:
        engine = _backend_engine.get()

        if engine is not None:
            engine.shutdown()

        _backend_engine.reset(token)


def get_parallel_backend() -> Optional[BackendEngine]:
    """
    :return: the current backend instance, or :py:obj:`None` if no backend is currently set.
    :rtype: Optional[BackendEngine]
    """
    return _backend_engine.get()


def add_parallel_options(parser: argparse.ArgumentParser) -> None:
    """
    Adds argparse options required to initialize this parallel toolkit.

    :type parser: argparse.ArgumentParser
    :rtype: None
    """
    group = parser.add_argument_group()
    group.add_argument(
        "--parallel-backend",
        type=str,
        choices=list(BACKEND_REGISTRY.keys()),
        default="local_multiprocessing",
        help="The backend engine selected to run code. If 'none', disables parallel computations.",
    )


def _set_parallel_backend(backend: Union[str, BackendEngine], *args, **kwargs) -> Token:
    if isinstance(backend, BackendEngine):
        if len(args) > 0 or len(kwargs) > 0:
            raise ValueError("Cannot pass additional arguments when passing a backend instance")

        backend_instance = backend
        backend_name = backend.__class__.__name__
    elif backend in BACKEND_REGISTRY:
        backend_instance = BACKEND_REGISTRY[backend](*args, **kwargs)
        backend_name = backend
    else:
        raise ValueError(f"Supported parallel backends are: {set(BACKEND_REGISTRY.keys())}")

    if backend != "none":
        # set numpy OpenBlas threads to be 1 each process only have 1 thread, easier to manage resources
        os.environ["OPENBLAS_NUM_THREADS"] = "1"

    logging.info(f"Set up parallel backend: {backend_name}")

    return _backend_engine.set(backend_instance)
