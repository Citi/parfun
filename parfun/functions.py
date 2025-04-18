import collections
import logging
from typing import Any, Callable, Deque, Iterable, Optional, Tuple

from parfun.backend.mixins import BackendSession, ProfiledFuture
from parfun.entry_point import get_parallel_backend


def parallel_map(func: Callable, *iterables, backend_session: Optional[BackendSession] = None) -> Iterable:
    """
    Similar to :py:func:`concurrent.futures.Executor.map()` but lazily consumes and returns the iterators' content as
    worker nodes become available.

    .. code:: python

        parallel_map(math.sqrt, [4, 9, 16, 25])  # [2.0, 3.0, 4.0, 5.0]

        parallel_map(operator.add, [10, 7, 15], [12, 15, 5])  # [22, 22, 20]


    :param backend_session: the parallel backend session. If `None`, creates a new session from the current backend.
    """

    # Uses a generator function, so that we can use deque.pop() and thus discard the no longer required futures'
    # references as we yield them.
    def result_generator(backend_session: BackendSession):
        futures: Deque[ProfiledFuture] = collections.deque()
        try:
            for args in zip(*iterables):
                futures.append(backend_session.submit(func, *args))

                # Yields any finished future from the head of the queue.
                while len(futures) > 0 and futures[0].done():
                    yield futures.popleft().result()

            # Yields the remaining results.
            while len(futures) > 0:
                yield futures.popleft().result()
        finally:
            # If any failure, cancels all unfinished tasks.
            for future in futures:
                future.cancel()

    if backend_session is None:
        current_backend = get_parallel_backend()

        if current_backend is None:
            logging.warning(f"no parallel backend engine set, run `{func.__name__}()` sequentially.")
            return map(func, *iterables)

        with current_backend.session() as current_backend_session:
            return result_generator(current_backend_session)
    else:
        return result_generator(backend_session)


def parallel_starmap(
    func: Callable,
    iterable: Iterable[Tuple[Any, ...]],
    backend_session: Optional[BackendSession] = None
) -> Iterable:
    """
    Similar to :py:func:`concurrent.futures.Executor.starmap()` but lazily consumes and returns the iterators' content
    as worker nodes become available.

    .. code:: python

        parallel_starmap(operator.add, [(10, 12), (7, 15), (15, 5)])  # [22, 22, 20]

    """
    yield from parallel_map(func, *zip(*iterable), backend_session=backend_session)
