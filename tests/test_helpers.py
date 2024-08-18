from itertools import islice
from typing import Iterator

import numpy as np

try:
    import pandas as pd
except ImportError:
    raise ImportError("Pandas dependency missing. Use `pip install 'parfun[pandas]'` to install Pandas.")


def random_df(rows: int, columns: int, low: int = 0, high: int = 100) -> pd.DataFrame:
    return pd.DataFrame(np.random.randint(low, high, size=(rows, columns)))


def is_prime(x: int) -> bool:
    return all(x % i != 0 for i in range(2, x))


def _gen_primes() -> Iterator[int]:
    i = 1
    while True:
        if is_prime(i):
            yield i
        i += 1


def find_nth_prime(n: int) -> int:
    return next(islice(_gen_primes(), n, n + 1))
