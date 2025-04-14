"""
Counts the most common two-letters sequences (bigrams) in the content of an URL.

Usage:

    $ git clone https://github.com/Citi/parfun && cd parfun
    $ python -m examples.count_bigrams.main
"""

import collections
import psutil
import ssl

from typing import Counter, Iterable, List
from urllib.request import urlopen

import parfun as pf


def sum_counters(counters: Iterable[Counter[str]]) -> Counter[str]:
    return sum(counters, start=collections.Counter())


@pf.parallel(
    split=pf.per_argument(
        lines=pf.py_list.by_chunk
    ),
    combine_with=sum_counters,
)
def count_bigrams(lines: List[str]) -> Counter:
    counter: Counter[str] = collections.Counter()

    for line in lines:
        for word in line.split():
            for first, second in zip(word, word[1:]):
                bigram = f"{first}{second}"
                counter[bigram] += 1

    return counter


if __name__ == "__main__":
    N_WORKERS = psutil.cpu_count(logical=False)
    URL = "https://www.gutenberg.org/ebooks/100.txt.utf-8"
    TOP_K = 10

    with urlopen(URL, context=ssl._create_unverified_context()) as response:
        content = response.read().decode("utf-8").splitlines()

    with pf.set_parallel_backend_context("local_multiprocessing", max_workers=N_WORKERS):
        counts = count_bigrams(content)

    print(f"Top {TOP_K} words:")
    for word, count in counts.most_common(TOP_K):
        print(f"\t{word:<10}:\t{count}")
