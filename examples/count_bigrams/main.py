"""
This example counts the most common two-letters sequences (bigrams) in the content of an URL.

Usage:

    $ python -m examples.count_bigrams.main
"""

import argparse
import collections
import json
import psutil

from typing import Counter, Iterable, List
from urllib.request import urlopen

from parfun import parfun
from parfun.entry_point import BACKEND_REGISTRY, set_parallel_backend_context
from parfun.partition.api import per_argument
from parfun.partition.collection import list_by_chunk


def sum_counters(counters: Iterable[Counter[str]]) -> Counter[str]:
    return sum(counters, start=collections.Counter())


@parfun(
    split=per_argument(
        lines=list_by_chunk
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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "url",
        action="store",
        type=str,
        nargs="?",
        default="https://www.gutenberg.org/ebooks/100.txt.utf-8",
    )

    parser.add_argument("--n_workers", action="store", type=int, default=psutil.cpu_count(logical=False))
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--backend", type=str, choices=BACKEND_REGISTRY.keys(), default="local_multiprocessing")
    parser.add_argument("--backend_args", type=str, default="{}")

    args = parser.parse_args()

    with urlopen(args.url) as response:
        content = response.read().decode("utf-8").splitlines()

    backend_args = {"max_workers": args.n_workers, **json.loads(args.backend_args)}
    with set_parallel_backend_context(args.backend, **backend_args):
        counts = count_bigrams(content)

    print(f"Top {args.top_k} words:")
    for word, count in counts.most_common(args.top_k):
        print(f"\t{word:<10}:\t{count}")
