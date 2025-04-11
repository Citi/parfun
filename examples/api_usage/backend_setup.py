"""
Shows the two ways of initializing a Parfun backend.

Usage:

    $ git clone https://github.com/Citi/parfun && cd parfun
    $ python -m examples.api_usage.backend_setup
"""

from parfun import set_parallel_backend, set_parallel_backend_context


if __name__ == "__main__":
    # Set the parallel backend process-wise.
    set_parallel_backend("local_multiprocessing")

    # Set the parallel backend with a Python context.
    with set_parallel_backend_context("scaler_remote", scheduler_address="tcp://scaler.cluster:1243"):
        ...  # Will run the parallel tasks over a remotely setup Scaler cluster.
