<div align="center">
  <a href="https://github.com/citi">
    <img src="https://github.com/citi.png" alt="Citi" width="80" height="80">
  </a>

  <h3 align="center">Citi/parfun</h3>

  <p align="center">
    Lightweight parallelisation library for Python.
  </p>

  <p align="center">
    <a href="https://citi.github.io/parfun/">
      <img src="https://img.shields.io/badge/read%20our%20documentation-0f1632">
    </a>
    <a href="./LICENSE">
      <img src="https://img.shields.io/github/license/citi/parfun?label=license&colorA=0f1632&colorB=255be3">
    </a>
    <a href="https://pypi.org/project/parfun/">
      <img alt="PyPI - Version" src="https://img.shields.io/pypi/v/parfun?colorA=0f1632&colorB=255be3">
    </a>
    <img src="https://api.securityscorecards.dev/projects/github.com/Citi/parfun/badge">
  </p>
</div>

<br />

Parfun is a lightweight library providing helpers to **make it easy to write and run a Python function in parallel
and distributed systems**.

The main feature of the library is its `@parfun` decorator that transparently executes standard Python functions
following the [map-reduce](https://en.wikipedia.org/wiki/MapReduce) pattern:

```Python
from parfun import parfun
from parfun.combine.collection import list_concat
from parfun.partition.api import per_argument
from parfun.partition.collection import list_by_chunk

@parfun(
    split=per_argument(
        values=list_by_chunk
    ),
    combine_with=list_concat,
)
def list_pow(values: List[float], factor: float) -> List[float]:
    return [v**factor for v in values]
```


## Features

* **Provides significant speedups** to existing Python functions
* **Does not require any deep knowledge of parallel or distributed computing systems**
* **Automatically estimates the optimal sub-task splitting** (the *partition size*)
* **Automatically handles data transmission, caching and synchronization**.
* **Supports various distributed computing backends**, including Python's multiprocessing,
  [Scaler](https://github.com/citi/scaler) or Dask.


## Benchmarks

**Parfun efficiently parallelizes short-duration functions**.

When running a short 0.28-second ML function on an AMD Epyc 7313 16-Cores Processor, Parfun provides an impressive
**7.4x speedup**. Source code for this experiment [here](benchmarks/california_housing.py).

![Benchmark Results](benchmarks/california_housing_results.svg)


## Quick Start

The official documentation is availaible at [citi.github.io/parfun/](https://citi.github.io/parfun/).

Alternatively, you can build the HTML documentation from the source code:

```bash
cd docs
pip install -r requirements.txt
make html
```

The documentation's main page can then ben found at `docs/build/html/index.html`.

Take a look at our documentation's [quickstart tutorial](https://citi.github.io/parfun/tutorials/quickstart.html) to get
more examples and a deeper overview of the library.


## Contributing

Your contributions are at the core of making this a true open source project. Any contributions you make are
**greatly appreciated**.

We welcome you to:

- Fix typos or touch up documentation
- Share your opinions on [existing issues](https://github.com/citi/parfun/issues)
- Help expand and improve our library by [opening a new issue](https://github.com/citi/parfun/issues/new)

Please review our [community contribution guidelines](https://github.com/Citi/.github/blob/main/CONTRIBUTING.md) and
[functional contribution guidelines](./CONTRIBUTING.md) to get started 👍.


## Code of Conduct

We are committed to making open source an enjoyable and respectful experience for our community. See
[`CODE_OF_CONDUCT`](https://github.com/Citi/.github/blob/main/CODE_OF_CONDUCT.md) for more information.


## License

This project is distributed under the [Apache-2.0 License](https://www.apache.org/licenses/LICENSE-2.0). See
[`LICENSE`](./LICENSE) for more information.


## Contact

If you have a query or require support with this project, [raise an issue](https://github.com/Citi/parfun/issues).
Otherwise, reach out to [opensource@citi.com](mailto:opensource@citi.com).