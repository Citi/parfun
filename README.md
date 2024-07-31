<div align="center">
  <a href="https://github.com/citi">
    <img src="https://github.com/citi.png" alt="Citi" width="80" height="80">
  </a>

  <h3 align="center">Citi/parafun</h3>

  <p align="center">
    Lightweight parallelisation library for Python.
  </p>

  <p align="center">
    <a href="./LICENSE">
        <img src="https://img.shields.io/github/license/citi/parafun?label=license&colorA=0f1632&colorB=255be3">
    </a>
  </p>
</div>

<br />

Parafun is a lightweight library providing helpers to **make it easy to write and run a Python function in parallel
and distributed systems**.

The main feature of the library is its `@parafun` decorator that transparently executes standard Python functions
following the [map-reduce](https://en.wikipedia.org/wiki/MapReduce) pattern:

```Python
from parafun import parafun
from parafun.combine.collection import list_concat
from parafun.partition.api import per_argument
from parafun.partition.collection import list_by_chunk

@parafun(
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


## Quick Start
The built-in Sphinx documentation contains detailed usage instructions, implementation details, and an exhaustive
API reference.

Use the `doc` Make target to build the HTML documentation from the source code:

```bash
make doc
```

The documentation's main page can then ben found at `docs/build/html/index.html`.

Take a look at our documentation's [quickstart tutorial](./docs/build/html/tutorials/quickstart.html) to get more
examples and a deeper overview of the library.


## Contributing

Your contributions are at the core of making this a true open source project. Any contributions you make are
**greatly appreciated**.

We welcome you to:

- Fix typos or touch up documentation
- Share your opinions on [existing issues](https://github.com/citi/parafun/issues)
- Help expand and improve our library by [opening a new issue](https://github.com/citi/parafun/issues/new)

Please review our [community contribution guidelines](https://github.com/Citi/.github/blob/main/CONTRIBUTING.md) and
[functional contribution guidelines](./CONTRIBUTING.md) to get started 👍.


## Code of Conduct

We are committed to making open source an enjoyable and respectful experience for our community. See
[`CODE_OF_CONDUCT`](https://github.com/Citi/.github/blob/main/CODE_OF_CONDUCT.md) for more information.


## License

This project is distributed under the [Apache-2.0 License](https://www.apache.org/licenses/LICENSE-2.0). See
[`LICENSE`](./LICENSE) for more information.


## Contact

If you have a query or require support with this project, [raise an issue](https://github.com/Citi/parafun/issues).
Otherwise, reach out to [opensource@citi.com](mailto:opensource@citi.com).