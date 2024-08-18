We welcome contributions to the Parfun library.


## Helpful Resources

* [README.md](./README.md)

* Documentation: [./docs](./docs) (see [README.md](./README.md) for building instructions)

* [Repository](https://github.com/citi/parfun)

* [Issue tracking](https://github.com/citi/parfun/issues)


## Contributing Guide

When contributing to the project, please take care of following these requirements.


### Style guide

**We enforce the [PEP 8](https://peps.python.org/pep-0008/) coding style, with a relaxed constraint on the maximum line
length (120 columns)**.

Before merging your changes into your `master` branch, our CI system will run the following checks:

```bash
isort --profile black --line-length 120
black -l 120 -C
flake8 --max-line-length 120 --extend-ignore=E203
```

The `isort`, `black` and `flake8` packages can be installed through Python's PIP.


### Bump version number

Before pushing your pull request, please update the version number of the library in the
[about.py](./parfun/about.py) file.

Update the minor version number if your changes are backward-compatible, otherwise update the major version number:

```python
# Original
__version__ = "1.29"

# Non-breaking changes
__version__ = "1.30"

# Breaking changes
__version__ = "2.0"
```


## Code of Conduct

We are committed to making open source an enjoyable and respectful experience for our community. See
[`CODE_OF_CONDUCT`](https://github.com/citi/.github/blob/main/CODE_OF_CONDUCT.md) for more information.