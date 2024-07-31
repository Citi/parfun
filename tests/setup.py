from setuptools import find_packages, setup

from parafun.about import __version__

with open("requirements.txt", "rt") as f:
    requirements = [i.strip() for i in f.readlines()]

setup(
    name="parafun",
    version=__version__,
    packages=find_packages(exclude=("tests",)),
    install_requires=requirements,
    extras_require={"pandas": ["pandas"], "dask": ["dask", "distributed"], "scaler": ["scaler[graphblas]"]},
    url="",
    license="",
    author="Citi",
    author_email="opensource@citi.com",
    description="Parafun makes it easy to distribute the computation of Python functions using a map-reduce cluster.",
)
