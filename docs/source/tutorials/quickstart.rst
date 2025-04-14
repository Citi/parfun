Quick start
===========


When to use it
--------------

**Parfun** works well with computations that are CPU-intensive and can be easily divided into *independent sub-tasks*.


.. image:: images/parallel_scatter_gather.png


Here are a few examples of computations that can be easily parallelized:

* ✔ Filtering tasks (e.g. :py:func:`pandas.Dataframe.filter`)
* ✔ Operations that are associative or independent (e.g. matrix addition, sum ...)
* ✔ By-row data processing tasks (e.g. cleaning or normalizing input data).

Other tasks cannot easily be parallelized:

* ✖ Computations on non-partitionable datasets (e.g. median computation, sorting)
* ✖ I/O intensive tasks (file loading, network communications)
* ✖ Very short (< 100ms) tasks.

  These tasks are too small for the parallelism gains to exceed the system overhead caused by our parallelization
  system. Always keep in mind that parallelization is a trade-off between CPU speed and the time needed to transfer data
  between processes.


Setup and backend selection
---------------------------

First, add the ``parfun`` package to your *requirements.txt* file, or install it using PIP:

.. code:: bash

    pip install parfun


The above command will **only install the base package**. If you wish to use a more advanced computing backend, such as
Scaler or Dask, or to enable Pandas' support, use the `scaler`, `dask` and/or `pandas` extras:

.. code:: bash

    pip install "parfun[dask,scaler,pandas]"


The library relies on a registered computing backend to schedule and distribute sub-tasks among multiple worker
processes.

**Before using the library, the user should select the backend instance**. This can either be done process wise
with :py:func:`~parfun.set_parallel_backend` or temporarily using a Python context manager with
:py:func:`~parfun.set_parallel_backend_context`.

.. literalinclude:: ../../../examples/api_usage/backend_setup.py
    :language: python
    :start-at: import parfun as pf

See :py:func:`~parfun.set_parallel_backend` for a description of the available backend options.


Your first parallel function
----------------------------

The following example does basic statistical analysis on a stock portfolio. By using Parfun, these metrics are
calculated in parallel, **splitting the computation by country**.

.. literalinclude:: ../../../examples/portfolio_metrics/main.py
    :language: python
    :start-at: from typing import List

In this example, the :py:func:`~parfun.parallel` **decorator** is configure to *parallelize the execution of*
``relative_metrics()`` *by country*, and then to concat the resulting parallel sub-results:

.. literalinclude:: ../../../examples/portfolio_metrics/main.py
    :language: python
    :start-at: @pf.parallel
    :end-at: def relative_metrics

First, we tell the parallel engine how to partition the data with the ``split`` parameter. Note that we can safely
split the calculation over countries, as the function itself processes these countries independently.

- We use :py:func:`~parfun.per_argument` to tell the parallel engine which arguments to partition on. In
  the example, we will only partition on the ``portfolio`` argument, and we don't touch the ``columns`` argument.

- We use :py:func:`~parfun.dataframe.by_group` to specify that the portfolio dataframe can be partitioned
  over it's ``country`` column.

Finally, the parallel engine needs to know how to *combine the results* of the partitioned calls to
``relative_metrics()``. This is done with the help of the ``combine_with`` parameter. In our example, we just concat the
result dataframes with :py:func:`~parfun.dataframe.concat`.

When executed, the parallel engine with automatically take care of executing the function in parallel, which
would schematically look like this:

.. image:: images/parallel_function.png

This parallelization architecture is a well-known **pattern named map/reduce** or scatter/gather.

Modern computers usually have multiple computing units, or cores. **These cores excel when computing data-independent
tasks**. It's important to specify a partitioning strategy that leverage this.


Partitioning functions
----------------------

As seen in the example above, the ``@parallel`` decorator accepts a partitioning function (``split``).

Previously, we applied a single partitioning function (:py:func:`~parfun.dataframe.by_group`) on a
single argument. However, we could also use :py:func:`~parfun.per_argument` to apply different
partitioning functions on various parameters:

.. literalinclude:: ../../../examples/api_usage/per_argument.py
    :language: python
    :start-at: from typing import List

We are using two partitioning functions, :py:func:`~parfun.py_list.by_chunk` and :py:func:`~parfun.dataframe.by_row`.
These splits the arguments in equally sized partitions. It's semantically equivalent to iterating all these partitioned
arguments simultaneously:


.. code-block:: python

    size = min(len(factors), len(dataframe))
    for begin in range(0, size, PARTITION_SIZE):
        end = min(begin + PARTITION_SIZE, size)
        multiply_by_row(factors[begin:end], dataframe.iloc[begin:end])


Alternatively, it might be sometimes desired to run the same partitioning function on all parameters simultaneously with
:py:func:`~parfun.all_arguments`:

.. literalinclude:: ../../../examples/api_usage/all_arguments.py
    :language: python
    :start-at: import pandas as pd


Combining functions
-------------------

In addition to the partitioning function, the ``@parallel`` decorator requires a combining function (``combine_with``).

The library provides useful partitioning and combining functions to deal with :ref:`Python lists <section-lists>`
and :ref:`Pandas dataframes <section-dataframes>`.


Custom partitioning and combining generators
--------------------------------------------

If you wish to implement more complex partitioning schemes, Parfun allows the use of regular Python generators as
partitioning and combing functions:

.. literalinclude:: ../../../examples/api_usage/custom_generators.py
    :language: python
    :start-at: from typing import Generator, Iterable, Tuple

To work properly, custom partitioning generators should:

1. **use the** ``yield`` **mechanism**, and not return a collection (e.g. a list). Returning a collection instead of
   using a generator will lead to deteriorated performances and higher memory usage.
2. **accept the parameters to partition, and yield these partitioned parameters as a tuple**, in the same order.

When used with ``per_argument``, multiple custom generators can be mixed with pre-defined generators, or with other
customer generators.


Partition size estimate
-----------------------

The library tries to automatically determine the optimal size for the parallelly distributed partitions.

:doc:`Read more </tutorials/implementation_details>` about how the library computes the optimal partition size.

**You can override how the library choose the partition size to use by either providing either the**
``initial_partition_size: int`` **or** ``fixed_partition_size: int`` **parameter:**

.. literalinclude:: ../../../examples/api_usage/partition_size.py
    :language: python
    :start-at: import numpy as np

.. note::

    The partition size estimation is disabled for custom partition generators.

Nested parallel function calls
------------------------------

Parfun functions can be safely called from other Parfun functions.


.. note::
    Currently, Scaler is the only backend that can run the inner functions in parallel. Other backends will execute the
    inner functions sequentially, as regular Python functions.


.. literalinclude:: ../../../examples/api_usage/nested_functions.py
    :language: python
    :start-at: import pprint


Profiling
---------

The easiest way to profile the speedup provided by a parallel function is to either use Python's `timeit` module, or the
IPython/Jupyter ``%timeit`` command.

In addition, **the decorator accepts a** ``profile: bool`` **and a** ``trace_export: Optional[str]`` **parameter** that
can be used get profiling metrics about the execution of a parallel function.

.. literalinclude:: ../../../examples/api_usage/profiling.py
    :language: python
    :start-at: from typing import List

Setting ``profile`` to ``True`` returns a performance summarizing board:

.. code-block:: console

   parallel_sum()
	total CPU execution time: 0:00:00.372508.
	compute time: 0:00:00.347168 (93.20%)
		min.: 0:00:00.001521
		max.: 0:00:00.006217
		avg.: 0:00:00.001973
	total parallel overhead: 0:00:00.025340 (6.80%)
		total partitioning: 0:00:00.024997 (6.71%)
		average partitioning: 0:00:00.000142
		total combining: 0:00:00.000343 (0.09%)
	maximum speedup (theoretical): 14.70x
	total partition count: 176
		estimator state: running
		estimated partition size: 3127


* **total CPU execution time** is the actual CPU time required to execute the parallel function.

  This duration is larger than the value returned by ``%timeit``. That is because **it sums the execution times for all
  the cores that processed our function**. It also include the additional processing required to run Parfun (e.g. for
  partitioning the input and combining the results).

* **compute time** is how much CPU time was spent doing the actual computation.

  This value should roughly match the duration of the original sequential function if measured with ``%timeit``.
  The *min*, *max* and *avg* values tell us that there is some discrepancy in the partitioned execution of our function,
  most probably caused by the possibly uneven workload in the partitioned dataset.

* **total parallel overhead**, **total partitioning** and **total combining** are the overheads related to the parallel
  execution of the function.

  These overheads limit the performance gains achievable through parallelization.

* **maximum speedup (theoretical)** estimates how much faster the function would run on a parallel machine with an
  infinite number of cores.

  This value is calculated based on the input data size, function behavior, and the measured parallel overheads.
* **total partition count** and **current estimated partition size** describes how Parfun is splitting the input data.

  The library uses heuristics to estimate the optimal partition size. The library tries to find a partition size that
  provides significant parallel speedup without causing too much parallel overhead.
  :doc:`Read more </tutorials/implementation_details>` about how the optimal partition size is estimated.


.. note::
    As the library is constantly learning the optimal partition size, the first call to the parallelized function might
    not produce the most optimal run-times. In these cases, it is recommended to call the function multiple times before
    analyzing the profiler output.


When setting the ``trace_export`` parameter, Parfun will dump the latest parallel function call metrics to the provided
CSV file. All durations in this file are in nanoseconds (10E–9).
