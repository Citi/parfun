import bisect
import logging
from math import ceil
from typing import Callable, List, Optional, Tuple

import attrs
import numpy as np
from attrs.validators import instance_of, is_callable
from sklearn.base import BaseEstimator
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer

from parfun.entry_point import get_parallel_backend
from parfun.partition_size_estimator.mixins import PartitionSizeEstimator
from parfun.partition_size_estimator.object import PartitionSizeEstimate, PartitionSizeEstimatorState
from parfun.profiler.object import PartitionedTaskTrace


@attrs.define()
class LinearRegressionCoefficients:
    a: float = attrs.field(validator=instance_of(float))
    b: float = attrs.field(validator=instance_of(float))

    score: float = attrs.field(validator=instance_of(float))

    # The number of traces used to train the estimator.
    trace_count: int = attrs.field(validator=instance_of(int))


@attrs.define()
class LinearRegessionEstimate(PartitionSizeEstimate):
    coefficients: Optional[LinearRegressionCoefficients]


@attrs.define()
class LinearRegessionEstimator(PartitionSizeEstimator[LinearRegessionEstimate]):
    """
    Train a linear regression model to estimate the optimal partition size, based on the function's initialization time,
    and the function's processing time.
    """

    # Parallel tasks have some constant computational overhead that stay the same whatever the partition size is (i.e.
    # code loading, preprocessing, input checks, initialisation ...).
    #
    # We would not like the parallel functions to spend too much time on these by selecting a small partition size, as
    # this will waste CPU resources while only providing a negligeable parallel speedup.
    #
    # This is a tradeoff between computation efficiency and parallelisation. The larger this parameter, the less
    # parallel the task will run, but the most efficient the task will be computed.
    min_parallelism_efficiency: float = attrs.field(validator=instance_of(float), default=0.95)

    # Will partially randomly probe the task's execution profile before making chunk size estimates.
    learning_sample_count: int = attrs.field(validator=instance_of(int), default=5)

    # Will circle these partition sizes until the estimator receives `n_learning_samples`.
    learning_sample_sizes: List[int] = attrs.field(init=False, default=[64, 8, 96, 32, 256, 1, 128, 48, 4])
    _current_learning_sample: int = attrs.field(init=False, default=0)

    # Will keep up to `max_traces` before starting to forget previously added traces.
    max_traces: int = attrs.field(validator=instance_of(int), default=100)

    _run_traces: List[Tuple[int, int]] = attrs.field(init=False, factory=list)

    regressor_factory: Callable[[], BaseEstimator] = attrs.field(
        validator=is_callable(), default=lambda: LinearRegessionEstimator.default_regressor()
    )

    _current_coefficients: Optional[LinearRegressionCoefficients] = attrs.field(default=None)
    _current_estimate: Optional[LinearRegessionEstimate] = attrs.field(default=None)

    def add_partition_trace(self, trace: PartitionedTaskTrace) -> None:
        partition_size = trace.partition_size

        tupled_trace = (partition_size, trace.total_duration // partition_size)

        if len(self._run_traces) < self.max_traces:
            self._run_traces.append(tupled_trace)

            if len(self._run_traces) >= self.max_traces:
                # Next trace, we will have to replace one exisiting value. Prepare the lists for bisect() by sorting.
                self._run_traces.sort(key=lambda t: t[0])
        else:
            # Replaces the existing entry with the closest partition size.
            #
            # As the estimator will converge to similar partition size estimates, this will ensure we keep older but
            # valuable traces from the initial learning phase of the estimator.

            left_idx = bisect.bisect_left(self._run_traces, tupled_trace)
            right_idx = left_idx + 1

            if left_idx <= 0:
                self._run_traces[0] = tupled_trace
            elif right_idx >= len(self._run_traces):
                self._run_traces[-1] = tupled_trace
            else:
                # Replaces the closest value when the value fall between two existing values.
                left_partition_size = self._run_traces[left_idx][0]
                right_partition_size = self._run_traces[right_idx][0]

                if partition_size - left_partition_size < right_partition_size - partition_size:
                    self._run_traces[left_idx] = tupled_trace
                else:
                    self._run_traces[right_idx] = tupled_trace

                assert self._run_traces[left_idx][0] <= self._run_traces[right_idx][0]

        self._current_estimate = None
        self._current_coefficients = None

    def state(self) -> PartitionSizeEstimatorState:
        if len(self._run_traces) < self.learning_sample_count:
            return PartitionSizeEstimatorState.Learning
        else:
            return PartitionSizeEstimatorState.Running

    def coefficients(self) -> LinearRegressionCoefficients:
        """Trains a linear regression ´f(partition_size) = a + b / partition_size´ on the previously recorded task runs.

        This pretty accurately estimates the time it takes to process a single item (i.e. row) when feeding a dataset of
        a given partition size. The behavior of parallel functions is that the larger the partition size, the less
        function initialization overhead (`b`) will be weighted when compared to the actual processing time of that
        single item (`a`)."""

        if self._current_coefficients is not None:
            return self._current_coefficients

        regressor = self.regressor_factory()

        numpy_traces = np.array(self._run_traces)
        regressor.fit(numpy_traces[:, 0:1], numpy_traces[:, 1])

        linear_regressor = dict(regressor.steps)["linear"]
        a = linear_regressor.intercept_
        b = linear_regressor.coef_[0]

        score = regressor.score(numpy_traces[:, 0:1], numpy_traces[:, 1])

        self._current_coefficients = LinearRegressionCoefficients(a, b, score, len(self._run_traces))

        return self._current_coefficients

    def estimate(self, dry_run: bool = False) -> LinearRegessionEstimate:
        if self._current_estimate is not None:
            return self._current_estimate

        if self.state() == PartitionSizeEstimatorState.Learning:
            return self._learn_estimate(dry_run=dry_run)

        # Knowing f()'s coefficients, we can accuratly compute when the parallel overheads become negligeable when
        # compared to the actual computation time (`min_parallelism_efficiency`).

        coefficients = self.coefficients()
        a = coefficients.a
        b = coefficients.b
        if b <= 0 or a < 0:
            # TODO: we could use more advanced heurestics, like the error value of the regressor.
            if len(self._run_traces) >= self.max_traces:
                logging.debug("failed to estimate a valid partition size, fallback to learning.")

            return self._learn_estimate(dry_run=dry_run)

        current_backend = get_parallel_backend()

        if current_backend is None:
            raise ValueError("partition size estimator requires a contextual parallel backend instance.")

        # Solves the partition size that satisfies `min_parallelism_efficiency`.
        partition_size = ceil(b / (a * (1 - self.min_parallelism_efficiency)))

        self._current_estimate = LinearRegessionEstimate(partition_size, coefficients)
        return self._current_estimate

    def _learn_estimate(self, dry_run: bool = False) -> LinearRegessionEstimate:
        """Learning estimate. Probes the task execution times before running the actual estimator."""

        partition_size = self.learning_sample_sizes[self._current_learning_sample]

        if not dry_run:
            self._current_learning_sample += 1
            self._current_learning_sample %= len(self.learning_sample_sizes)

        return LinearRegessionEstimate(partition_size, None)

    @staticmethod
    def default_regressor() -> BaseEstimator:
        return Pipeline(
            steps=[("inv", FunctionTransformer(func=lambda xs: 1.0 / xs)), ("linear", LinearRegression(positive=True))]
        )
