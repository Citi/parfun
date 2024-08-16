import abc
from typing import Generic

import attrs

from parfun.partition_size_estimator.object import PartitionSizeEstimateType, PartitionSizeEstimatorState
from parfun.profiler.object import PartitionedTaskTrace


@attrs.define
class PartitionSizeEstimator(Generic[PartitionSizeEstimateType], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def add_partition_trace(self, trace: PartitionedTaskTrace) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def state(self) -> PartitionSizeEstimatorState:
        raise NotImplementedError()

    @abc.abstractmethod
    def estimate(self, dry_run: bool = False) -> PartitionSizeEstimateType:
        raise NotImplementedError()
