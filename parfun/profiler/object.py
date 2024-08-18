from typing import List, Optional

import attrs
from attrs.validators import gt, instance_of, optional

from parfun.partition_size_estimator.object import PartitionSizeEstimate

TraceTime = int  # Process time in nanosecond used for profiling


@attrs.define
class ProfileDuration:
    """See :py:func:`parfun.profiler.functions.profile`."""

    value: Optional[TraceTime] = attrs.field(default=None)


@attrs.define
class PartitionedTaskTrace:
    """The profiling traces for a single partitioned task (i.e. a function call with a single partitioned dataset)."""

    partition_size_estimate: Optional[PartitionSizeEstimate] = attrs.field(
        validator=optional(instance_of(PartitionSizeEstimate))
    )

    partition_size: int = attrs.field(validator=(instance_of(int), gt(0)))
    partition_duration: TraceTime = attrs.field(validator=instance_of(TraceTime))

    task_duration: Optional[TraceTime] = attrs.field(validator=optional(instance_of(TraceTime)), default=None)
    combine_duration: Optional[TraceTime] = attrs.field(validator=optional(instance_of(TraceTime)), default=None)

    @property
    def total_duration(self) -> TraceTime:
        if self.task_duration is None or self.combine_duration is None:
            raise ValueError("`task_duration` and `combine_duration` should be initialized.")

        return self.partition_duration + self.task_duration + self.combine_duration


@attrs.define
class TaskTrace:
    """The profiling traces for a partitioned job (i.e. a `ParallelFunction` call)."""

    partition_traces: List[PartitionedTaskTrace] = attrs.field(validator=instance_of(list), factory=list)

    @property
    def partition_count(self) -> int:
        return len(self.partition_traces)

    @property
    def total_partition_duration(self) -> TraceTime:
        return sum((t.partition_duration for t in self.partition_traces), 0)

    @property
    def total_task_duration(self) -> TraceTime:
        """Returns the total CPU time required to schedule and compute the function."""

        if any(t.task_duration is None for t in self.partition_traces):
            raise ValueError("`function_duration` values shall be initialized.")

        return sum((t.task_duration for t in self.partition_traces), 0)

    @property
    def total_combine_duration(self) -> TraceTime:
        if any(t.combine_duration is None for t in self.partition_traces):
            raise ValueError("`combine_duration` values shall be initialized.")

        return sum((t.combine_duration for t in self.partition_traces), 0)
