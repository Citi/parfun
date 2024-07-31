import abc
import enum
from typing import TypeVar

import attrs
from attrs.validators import gt, instance_of


class PartitionSizeEstimatorState(enum.Enum):
    Learning = "learning"
    Running = "running"


@attrs.define
class PartitionSizeEstimate(metaclass=abc.ABCMeta):
    value: int = attrs.field(validator=(instance_of(int), gt(0)))


PartitionSizeEstimateType = TypeVar("PartitionSizeEstimateType", bound=PartitionSizeEstimate)
