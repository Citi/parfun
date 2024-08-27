from typing import Any, TypeVar

# TODO we can specify and limit their values in future
FunctionInputType = Any
FunctionOutputType = Any

PartitionType = TypeVar("PartitionType")  # Input and output are identical for partitioning functions.
