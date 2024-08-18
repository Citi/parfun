from typing import TypeVar

# TODO we can specify and limit their values in future
FunctionInputType = any
FunctionOutputType = any

PartitionType = TypeVar("PartitionType")  # Input and output are identical for partitioning functions.
