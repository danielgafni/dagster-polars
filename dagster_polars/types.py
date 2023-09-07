import sys
from typing import Any, Dict, Tuple

if sys.version < "3.10":
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

import polars as pl

StorageMetadata: TypeAlias = Dict[str, Any]
DataFramePartitions: TypeAlias = Dict[str, pl.DataFrame]
DataFramePartitionsWithMetadata: TypeAlias = Dict[str, Tuple[pl.DataFrame, StorageMetadata]]
LazyFramePartitions: TypeAlias = Dict[str, pl.LazyFrame]
LazyFramePartitionsWithMetadata: TypeAlias = Dict[str, Tuple[pl.LazyFrame, StorageMetadata]]

__all__ = [
    "StorageMetadata",
    "DataFramePartitions",
    "DataFramePartitionsWithMetadata",
    "LazyFramePartitions",
    "LazyFramePartitionsWithMetadata",
]
