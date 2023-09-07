import sys
from typing import Any, Dict, Tuple, TypeAlias

import polars as pl

StorageMetadata: TypeAlias = Dict[str, Any]
DataFramePartitions: TypeAlias = Dict[str, pl.DataFrame]
DataFramePartitionsWithMetadata: TypeAlias = Dict[str, Tuple[pl.DataFrame, StorageMetadata]]
LazyFramePartitions: TypeAlias = Dict[str, pl.LazyFrame]
LazyFramePartitionsWithMetadata: TypeAlias = Dict[str, Tuple[pl.LazyFrame, StorageMetadata]]

if sys.version >= "3.9":
    StorageMetadata = dict[str, Any]  # type: ignore
    DataFramePartitions = dict[str, pl.DataFrame]  # type: ignore
    LazyFramePartitions = dict[str, pl.LazyFrame]  # type: ignore
    DataFramePartitionsWithMetadata = dict[str, tuple[pl.DataFrame, StorageMetadata]]  # type: ignore
    LazyFramePartitionsWithMetadata = dict[str, tuple[pl.LazyFrame, StorageMetadata]]  # type: ignore
