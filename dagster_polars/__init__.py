from typing import TYPE_CHECKING

import lazy_import

from dagster_polars._version import __version__
from dagster_polars.types import (
    DataFramePartitions,
    DataFramePartitionsWithMetadata,
    DataFrameWithMetadata,
    LazyFramePartitions,
    LazyFramePartitionsWithMetadata,
    LazyFrameWithMetadata,
    StorageMetadata,
)

lazy_import.lazy_callable("dagster_polars.io_managers.parquet.BigQueryPolarsIOManager")
lazy_import.lazy_callable("dagster_polars.io_managers.delta.PolarsDeltaIOManager")
lazy_import.lazy_callable("dagster_polars.io_managers.delta.PolarsDeltaIOManager")
lazy_import.lazy_callable("dagster_polars.io_managers.bigquery.BigQueryPolarsIOManager")


if TYPE_CHECKING:
    # imports for type checking only
    from dagster_polars.io_managers.base import BasePolarsUPathIOManager
    from dagster_polars.io_managers.bigquery import BigQueryPolarsIOManager
    from dagster_polars.io_managers.delta import DeltaWriteMode, PolarsDeltaIOManager
    from dagster_polars.io_managers.parquet import PolarsParquetIOManager


__all__ = [
    "BasePolarsUPathIOManager",
    "PolarsParquetIOManager",
    "PolarsDeltaIOManager",
    "DeltaWriteMode",
    "BigQueryPolarsIOManager",
    "StorageMetadata",
    "DataFrameWithMetadata",
    "LazyFrameWithMetadata",
    "DataFramePartitions",
    "LazyFramePartitions",
    "DataFramePartitionsWithMetadata",
    "LazyFramePartitionsWithMetadata",
    "__version__",
]
