import lazy_import  # noqa

lazy_import.lazy_callable("dagster_polars.io_managers.parquet.BigQueryPolarsIOManager")  # noqa
lazy_import.lazy_callable("dagster_polars.io_managers.delta.PolarsDeltaIOManager")  # noqa
lazy_import.lazy_callable("dagster_polars.io_managers.delta.PolarsDeltaIOManager")  # noqa
lazy_import.lazy_callable("dagster_polars.io_managers.bigquery.BigQueryPolarsIOManager")  # noqa

from dagster_polars._version import __version__  # noqa
from dagster_polars.io_managers.base import BasePolarsUPathIOManager  # noqa
from dagster_polars.io_managers.bigquery import BigQueryPolarsIOManager  # noqa
from dagster_polars.io_managers.delta import DeltaWriteMode, PolarsDeltaIOManager  # noqa
from dagster_polars.io_managers.parquet import PolarsParquetIOManager  # noqa
from dagster_polars.types import (  # noqa
    DataFramePartitions,
    DataFramePartitionsWithMetadata,
    DataFrameWithMetadata,
    LazyFramePartitions,
    LazyFramePartitionsWithMetadata,
    LazyFrameWithMetadata,
    StorageMetadata,
)

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
