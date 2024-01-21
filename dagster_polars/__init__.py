from dagster_polars.io_managers.base import BasePolarsUPathIOManager
from dagster_polars.io_managers.parquet import PolarsParquetIOManager
from dagster_polars.types import (
    DataFramePartitions,
    DataFramePartitionsWithMetadata,
    DataFrameWithMetadata,
    LazyFramePartitions,
    LazyFramePartitionsWithMetadata,
    LazyFrameWithMetadata,
    StorageMetadata,
)
from dagster_polars.version import __version__

__all__ = [
    "PolarsParquetIOManager",
    "BasePolarsUPathIOManager",
    "StorageMetadata",
    "DataFrameWithMetadata",
    "LazyFrameWithMetadata",
    "DataFramePartitions",
    "LazyFramePartitions",
    "DataFramePartitionsWithMetadata",
    "LazyFramePartitionsWithMetadata",
    "__version__",
]


try:
    # provided by dagster-polars[delta]
    from dagster_polars.io_managers.delta import DeltaWriteMode, PolarsDeltaIOManager  # noqa

    __all__.extend(["DeltaWriteMode", "PolarsDeltaIOManager"])  # noqa
except ImportError:
    pass


try:
    # provided by dagster-polars[bigquery]
    from dagster_polars.io_managers.bigquery import (
        PolarsBigQueryIOManager,  # noqa
        PolarsBigQueryTypeHandler,  # noqa
    )

    __all__.extend(["PolarsBigQueryIOManager", "PolarsBigQueryTypeHandler"])
except ImportError:
    pass
