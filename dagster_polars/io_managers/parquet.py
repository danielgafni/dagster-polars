from typing import Union

import fsspec
import polars as pl
import pyarrow.dataset as ds
from dagster import InitResourceContext, InputContext, OutputContext, io_manager
from upath import UPath

from dagster_polars.io_managers.base import BasePolarsUPathIOManager


class PolarsParquetIOManager(BasePolarsUPathIOManager):
    extension: str = ".parquet"

    __doc__ = BasePolarsUPathIOManager.__doc__ + """\nWorks with Parquet files"""  # type: ignore

    def dump_df_to_path(self, context: OutputContext, df: pl.DataFrame, path: UPath):
        with path.open("wb") as file:
            df.write_parquet(file)

    def scan_df_from_path(self, path: UPath, context: InputContext) -> pl.LazyFrame:
        fs: Union[fsspec.AbstractFileSystem, None] = None

        try:
            fs = path._accessor._fs
        except AttributeError:
            pass

        return pl.scan_pyarrow_dataset(ds.dataset(str(path), filesystem=fs))


# old non-pythonic IOManager, you are encouraged to use the `PolarsParquetIOManager` instead
@io_manager(
    config_schema=PolarsParquetIOManager.to_config_schema(),
    description=PolarsParquetIOManager.__doc__,
)
def polars_parquet_io_manager(context: InitResourceContext):
    return PolarsParquetIOManager.from_resource_context(context)
