from typing import Union

import fsspec
import polars as pl
import pyarrow.dataset as ds
from dagster import InputContext, OutputContext
from upath import UPath

from dagster_polars.base_io_manager import BasePolarsIOManager


class PolarsParquetIOManager(BasePolarsIOManager):
    extension: str = ".parquet"

    __doc__ = BasePolarsIOManager.__doc__ + """  # type: ignore
        Works with Parquet files
        """

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
