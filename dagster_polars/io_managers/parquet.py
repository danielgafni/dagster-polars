from typing import Union

import fsspec
import polars as pl
import pyarrow.dataset as ds
from dagster import InputContext, OutputContext
from upath import UPath

from dagster_polars.io_managers.base import BasePolarsUPathIOManager


class PolarsParquetIOManager(BasePolarsUPathIOManager):
    extension: str = ".parquet"

    assert BasePolarsUPathIOManager.__doc__ is not None
    __doc__ = (
        BasePolarsUPathIOManager.__doc__
        + """\nWorks with Parquet files.
    All read/write arguments can be passed via corresponding metadata values."""
    )

    def dump_df_to_path(self, context: OutputContext, df: pl.DataFrame, path: UPath):
        assert context.metadata is not None

        with path.open("wb") as file:
            df.write_parquet(
                file,
                compression=context.metadata.get("compression", "zstd"),
                compression_level=context.metadata.get("compression_level"),
                statistics=context.metadata.get("statistics", False),
                row_group_size=context.metadata.get("row_group_size"),
                use_pyarrow=context.metadata.get("use_pyarrow", False),
                pyarrow_options=context.metadata.get("pyarrow_options"),
            )

    def scan_df_from_path(self, path: UPath, context: InputContext) -> pl.LazyFrame:
        assert context.metadata is not None

        fs: Union[fsspec.AbstractFileSystem, None] = None

        try:
            fs = path._accessor._fs
        except AttributeError:
            pass

        return pl.scan_pyarrow_dataset(
            ds.dataset(
                str(path),
                filesystem=fs,
                format=context.metadata.get("format", "parquet"),
                partitioning=context.metadata.get("partitioning"),
                partition_base_dir=context.metadata.get("partition_base_dir"),
                exclude_invalid_files=context.metadata.get("exclude_invalid_files", True),
                ignore_prefixes=context.metadata.get("ignore_prefixes", [".", "_"]),
            ),
            allow_pyarrow_filter=context.metadata.get("allow_pyarrow_filter", True),
        )
