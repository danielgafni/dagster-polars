from typing import Union

import fsspec
import polars as pl
import pyarrow.dataset as ds
from dagster import InputContext, OutputContext
from upath import UPath

from dagster_polars.io_managers.base import BasePolarsUPathIOManager


class PolarsCSVIOManager(BasePolarsUPathIOManager):
    extension: str = ".csv"

    assert BasePolarsUPathIOManager.__doc__ is not None
    __doc__ = (
        BasePolarsUPathIOManager.__doc__
        + """\nWorks with CSV files.
    All read/write arguments can be passed via corresponding metadata values."""
    )

    def dump_df_to_path(self, context: OutputContext, df: pl.DataFrame, path: UPath):
        assert context.metadata is not None

        with path.open("wb") as file:
            df.write_csv(
                file,
                has_header=context.metadata.get("has_header", True),
                separator=context.metadata.get("separator", ","),
                quote=context.metadata.get("quote", '"'),
                batch_size=context.metadata.get("batch_size", 1024),
                datetime_format=context.metadata.get("datetime_format"),
                date_format=context.metadata.get("date_format"),
                time_format=context.metadata.get("time_format"),
                float_precision=context.metadata.get("float_precision"),
                null_value=context.metadata.get("null_value"),
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
                source=str(path),
                filesystem=fs,
                format=context.metadata.get("format", "csv"),
                partitioning=context.metadata.get("partitioning"),
                partition_base_dir=context.metadata.get("partition_base_dir"),
                exclude_invalid_files=context.metadata.get("exclude_invalid_files", True),
                ignore_prefixes=context.metadata.get("ignore_prefixes", [".", "_"]),
            ),
            allow_pyarrow_filter=context.metadata.get("allow_pyarrow_filter", True),
        )
