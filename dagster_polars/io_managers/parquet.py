import json
from typing import Union

import fsspec
import polars as pl
import pyarrow.dataset as ds
import pyarrow.parquet
from dagster import InputContext, OutputContext
from upath import UPath

from dagster_polars.constants import DAGSTER_POLARS_STORAGE_METADATA_KEY
from dagster_polars.io_managers.base import BasePolarsUPathIOManager
from dagster_polars.types import StorageMetadata


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

    def save_metadata_to_path(self, path: UPath, context: OutputContext, metadata: StorageMetadata):
        context.log.debug(f"Writing Parquet metadata to {path}...")
        self.write_parquet_metadata(path, metadata)

    def load_metadata_from_path(self, path: UPath, context: InputContext) -> StorageMetadata:
        context.log.debug(f"Reading Parquet metadata from {path}...")
        return self.read_parquet_metadata(path)

    @classmethod
    def write_parquet_metadata(cls, path: UPath, metadata: StorageMetadata):
        existing_table = pyarrow.parquet.read_table(str(path), filesystem=path.fs if hasattr(path, "fs") else None)
        existing_metadata = (
            existing_table.schema.metadata.to_dict() if existing_table.schema.metadata is not None else {}
        )
        existing_metadata.update({DAGSTER_POLARS_STORAGE_METADATA_KEY: json.dumps(metadata)})
        existing_table = existing_table.replace_schema_metadata(existing_metadata)
        pyarrow.parquet.write_metadata(
            existing_table.schema, str(path), filesystem=path.fs if hasattr(path, "fs") else None
        )

    @classmethod
    def read_parquet_metadata(cls, path: UPath) -> StorageMetadata:
        metadata = pyarrow.parquet.read_metadata(
            str(path), filesystem=path.fs if hasattr(path, "fs") else None
        ).metadata

        dagster_polars_metadata = (
            metadata.get(DAGSTER_POLARS_STORAGE_METADATA_KEY.encode("utf-8")) if metadata is not None else None
        )

        return json.loads(dagster_polars_metadata) if dagster_polars_metadata is not None else {}
