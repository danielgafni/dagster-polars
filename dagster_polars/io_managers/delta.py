from pprint import pformat
from typing import Union

import polars as pl
from dagster import InputContext, OutputContext
from deltalake import DeltaTable
from upath import UPath

from dagster_polars.io_managers.base import BasePolarsUPathIOManager


class PolarsDeltaIOManager(BasePolarsUPathIOManager):
    extension: str = ".delta"

    assert BasePolarsUPathIOManager.__doc__ is not None
    __doc__ = (
        BasePolarsUPathIOManager.__doc__
        + """\nWorks with Delta files.
    All read/write arguments can be passed via corresponding metadata values."""
    )

    def get_path_for_partition(self, context: Union[InputContext, OutputContext], path: UPath, partition: str) -> UPath:
        if isinstance(context, InputContext):
            if (
                context.upstream_output is not None
                and context.upstream_output.metadata is not None
                and context.upstream_output.metadata.get("partition_by") is not None
            ):
                # upstream asset has "partition_by" metadata set, so partitioning for it is handled by DeltaLake itself
                return path

        if isinstance(context, OutputContext):
            if context.metadata is not None and context.metadata.get("partition_by") is not None:
                # this asset has "partition_by" metadata set, so partitioning for it is handled by DeltaLake itself
                return path

        return path / partition  # partitioning is handled by the IOManager

    def dump_df_to_path(self, context: OutputContext, df: pl.DataFrame, path: UPath):
        assert context.metadata is not None

        delta_write_options = context.metadata.get("delta_write_options")

        if context.has_asset_partitions:
            delta_write_options = delta_write_options or {}
            partition_by = context.metadata.get("partition_by")

            if partition_by is not None:
                delta_write_options["partition_by"] = partition_by

        if delta_write_options is not None:
            context.log.debug(f"Writing with delta_write_options: {pformat(delta_write_options)}")

        storage_options = self.get_storage_options(path)

        df.write_delta(
            str(path),
            mode=context.metadata.get("mode", "overwrite"),  # type: ignore
            overwrite_schema=context.metadata.get("overwrite_schema", False),
            storage_options=storage_options,
            delta_write_options=delta_write_options,
        )
        table = DeltaTable(str(path), storage_options=storage_options)
        context.add_output_metadata({"version": table.version()})

    def scan_df_from_path(self, path: UPath, context: InputContext) -> pl.LazyFrame:
        assert context.metadata is not None

        return pl.scan_delta(
            str(path),
            version=context.metadata.get("version"),
            delta_table_options=context.metadata.get("delta_table_options"),
            pyarrow_options=context.metadata.get("pyarrow_options"),
            storage_options=self.get_storage_options(path),
        )
