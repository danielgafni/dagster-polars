from enum import Enum
from pprint import pformat
from typing import Dict, Optional, Union

import polars as pl
from dagster import InputContext, MetadataValue, OutputContext
from deltalake import DeltaTable
from upath import UPath

from dagster_polars.io_managers.base import BasePolarsUPathIOManager


class DeltaWriteMode(str, Enum):
    error = "error"
    append = "append"
    overwrite = "overwrite"
    ignore = "ignore"


class PolarsDeltaIOManager(BasePolarsUPathIOManager):
    extension: str = ".delta"
    mode: DeltaWriteMode = DeltaWriteMode.overwrite.value  # type: ignore
    overwrite_schema: bool = False
    version: Optional[int] = None

    assert BasePolarsUPathIOManager.__doc__ is not None
    __doc__ = (
        BasePolarsUPathIOManager.__doc__
        + """\nWorks with Delta files.
    All read/write arguments can be passed via corresponding metadata values.
    Metadata values take precedence over config parameters."""  # TODO: should this be opposite?
    )

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
            mode=context.metadata.get("mode", self.mode),  # type: ignore
            overwrite_schema=context.metadata.get("overwrite_schema", self.overwrite_schema),
            storage_options=storage_options,
            delta_write_options=delta_write_options,
        )
        table = DeltaTable(str(path), storage_options=storage_options)
        context.add_output_metadata({"version": table.version()})

    def scan_df_from_path(self, path: UPath, context: InputContext) -> pl.LazyFrame:
        assert context.metadata is not None

        return pl.scan_delta(
            str(path),
            version=context.metadata.get("version") or self.version or None,
            delta_table_options=context.metadata.get("delta_table_options"),
            pyarrow_options=context.metadata.get("pyarrow_options"),
            storage_options=self.get_storage_options(path),
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

    def get_metadata(self, context: OutputContext, obj: pl.DataFrame) -> Dict[str, MetadataValue]:
        assert context.metadata is not None

        metadata = super().get_metadata(context, obj)

        if context.has_asset_partitions:
            partition_by = context.metadata.get("partition_by")
            if partition_by is not None:
                metadata["partition_by"] = partition_by

        if context.metadata.get("mode") == "append":
            # modify the medatata to reflect the fact that we are appending to the table

            if context.has_asset_partitions:
                # paths = self._get_paths_for_partitions(context)
                # assert len(paths) == 1
                # path = list(paths.values())[0]

                # FIXME: what to about row_count metadata do if we are appending to a partitioned table?
                # we should not be using the full table length,
                # but it's unclear how to get the length of the partition we are appending to
                pass
            else:
                metadata["append_row_count"] = metadata["row_count"]

                path = self._get_path(context)
                # we need to get row_count from the full table
                metadata["row_count"] = MetadataValue.int(
                    DeltaTable(str(path), storage_options=self.get_storage_options(path))
                    .to_pyarrow_dataset()
                    .count_rows()
                )

        return metadata
