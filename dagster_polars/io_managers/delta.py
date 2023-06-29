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

    def dump_df_to_path(self, context: OutputContext, df: pl.DataFrame, path: UPath):
        assert context.metadata is not None

        storage_options = self.get_storage_options(path)
        df.write_delta(
            str(path),
            mode=context.metadata.get("mode", "overwrite"),  # type: ignore
            overwrite_schema=context.metadata.get("overwrite_schema", False),
            storage_options=storage_options,
            delta_write_options=context.metadata.get("delta_write_options"),
        )
        table = DeltaTable(str(path), storage_options=storage_options)
        context.add_output_metadata({"version": table.version()})

    def scan_df_from_path(self, path: UPath, context: InputContext) -> pl.LazyFrame:
        assert context.metadata is not None

        storage_options = {}

        try:
            storage_options.update(path._kwargs.copy())
        except AttributeError:
            pass

        return pl.scan_delta(
            str(path),
            version=context.metadata.get("version"),
            delta_table_options=context.metadata.get("delta_table_options"),
            pyarrow_options=context.metadata.get("pyarrow_options"),
            storage_options=self.get_storage_options(path),
        )
