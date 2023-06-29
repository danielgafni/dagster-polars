import polars as pl
from dagster import InputContext, OutputContext
from deltalake import DeltaTable
from upath import UPath

from dagster_polars.io_managers.base import BasePolarsUPathIOManager


class PolarsDeltaIOManager(BasePolarsUPathIOManager):
    extension: str = ".delta"
    overwrite_schema: bool = False

    __doc__ = BasePolarsUPathIOManager.__doc__ + """\nWorks with Delta files"""  # type: ignore

    def dump_df_to_path(self, context: OutputContext, df: pl.DataFrame, path: UPath):
        assert context.metadata is not None
        mode = context.metadata.get("mode")
        storage_options = self.get_storage_options(path)
        df.write_delta(
            str(path),
            mode=mode,  # type: ignore
            overwrite_schema=self.overwrite_schema,
            storage_options=storage_options,
        )
        table = DeltaTable(str(path), storage_options=storage_options)
        context.add_output_metadata({"version": int(table.version)})  # type: ignore

    def scan_df_from_path(self, path: UPath, context: InputContext) -> pl.LazyFrame:
        storage_options = {}

        try:
            storage_options.update(path._kwargs.copy())
        except AttributeError:
            pass

        return pl.scan_delta(str(path), storage_options=self.get_storage_options(path))

    @staticmethod
    def get_storage_options(path: UPath) -> dict:
        storage_options = {}

        try:
            storage_options.update(path._kwargs.copy())
        except AttributeError:
            pass

        return storage_options
