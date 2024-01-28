import json
from typing import TYPE_CHECKING, Any, Literal, Optional, Union, overload

import polars as pl
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from dagster import InputContext, OutputContext
from dagster._annotations import experimental
from fsspec.implementations.local import LocalFileSystem
from packaging.version import Version
from pyarrow import Table

from dagster_polars.io_managers.base import BasePolarsUPathIOManager
from dagster_polars.types import LazyFrameWithMetadata, StorageMetadata

if TYPE_CHECKING:
    from upath import UPath


DAGSTER_POLARS_STORAGE_METADATA_KEY = "dagster_polars_metadata"


def get_pyarrow_dataset(path: "UPath", context: InputContext) -> ds.Dataset:
    context_metadata = context.metadata or {}

    fs = path.fs if hasattr(path, "fs") else None

    if context_metadata.get("partitioning") is not None:
        context.log.warning(
            f'"partitioning" metadata value for PolarsParquetIOManager is deprecated '
            f'in favor of "partition_by" (loading from {path})'
        )

    dataset = ds.dataset(
        str(path),
        filesystem=fs,
        format=context_metadata.get("format", "parquet"),
        partitioning=context_metadata.get("partitioning") or context_metadata.get("partition_by"),
        partition_base_dir=context_metadata.get("partition_base_dir"),
        exclude_invalid_files=context_metadata.get("exclude_invalid_files", True),
        ignore_prefixes=context_metadata.get("ignore_prefixes", [".", "_"]),
    )

    return dataset


def scan_parquet(path: "UPath", context: InputContext) -> pl.LazyFrame:
    """Scan a parquet file and return a lazy frame (uses polars native reader).

    :param path:
    :param context:
    :return:
    """
    context_metadata = context.metadata or {}

    storage_options: Optional[dict[str, Any]] = path.storage_options if hasattr(path, "storage_options") else None

    kwargs = dict(
        n_rows=context_metadata.get("n_rows", None),
        cache=context_metadata.get("cache", True),
        parallel=context_metadata.get("parallel", "auto"),
        rechunk=context_metadata.get("rechunk", True),
        low_memory=context_metadata.get("low_memory", False),
        use_statistics=context_metadata.get("use_statistics", True),
        hive_partitioning=context_metadata.get("hive_partitioning", True),
        retries=context_metadata.get("retries", 0),
    )
    if Version(pl.__version__) >= Version("0.20.4"):
        kwargs["row_index_name"] = context_metadata.get("row_index_name", None)
        kwargs["row_index_offset"] = context_metadata.get("row_index_offset", 0)
    else:
        kwargs["row_count_name"] = context_metadata.get("row_count_name", None)
        kwargs["row_count_offset"] = context_metadata.get("row_count_offset", 0)

    return pl.scan_parquet(str(path), storage_options=storage_options, **kwargs)  # type: ignore


@experimental
class PolarsParquetIOManager(BasePolarsUPathIOManager):
    """Implements reading and writing Polars DataFrames in Apache Parquet format.

    Features:
     - All features provided by :py:class:`~dagster_polars.BasePolarsUPathIOManager`.
     - All read/write options can be set via corresponding metadata or config parameters (metadata takes precedence).
     - Supports reading partitioned Parquet datasets (for example, often produced by Spark).
     - Supports reading/writing custom metadata in the Parquet file's schema as json-serialized bytes at `"dagster_polars_metadata"` key.

    Examples:

        .. code-block:: python

            from dagster import asset
            from dagster_polars import PolarsParquetIOManager
            import polars as pl

            @asset(
                io_manager_key="polars_parquet_io_manager",
                key_prefix=["my_dataset"]
            )
            def my_asset() -> pl.DataFrame:  # data will be stored at <base_dir>/my_dataset/my_asset.parquet
                ...

            defs = Definitions(
                assets=[my_table],
                resources={
                    "polars_parquet_io_manager": PolarsParquetIOManager(base_dir="s3://my-bucket/my-dir")
                }
            )

        Reading partitioned Parquet datasets:

        .. code-block:: python

            from dagster import SourceAsset

            my_asset = SourceAsset(
                key=["path", "to", "dataset"],
                io_manager_key="polars_parquet_io_manager",
                metadata={
                    "partition_by": ["year", "month", "day"]
                }
            )

        Storing custom metadata in the Parquet file schema (this metadata can be read outside of Dagster with a helper function :py:meth:`dagster_polars.PolarsParquetIOManager.read_parquet_metadata`):

        .. code-block:: python

            from dagster_polars import DataFrameWithMetadata


            @asset(
                io_manager_key="polars_parquet_io_manager",
            )
            def upstream() -> DataFrameWithMetadata:
                return pl.DataFrame(...), {"my_custom_metadata": "my_custom_value"}


            @asset(
                io_manager_key="polars_parquet_io_manager",
            )
            def downsteam(upstream: DataFrameWithMetadata):
                df, metadata = upstream
                assert metadata["my_custom_metadata"] == "my_custom_value"
    """

    extension: str = ".parquet"  # type: ignore

    def sink_df_to_path(
        self,
        context: OutputContext,
        df: pl.LazyFrame,
        path: "UPath",
        metadata: Optional[StorageMetadata] = None,
    ):
        context_metadata = context.metadata or {}

        if metadata is not None:
            context.log.warning("Sink not possible with StorageMetadata, instead it's dispatched to pyarrow writer.")
            return self.write_df_to_path(context, df.collect(), path, metadata)
        else:
            fs = path.fs if hasattr(path, "fs") else None
            if isinstance(fs, LocalFileSystem):
                compression = context_metadata.get("compression", "zstd")
                compression_level = context_metadata.get("compression_level")
                statistics = context_metadata.get("statistics", False)
                row_group_size = context_metadata.get("row_group_size")

                df.sink_parquet(
                    str(path),
                    compression=compression,
                    compression_level=compression_level,
                    statistics=statistics,
                    row_group_size=row_group_size,
                )
            else:
                # TODO(ion): add sink_parquet once this PR gets merged: https://github.com/pola-rs/polars/pull/11519
                context.log.warning(
                    "Cloud sink is not possible yet, instead it's dispatched to pyarrow writer which collects it into memory first.",
                )
                return self.write_df_to_path(context, df.collect(), path, metadata)

    def write_df_to_path(
        self,
        context: OutputContext,
        df: pl.DataFrame,
        path: "UPath",
        metadata: Optional[StorageMetadata] = None,
    ):
        context_metadata = context.metadata or {}
        compression = context_metadata.get("compression", "zstd")
        compression_level = context_metadata.get("compression_level")
        statistics = context_metadata.get("statistics", False)
        row_group_size = context_metadata.get("row_group_size")
        pyarrow_options = context_metadata.get("pyarrow_options", None)

        fs = path.fs if hasattr(path, "fs") else None

        if metadata is not None:
            table: Table = df.to_arrow()
            context.log.warning("StorageMetadata is passed, so the PyArrow writer is used.")
            existing_metadata = table.schema.metadata.to_dict() if table.schema.metadata is not None else {}
            existing_metadata.update({DAGSTER_POLARS_STORAGE_METADATA_KEY: json.dumps(metadata)})
            table = table.replace_schema_metadata(existing_metadata)

            if pyarrow_options is not None and pyarrow_options.get("partition_cols"):
                pyarrow_options["compression"] = None if compression == "uncompressed" else compression
                pyarrow_options["compression_level"] = compression_level
                pyarrow_options["write_statistics"] = statistics
                pyarrow_options["row_group_size"] = row_group_size
                pq.write_to_dataset(
                    table=table,
                    root_path=str(path),
                    fs=fs,
                    **(pyarrow_options or {}),
                )
            else:
                pq.write_table(
                    table=table,
                    where=str(path),
                    row_group_size=row_group_size,
                    compression=None if compression == "uncompressed" else compression,  # type: ignore
                    compression_level=compression_level,
                    write_statistics=statistics,
                    filesystem=fs,
                    **(pyarrow_options or {}),
                )
        else:
            if pyarrow_options is not None:
                pyarrow_options["filesystem"] = fs
                df.write_parquet(
                    str(path),
                    compression=compression,  # type: ignore
                    compression_level=compression_level,
                    statistics=statistics,
                    row_group_size=row_group_size,
                    use_pyarrow=True,
                    pyarrow_options=pyarrow_options,
                )
            elif fs is not None:
                with fs.open(str(path), mode="wb") as f:
                    df.write_parquet(
                        f,  # type: ignore
                        compression=compression,  # type: ignore
                        compression_level=compression_level,
                        statistics=statistics,
                        row_group_size=row_group_size,
                    )
            else:
                df.write_parquet(
                    str(path),
                    compression=compression,  # type: ignore
                    compression_level=compression_level,
                    statistics=statistics,
                    row_group_size=row_group_size,
                )

    @overload
    def scan_df_from_path(
        self, path: "UPath", context: InputContext, with_metadata: Literal[None, False]
    ) -> pl.LazyFrame:
        ...

    @overload
    def scan_df_from_path(
        self, path: "UPath", context: InputContext, with_metadata: Literal[True]
    ) -> LazyFrameWithMetadata:
        ...

    def scan_df_from_path(
        self,
        path: "UPath",
        context: InputContext,
        with_metadata: Optional[bool] = False,
        partition_key: Optional[str] = None,
    ) -> Union[pl.LazyFrame, LazyFrameWithMetadata]:
        ldf = scan_parquet(path, context)

        if not with_metadata:
            return ldf
        else:
            ds = get_pyarrow_dataset(path, context)
            dagster_polars_metadata = (
                ds.schema.metadata.get(DAGSTER_POLARS_STORAGE_METADATA_KEY.encode("utf-8"))
                if ds.schema.metadata is not None
                else None
            )

            metadata = json.loads(dagster_polars_metadata) if dagster_polars_metadata is not None else {}

            return ldf, metadata

    @classmethod
    def read_parquet_metadata(cls, path: "UPath") -> StorageMetadata:
        """Just a helper method to read metadata from a parquet file.

        Is not used internally, but is helpful for reading Parquet metadata from outside of Dagster.
        :param path:
        :return:
        """
        metadata = pq.read_metadata(str(path), filesystem=path.fs if hasattr(path, "fs") else None).metadata

        dagster_polars_metadata = (
            metadata.get(DAGSTER_POLARS_STORAGE_METADATA_KEY.encode("utf-8")) if metadata is not None else None
        )

        return json.loads(dagster_polars_metadata) if dagster_polars_metadata is not None else {}
