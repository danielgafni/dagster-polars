import json
import sys
from abc import abstractmethod
from datetime import date, datetime, time, timedelta
from pprint import pformat
from typing import Any, Dict, Mapping, Optional, Tuple, Union

import polars as pl
from dagster import (
    ConfigurableIOManager,
    InitResourceContext,
    InputContext,
    MetadataValue,
    MultiPartitionKey,
    OutputContext,
    TableColumn,
    TableMetadataValue,
    TableRecord,
    TableSchema,
    UPathIOManager,
)
from dagster import _check as check
from pydantic.fields import Field, PrivateAttr
from upath import UPath

from dagster_polars.io_managers.utils import get_polars_metadata

POLARS_DATA_FRAME_ANNOTATIONS = [
    Any,
    pl.DataFrame,
    Dict[str, pl.DataFrame],
    Mapping[str, pl.DataFrame],
    type(None),
    None,
]

POLARS_LAZY_FRAME_ANNOTATIONS = [
    pl.LazyFrame,
    Dict[str, pl.LazyFrame],
    Mapping[str, pl.LazyFrame],
]


if sys.version >= "3.9":
    POLARS_DATA_FRAME_ANNOTATIONS.append(dict[str, pl.DataFrame])  # type: ignore
    POLARS_LAZY_FRAME_ANNOTATIONS.append(dict[str, pl.DataFrame])  # type: ignore


def cast_polars_single_value_to_dagster_table_types(val: Any):
    if val is None:
        return ""
    elif isinstance(val, (date, datetime, time, timedelta)):
        return str(val)
    elif isinstance(val, (list, dict)):
        # default=str because sometimes the object can be a list of datetimes or something like this
        return json.dumps(val, default=str)
    else:
        return val


def get_metadata_schema(
    df: pl.DataFrame,
    descriptions: Optional[Dict[str, str]] = None,
):
    descriptions = descriptions or {}
    return TableSchema(
        columns=[
            TableColumn(name=col, type=str(pl_type), description=descriptions.get(col))
            for col, pl_type in df.schema.items()
        ]
    )


def get_metadata_table_and_schema(
    context: OutputContext,
    df: pl.DataFrame,
    n_rows: Optional[int] = 5,
    fraction: Optional[float] = None,
    descriptions: Optional[Dict[str, str]] = None,
) -> Tuple[TableSchema, Optional[TableMetadataValue]]:
    assert not fraction and n_rows, "only one of n_rows and frac should be set"
    n_rows = min(n_rows, len(df))

    schema = get_metadata_schema(df, descriptions=descriptions)

    df_sample = df.sample(n=n_rows, fraction=fraction, shuffle=True)

    try:
        # this can fail sometimes
        # because TableRecord doesn't support all python types
        table = MetadataValue.table(
            records=[
                TableRecord(
                    {
                        col: cast_polars_single_value_to_dagster_table_types(  # type: ignore
                            df_sample.to_dicts()[i][col]
                        )
                        for col in df.columns
                    }
                )
                for i in range(len(df_sample))
            ],
            schema=schema,
        )

    except TypeError as e:
        context.log.error(
            f"Failed to create table sample metadata. Will only record table schema metadata. "
            f"Reason:\n{e}\n"
            f"Schema:\n{df.schema}\n"
            f"Polars sample:\n{df_sample}\n"
            f"dict sample:\n{pformat(df_sample.to_dicts())}"
        )
        return schema, None

    return schema, table


def get_polars_df_stats(
    df: pl.DataFrame,
) -> Dict[str, Dict[str, Union[str, int, float]]]:
    describe = df.describe().fill_null(pl.lit("null"))
    return {
        col: {stat: describe[col][i] for i, stat in enumerate(describe["describe"].to_list())}
        for col in describe.columns[1:]
    }


class BasePolarsUPathIOManager(ConfigurableIOManager, UPathIOManager):
    # This is a base class which doesn't define the specific format (parquet, csv, etc) to use
    """
    `IOManager` for `polars` based on the `UPathIOManager`.
    Features:
     - returns the correct type (`polars.DataFrame` or `polars.LazyFrame`) based on the type annotation
     - logs various metadata about the DataFrame - size, schema, sample, stats, ...
     - the "columns" input metadata value can be used to select a subset of columns
     - inherits all the features of the `UPathIOManager` - works with local and remote filesystems (like S3),
         supports loading multiple partitions, ...
    """

    base_dir: Optional[str] = Field(default=None, description="Base directory for storing files.")

    _base_path: UPath = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._base_path = (
            UPath(self.base_dir)
            if self.base_dir is not None
            else UPath(check.not_none(context.instance).storage_directory())
        )

    @abstractmethod
    def dump_df_to_path(self, context: OutputContext, df: pl.DataFrame, path: UPath):
        ...

    @abstractmethod
    def scan_df_from_path(self, path: UPath, context: InputContext) -> pl.LazyFrame:
        ...

    def dump_to_path(self, context: OutputContext, obj: pl.DataFrame, path: UPath):
        self.dump_df_to_path(context=context, df=obj, path=path)

    def load_from_path(self, path: UPath, context: InputContext) -> Union[pl.DataFrame, pl.LazyFrame]:
        assert context.metadata is not None

        ldf = self.scan_df_from_path(path=path, context=context)

        columns = context.metadata.get("columns")
        if columns is not None:
            context.log.debug(f"Loading {columns=}")
            ldf = ldf.select(columns)

        if context.dagster_type.typing_type in POLARS_DATA_FRAME_ANNOTATIONS:
            return ldf.collect(streaming=True)
        elif context.dagster_type.typing_type in POLARS_LAZY_FRAME_ANNOTATIONS:
            return ldf
        else:
            raise NotImplementedError(f"Can't load object for type annotation {context.dagster_type.typing_type}")

    def get_metadata(self, context: OutputContext, obj: pl.DataFrame) -> Dict[str, MetadataValue]:
        return get_polars_metadata(context, obj)

    @staticmethod
    def get_storage_options(path: UPath) -> dict:
        storage_options = {}

        try:
            storage_options.update(path._kwargs.copy())
        except AttributeError:
            pass

        return storage_options

    def get_path_for_partition(self, context: Union[InputContext, OutputContext], path: UPath, partition: str) -> UPath:
        """
        Override this method if you want to use a different partitioning scheme
        (for example, if the saving function handles partitioning instead).
        The extension will be added later.
        :param context:
        :param path: asset path before partitioning
        :param partition: formatted partition key
        :return:
        """
        return path / partition

    def _get_paths_for_partitions(self, context: Union[InputContext, OutputContext]) -> Dict[str, "UPath"]:
        """Returns a dict of partition_keys into I/O paths for a given context."""
        if not context.has_asset_partitions:
            raise TypeError(
                f"Detected {context.dagster_type.typing_type} input type " "but the asset is not partitioned"
            )

        def _formatted_multipartitioned_path(partition_key: MultiPartitionKey) -> str:
            ordered_dimension_keys = [
                key[1] for key in sorted(partition_key.keys_by_dimension.items(), key=lambda x: x[0])
            ]
            return "/".join(ordered_dimension_keys)

        formatted_partition_keys = [
            _formatted_multipartitioned_path(pk) if isinstance(pk, MultiPartitionKey) else pk
            for pk in context.asset_partition_keys
        ]

        asset_path = self._get_path_without_extension(context)
        return {
            partition: self._with_extension(self.get_path_for_partition(context, asset_path, partition))
            for partition in formatted_partition_keys
        }
