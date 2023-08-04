import sys
from abc import abstractmethod
from typing import Any, Dict, Mapping, Optional, Union, get_args, get_origin

import polars as pl
from dagster import (
    ConfigurableIOManager,
    InitResourceContext,
    InputContext,
    MetadataValue,
    MultiPartitionKey,
    OutputContext,
    UPathIOManager,
)
from dagster import _check as check
from pydantic.fields import Field, PrivateAttr
from upath import UPath

from dagster_polars.io_managers.utils import get_polars_metadata

POLARS_EAGER_FRAME_ANNOTATIONS = [
    Any,
    pl.DataFrame,
    Optional[pl.DataFrame],
    Dict[str, pl.DataFrame],
    Dict[str, Optional[pl.DataFrame]],
    Mapping[str, pl.DataFrame],
    Mapping[str, Optional[pl.DataFrame]],
    type(None),
    None,
]

POLARS_LAZY_FRAME_ANNOTATIONS = [
    pl.LazyFrame,
    Optional[pl.LazyFrame],
    Dict[str, pl.LazyFrame],
    Dict[str, Optional[pl.LazyFrame]],
    Mapping[str, pl.LazyFrame],
    Mapping[str, Optional[pl.LazyFrame]],
]


if sys.version >= "3.9":
    POLARS_EAGER_FRAME_ANNOTATIONS.append(dict[str, pl.DataFrame])  # type: ignore
    POLARS_EAGER_FRAME_ANNOTATIONS.append(dict[str, Optional[pl.DataFrame]])  # type: ignore
    POLARS_LAZY_FRAME_ANNOTATIONS.append(dict[str, pl.LazyFrame])  # type: ignore
    POLARS_LAZY_FRAME_ANNOTATIONS.append(dict[str, Optional[pl.LazyFrame]])  # type: ignore


def annotation_is_typing_optional(annotation):
    return get_origin(annotation) == Union and type(None) in get_args(annotation)


class BasePolarsUPathIOManager(ConfigurableIOManager, UPathIOManager):
    # This is a base class which doesn't define the specific format (parquet, csv, etc) to use
    """
    `IOManager` for `polars` based on the `UPathIOManager`.
    Features:
     - returns the correct type (`polars.DataFrame` or `polars.LazyFrame`) based on the type annotation
     - handles `Optional` types by skipping loading missing inputs or `None` outputs
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
        if annotation_is_typing_optional(context.dagster_type.typing_type) and obj is None:
            context.log.warning(self.get_optional_output_none_log_message(context, path))
            return
        else:
            self.dump_df_to_path(context=context, df=obj, path=path)

    def load_from_path(self, path: UPath, context: InputContext) -> Union[pl.DataFrame, pl.LazyFrame, None]:
        if annotation_is_typing_optional(context.dagster_type.typing_type) and not path.exists():
            context.log.warning(self.get_missing_optional_input_log_message(context, path))
            return None

        assert context.metadata is not None

        ldf = self.scan_df_from_path(path=path, context=context)

        columns = context.metadata.get("columns")
        if columns is not None:
            context.log.debug(f"Loading {columns=}")
            ldf = ldf.select(columns)

        if context.dagster_type.typing_type in POLARS_EAGER_FRAME_ANNOTATIONS:
            return ldf.collect(streaming=True)
        elif context.dagster_type.typing_type in POLARS_LAZY_FRAME_ANNOTATIONS:
            return ldf
        else:
            raise NotImplementedError(f"Can't load object for type annotation {context.dagster_type.typing_type}")

    def get_metadata(self, context: OutputContext, obj: pl.DataFrame) -> Dict[str, MetadataValue]:
        return get_polars_metadata(context, obj) if obj is not None else {"missing": MetadataValue.bool(True)}

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

    def get_missing_optional_input_log_message(self, context: InputContext, path: UPath) -> str:
        return f"Optional input {context.name} at {path} doesn't exist in the filesystem and won't be loaded!"

    def get_optional_output_none_log_message(self, context: OutputContext, path: UPath) -> str:
        return f"The object for the optional output {context.name} is None, so it won't be saved to {path}!"
