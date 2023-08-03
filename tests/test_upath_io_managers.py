from typing import Dict, Optional, Tuple

import polars as pl
import polars.testing as pl_testing
from dagster import AssetExecutionContext, OpExecutionContext, StaticPartitionsDefinition, asset, materialize
from deepdiff import DeepDiff

from dagster_polars import BasePolarsUPathIOManager, PolarsDeltaIOManager, PolarsParquetIOManager
from tests.utils import get_saved_path


def test_polars_upath_io_manager_stats_metadata(io_manager_and_df: Tuple[BasePolarsUPathIOManager, pl.DataFrame]):
    manager, _ = io_manager_and_df

    df = pl.DataFrame({"a": [0, 1, None], "b": ["a", "b", "c"]})

    @asset(io_manager_def=manager)
    def upstream() -> pl.DataFrame:
        return df

    result = materialize(
        [upstream],
    )

    handled_output_events = list(filter(lambda evt: evt.is_handled_output, result.events_for_node("upstream")))

    stats = handled_output_events[0].event_specific_data.metadata["stats"].value  # type: ignore  # noqa

    assert (
        DeepDiff(
            stats,
            {
                "a": {
                    "count": 3.0,
                    "null_count": 1.0,
                    "mean": 0.5,
                    "std": 0.7071067811865476,
                    "min": 0.0,
                    "max": 1.0,
                    "median": 0.5,
                    "25%": 0.0,
                    "75%": 1.0,
                },
                "b": {
                    "count": "3",
                    "null_count": "0",
                    "mean": "null",
                    "std": "null",
                    "min": "a",
                    "max": "c",
                    "median": "null",
                    "25%": "null",
                    "75%": "null",
                },
            },
        )
        == {}
    )


def test_polars_upath_io_manager_type_annotations(io_manager_and_df: Tuple[BasePolarsUPathIOManager, pl.DataFrame]):
    manager, df = io_manager_and_df

    @asset(io_manager_def=manager)
    def upstream() -> pl.DataFrame:
        return df

    @asset(io_manager_def=manager)
    def downstream_default_eager(upstream) -> None:
        assert isinstance(upstream, pl.DataFrame), type(upstream)

    @asset(io_manager_def=manager)
    def downstream_eager(upstream: pl.DataFrame) -> None:
        assert isinstance(upstream, pl.DataFrame), type(upstream)

    @asset(io_manager_def=manager)
    def downstream_lazy(upstream: pl.LazyFrame) -> None:
        assert isinstance(upstream, pl.LazyFrame), type(upstream)

    partitions_def = StaticPartitionsDefinition(["a", "b"])

    @asset(io_manager_def=manager, partitions_def=partitions_def)
    def upstream_partitioned(context: OpExecutionContext) -> pl.DataFrame:
        return df.with_columns(pl.lit(context.partition_key).alias("partition"))

    @asset(io_manager_def=manager)
    def downstream_multi_partitioned_eager(upstream_partitioned: Dict[str, pl.DataFrame]) -> None:
        for _df in upstream_partitioned.values():
            assert isinstance(_df, pl.DataFrame), type(_df)
        assert set(upstream_partitioned.keys()) == {"a", "b"}, upstream_partitioned.keys()

    @asset(io_manager_def=manager)
    def downstream_multi_partitioned_lazy(upstream_partitioned: Dict[str, pl.LazyFrame]) -> None:
        for _df in upstream_partitioned.values():
            assert isinstance(_df, pl.LazyFrame), type(_df)
        assert set(upstream_partitioned.keys()) == {"a", "b"}, upstream_partitioned.keys()

    for partition_key in ["a", "b"]:
        materialize(
            [upstream_partitioned],
            partition_key=partition_key,
        )

    materialize(
        [
            upstream_partitioned.to_source_asset(),
            upstream,
            downstream_default_eager,
            downstream_eager,
            downstream_lazy,
            downstream_multi_partitioned_eager,
            downstream_multi_partitioned_lazy,
        ],
    )


def test_polars_upath_io_manager_nested_dtypes(io_manager_and_df: Tuple[BasePolarsUPathIOManager, pl.DataFrame]):
    manager, df = io_manager_and_df

    @asset(io_manager_def=manager)
    def upstream() -> pl.DataFrame:
        return df

    @asset(io_manager_def=manager)
    def downstream(upstream: pl.LazyFrame) -> pl.DataFrame:
        return upstream.collect(streaming=True)

    result = materialize(
        [upstream, downstream],
    )

    saved_path = get_saved_path(result, "upstream")

    if isinstance(manager, PolarsParquetIOManager):
        pl_testing.assert_frame_equal(df, pl.read_parquet(saved_path))
    elif isinstance(manager, PolarsDeltaIOManager):
        pl_testing.assert_frame_equal(df, pl.read_delta(saved_path))
    else:
        raise ValueError(f"Test not implemented for {type(manager)}")


def test_polars_upath_io_manager_optional_eager(io_manager_and_df: Tuple[BasePolarsUPathIOManager, pl.DataFrame]):
    manager, df = io_manager_and_df

    @asset(io_manager_def=manager)
    def upstream() -> pl.DataFrame:
        return df

    @asset(io_manager_def=manager)
    def downstream(upstream: Optional[pl.DataFrame]) -> pl.DataFrame:
        assert upstream is not None
        return upstream

    materialize(
        [upstream, downstream],
    )


def test_polars_upath_io_manager_optional_lazy(io_manager_and_df: Tuple[BasePolarsUPathIOManager, pl.DataFrame]):
    manager, df = io_manager_and_df

    @asset(io_manager_def=manager)
    def upstream() -> pl.DataFrame:
        return df

    @asset(io_manager_def=manager)
    def downstream(upstream: Optional[pl.LazyFrame]) -> pl.DataFrame:
        assert upstream is not None
        return upstream.collect()

    materialize(
        [upstream, downstream],
    )


def test_polars_upath_io_manager_optional_dict_eager(io_manager_and_df: Tuple[BasePolarsUPathIOManager, pl.DataFrame]):
    manager, df = io_manager_and_df

    @asset(io_manager_def=manager, partitions_def=StaticPartitionsDefinition(["a", "b"]))
    def upstream(context: AssetExecutionContext) -> pl.DataFrame:
        return df.with_columns(pl.lit(context.partition_key).alias("partition"))

    @asset(io_manager_def=manager)
    def downstream(upstream: Dict[str, Optional[pl.DataFrame]]) -> pl.DataFrame:
        dfs = []
        for partition, df in upstream.items():
            assert df is not None
            dfs.append(df)
        return pl.concat(dfs)

    for partition_key in ["a", "b"]:
        materialize(
            [upstream],
            partition_key=partition_key,
        )

    materialize(
        [upstream.to_source_asset(), downstream],
    )


def test_polars_upath_io_manager_optional_dict_lazy(io_manager_and_df: Tuple[BasePolarsUPathIOManager, pl.DataFrame]):
    manager, df = io_manager_and_df

    @asset(io_manager_def=manager, partitions_def=StaticPartitionsDefinition(["a", "b"]))
    def upstream(context: AssetExecutionContext) -> pl.DataFrame:
        return df.with_columns(pl.lit(context.partition_key).alias("partition"))

    @asset(io_manager_def=manager)
    def downstream(upstream: Dict[str, Optional[pl.LazyFrame]]) -> pl.DataFrame:
        dfs = []
        for partition, df in upstream.items():
            assert df is not None
            dfs.append(df)
        return pl.concat(dfs).collect()

    for partition_key in ["a", "b"]:
        materialize(
            [upstream],
            partition_key=partition_key,
        )

    materialize(
        [upstream.to_source_asset(), downstream],
    )
