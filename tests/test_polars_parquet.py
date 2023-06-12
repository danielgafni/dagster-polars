from typing import Dict

import polars as pl
import polars.testing as pl_testing
from dagster import IOManagerDefinition, OpExecutionContext, StaticPartitionsDefinition, asset, materialize
from deepdiff import DeepDiff
from hypothesis import given, settings
from polars.testing.parametric import dataframes

from dagster_polars import PolarsParquetIOManager


def test_polars_parquet_io_manager_stats_metadata(
    tmp_polars_parquet_io_manager: PolarsParquetIOManager,
):
    df = pl.DataFrame({"a": [0, 1, None], "b": ["a", "b", "c"]})

    @asset(io_manager_key="polars_parquet_io_manager")
    def upstream() -> pl.DataFrame:
        return df

    result = materialize(
        [upstream],
        resources={"polars_parquet_io_manager": tmp_polars_parquet_io_manager},
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


# allowed_dtypes=[pl.List(inner) for inner in
# list(pl.TEMPORAL_DTYPES | pl.FLOAT_DTYPES | pl.INTEGER_DTYPES) + [pl.Boolean, pl.Utf8]]
@given(df=dataframes(excluded_dtypes=[pl.Categorical], min_size=5))
@settings(max_examples=100, deadline=None)
def test_polars_parquet_io_manager(tmp_polars_parquet_io_manager: PolarsParquetIOManager, df: pl.DataFrame):
    @asset(io_manager_key="polars_parquet_io_manager")
    def upstream() -> pl.DataFrame:
        return df

    @asset(io_manager_key="polars_parquet_io_manager")
    def downstream(upstream: pl.LazyFrame) -> pl.DataFrame:
        return upstream.collect(streaming=True)

    result = materialize(
        [upstream, downstream],
        resources={"polars_parquet_io_manager": tmp_polars_parquet_io_manager},
    )

    handled_output_events = list(filter(lambda evt: evt.is_handled_output, result.events_for_node("upstream")))

    saved_path = handled_output_events[0].event_specific_data.metadata["path"].value  # type: ignore[index,union-attr]
    assert isinstance(saved_path, str)
    pl_testing.assert_frame_equal(df, pl.read_parquet(saved_path))


def test_polars_parquet_io_manager_nested_dtypes(
    tmp_polars_parquet_io_manager: PolarsParquetIOManager, df: pl.DataFrame
):
    @asset(io_manager_key="polars_parquet_io_manager")
    def upstream() -> pl.DataFrame:
        return df

    @asset(io_manager_key="polars_parquet_io_manager")
    def downstream(upstream: pl.LazyFrame) -> pl.DataFrame:
        return upstream.collect(streaming=True)

    result = materialize(
        [upstream, downstream],
        resources={"polars_parquet_io_manager": tmp_polars_parquet_io_manager},
    )

    handled_output_events = list(filter(lambda evt: evt.is_handled_output, result.events_for_node("upstream")))

    saved_path = handled_output_events[0].event_specific_data.metadata["path"].value  # type: ignore[index,union-attr]
    assert isinstance(saved_path, str)
    pl_testing.assert_frame_equal(df, pl.read_parquet(saved_path))


def test_polars_parquet_io_manager_type_annotations(
    tmp_polars_parquet_io_manager_legacy: IOManagerDefinition, df: pl.DataFrame
):
    @asset(io_manager_key="polars_parquet_io_manager")
    def upstream() -> pl.DataFrame:
        return df

    @asset
    def downstream_default_eager(upstream) -> None:
        assert isinstance(upstream, pl.DataFrame), type(upstream)

    @asset
    def downstream_eager(upstream: pl.DataFrame) -> None:
        assert isinstance(upstream, pl.DataFrame), type(upstream)

    @asset
    def downstream_lazy(upstream: pl.LazyFrame) -> None:
        assert isinstance(upstream, pl.LazyFrame), type(upstream)

    partitions_def = StaticPartitionsDefinition(["a", "b"])

    @asset(io_manager_key="polars_parquet_io_manager", partitions_def=partitions_def)
    def upstream_partitioned(context: OpExecutionContext) -> pl.DataFrame:
        return df.with_columns(pl.lit(context.partition_key).alias("partition"))

    @asset
    def downstream_multi_partitioned_eager(upstream_partitioned: Dict[str, pl.DataFrame]) -> None:
        for _df in upstream_partitioned.values():
            assert isinstance(_df, pl.DataFrame), type(_df)
        assert set(upstream_partitioned.keys()) == {"a", "b"}, upstream_partitioned.keys()

    @asset
    def downstream_multi_partitioned_lazy(upstream_partitioned: Dict[str, pl.LazyFrame]) -> None:
        for _df in upstream_partitioned.values():
            assert isinstance(_df, pl.LazyFrame), type(_df)
        assert set(upstream_partitioned.keys()) == {"a", "b"}, upstream_partitioned.keys()

    for partition_key in ["a", "b"]:
        materialize(
            [upstream_partitioned],
            resources={"polars_parquet_io_manager": tmp_polars_parquet_io_manager_legacy},
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
        resources={"polars_parquet_io_manager": tmp_polars_parquet_io_manager_legacy},
    )


def test_polars_parquet_io_manager_legacy(tmp_polars_parquet_io_manager_legacy: IOManagerDefinition, df: pl.DataFrame):
    @asset(io_manager_key="polars_parquet_io_manager")
    def upstream() -> pl.DataFrame:
        return df

    result = materialize(
        [upstream],
        resources={"polars_parquet_io_manager": tmp_polars_parquet_io_manager_legacy},
    )

    handled_output_events = list(filter(lambda evt: evt.is_handled_output, result.events_for_node("upstream")))

    saved_path = handled_output_events[0].event_specific_data.metadata["path"].value  # type: ignore[index,union-attr]
    assert isinstance(saved_path, str)
    pl_testing.assert_frame_equal(df, pl.read_parquet(saved_path))
