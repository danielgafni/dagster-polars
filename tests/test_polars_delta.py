import shutil
from typing import Dict

import polars as pl
import polars.testing as pl_testing
from dagster import DagsterInstance, OpExecutionContext, StaticPartitionsDefinition, asset, materialize
from deltalake import DeltaTable
from hypothesis import given, settings
from polars.testing.parametric import dataframes
from _pytest.tmpdir import TempPathFactory

from dagster_polars import PolarsDeltaIOManager
from dagster_polars.io_managers.delta import DeltaWriteMode
from tests.utils import get_saved_path

# TODO: remove pl.Time once it's supported
# TODO: remove pl.Duration pl.Duration once it's supported
# https://github.com/pola-rs/polars/issues/9631
# TODO: remove UInt types once they are supported
#  https://github.com/pola-rs/polars/issues/9627


delta_df_strategy =dataframes(
        excluded_dtypes=[
            pl.Categorical,
            pl.Duration,
            pl.Time,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
            pl.Datetime("ns", None),
        ],
        min_size=5,
        allow_infinities=False,
    )


def test_polars_delta_io_manager(polars_delta_io_manager: PolarsDeltaIOManager):
    # this test is not wrapped with `hypothesis.given` because it produces weird errors with deltalake
    # .example() is called manually instead
    for i in range(1000):
        df = delta_df_strategy.example()

        @asset(io_manager_def=polars_delta_io_manager, metadata={"overwrite_schema": True})
        def upstream() -> pl.DataFrame:
            return df

        @asset(io_manager_def=polars_delta_io_manager, metadata={"overwrite_schema": True})
        def downstream(upstream: pl.LazyFrame) -> pl.DataFrame:
            return upstream.collect()

        result = materialize(
            [upstream, downstream],
        )
        saved_path = get_saved_path(result, "upstream")
        assert isinstance(saved_path, str)
        pl_testing.assert_frame_equal(df, pl.read_delta(saved_path))
        shutil.rmtree(saved_path)  # cleanup manually because of hypothesis


def test_polars_delta_io_manager_append(polars_delta_io_manager: PolarsDeltaIOManager):
    df = pl.DataFrame(
        {
            "a": [1, 2, 3],
        }
    )

    @asset(io_manager_def=polars_delta_io_manager, metadata={"mode": "append"})
    def append_asset() -> pl.DataFrame:
        return df

    result = materialize(
        [append_asset],
    )

    handled_output_events = list(filter(lambda evt: evt.is_handled_output, result.events_for_node("append_asset")))
    saved_path = handled_output_events[0].event_specific_data.metadata["path"].value  # type: ignore
    assert handled_output_events[0].event_specific_data.metadata["row_count"].value == 3  # type: ignore
    assert handled_output_events[0].event_specific_data.metadata["append_row_count"].value == 3  # type: ignore
    assert isinstance(saved_path, str)

    result = materialize(
        [append_asset],
    )
    handled_output_events = list(filter(lambda evt: evt.is_handled_output, result.events_for_node("append_asset")))
    assert handled_output_events[0].event_specific_data.metadata["row_count"].value == 6  # type: ignore
    assert handled_output_events[0].event_specific_data.metadata["append_row_count"].value == 3  # type: ignore

    pl_testing.assert_frame_equal(pl.concat([df, df]), pl.read_delta(saved_path))


def test_polars_delta_io_manager_overwrite_schema(
    polars_delta_io_manager: PolarsDeltaIOManager, dagster_instance: DagsterInstance
):
    @asset(io_manager_def=polars_delta_io_manager)
    def overwrite_schema_asset_1() -> pl.DataFrame:  # type: ignore
        return pl.DataFrame(
            {
                "a": [1, 2, 3],
            }
        )

    result = materialize(
        [overwrite_schema_asset_1],
    )

    saved_path = get_saved_path(result, "overwrite_schema_asset_1")

    pl_testing.assert_frame_equal(
        pl.DataFrame(
            {
                "a": [1, 2, 3],
            }
        ),
        pl.read_delta(saved_path),
    )

    @asset(io_manager_def=polars_delta_io_manager, metadata={"overwrite_schema": True, "mode": "overwrite"})
    def overwrite_schema_asset_2() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "b": ["1", "2", "3"],
            }
        )

    result = materialize(
        [overwrite_schema_asset_2],
    )

    saved_path = get_saved_path(result, "overwrite_schema_asset_2")

    pl_testing.assert_frame_equal(
        pl.DataFrame(
            {
                "b": ["1", "2", "3"],
            }
        ),
        pl.read_delta(saved_path),
    )

    # test IOManager configuration works too
    @asset(
        io_manager_def=PolarsDeltaIOManager(
            base_dir=dagster_instance.storage_directory(), mode=DeltaWriteMode.overwrite, overwrite_schema=True
        )
    )
    def overwrite_schema_asset_3() -> pl.DataFrame:  # type: ignore
        return pl.DataFrame(
            {
                "a": [1, 2, 3],
            }
        )

    result = materialize(
        [overwrite_schema_asset_3],
    )

    saved_path = get_saved_path(result, "overwrite_schema_asset_3")

    pl_testing.assert_frame_equal(
        pl.DataFrame(
            {
                "a": [1, 2, 3],
            }
        ),
        pl.read_delta(saved_path),
    )


def test_polars_delta_native_partitioning(polars_delta_io_manager: PolarsDeltaIOManager, df_for_delta: pl.DataFrame):
    manager = polars_delta_io_manager
    df = df_for_delta

    partitions_def = StaticPartitionsDefinition(["a", "b"])

    @asset(io_manager_def=manager, partitions_def=partitions_def, metadata={"partition_by": "partition"})
    def upstream_partitioned(context: OpExecutionContext) -> pl.DataFrame:
        return df.with_columns(pl.lit(context.partition_key).alias("partition"))

    @asset(io_manager_def=manager)
    def downstream_load_multiple_partitions(upstream_partitioned: Dict[str, pl.LazyFrame]) -> None:
        for _df in upstream_partitioned.values():
            assert isinstance(_df, pl.LazyFrame), type(_df)
        assert set(upstream_partitioned.keys()) == {"a", "b"}, upstream_partitioned.keys()

    for partition_key in ["a", "b"]:
        result = materialize(
            [upstream_partitioned],
            partition_key=partition_key,
        )

        saved_path = get_saved_path(result, "upstream_partitioned")
        assert saved_path.endswith("upstream_partitioned.delta"), saved_path  # DeltaLake should handle partitioning!
        assert DeltaTable(saved_path).metadata().partition_columns == ["partition"]

    materialize(
        [
            upstream_partitioned.to_source_asset(),
            downstream_load_multiple_partitions,
        ],
    )

import shutil

import polars as pl
import polars.testing as pl_testing
from _pytest.tmpdir import TempPathFactory
from hypothesis import given, settings
from polars.testing.parametric import dataframes

# TODO: remove pl.Time once it's supported
# TODO: remove pl.Duration pl.Duration once it's supported
# https://github.com/pola-rs/polars/issues/9631
# TODO: remove UInt types once they are fixed:
#  https://github.com/pola-rs/polars/issues/9627
@given(
    df=dataframes(
        excluded_dtypes=[
            pl.Categorical,
            pl.Duration,
            pl.Time,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
            pl.Datetime("ns", None),
        ],
        min_size=5,
        allow_infinities=False,
    )
)
@settings(max_examples=3000, deadline=None)
def test_polars_delta_io(df: pl.DataFrame, tmp_path_factory: TempPathFactory):
    tmp_path = tmp_path_factory.mktemp("data")
    df.write_delta(str(tmp_path))
    pl_testing.assert_frame_equal(df, pl.scan_delta(str(tmp_path)).collect())
    shutil.rmtree(str(tmp_path))  # cleanup manually because of hypothesis


import shutil

import polars as pl
import polars.testing as pl_testing
from _pytest.tmpdir import TempPathFactory
from polars.testing.parametric import dataframes


strategy = dataframes(
    excluded_dtypes=[
        pl.Categorical,
        pl.Duration,
        pl.Time,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
        pl.Datetime("ns", None),
    ],
    min_size=5,
    allow_infinities=False,
)


def test_polars_delta_io(tmp_path_factory: TempPathFactory):
    for i in range(3000):
        tmp_path = tmp_path_factory.mktemp("data")
        df = strategy.example()
        assert isinstance(df, pl.DataFrame)

        df.write_delta(str(tmp_path))
        pl_testing.assert_frame_equal(df, pl.read_delta(str(tmp_path)))
        shutil.rmtree(str(tmp_path))
