import os

import polars as pl
import polars.testing as pl_testing
from dagster import asset, materialize
from hypothesis import given, settings
from polars.testing.parametric import dataframes

from dagster_polars import PolarsParquetIOManager


# allowed_dtypes=[pl.List(inner) for inner in
# list(pl.TEMPORAL_DTYPES | pl.FLOAT_DTYPES | pl.INTEGER_DTYPES) + [pl.Boolean, pl.Utf8]]
@given(df=dataframes(excluded_dtypes=[pl.Categorical], min_size=5))
@settings(max_examples=100, deadline=None)
def test_polars_parquet_io_manager(session_polars_parquet_io_manager: PolarsParquetIOManager, df: pl.DataFrame):
    @asset(io_manager_def=session_polars_parquet_io_manager)
    def upstream() -> pl.DataFrame:
        return df

    @asset(io_manager_def=session_polars_parquet_io_manager)
    def downstream(upstream: pl.LazyFrame) -> pl.DataFrame:
        return upstream.collect(streaming=True)

    result = materialize(
        [upstream, downstream],
    )

    handled_output_events = list(filter(lambda evt: evt.is_handled_output, result.events_for_node("upstream")))

    saved_path = handled_output_events[0].event_specific_data.metadata["path"].value  # type: ignore[index,union-attr]
    assert isinstance(saved_path, str)
    pl_testing.assert_frame_equal(df, pl.read_parquet(saved_path))
    os.remove(saved_path)  # cleanup manually because of hypothesis
