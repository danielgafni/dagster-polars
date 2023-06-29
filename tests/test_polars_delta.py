import polars as pl
import polars.testing as pl_testing
from dagster import asset, materialize
from hypothesis import assume, given, settings
from polars.testing.parametric import dataframes

from dagster_polars import PolarsDeltaIOManager


# TODO: remove `pl.UInt8` once this is fixed:
@given(df=dataframes(excluded_dtypes=[pl.Categorical], min_size=5))
@settings(max_examples=100, deadline=None)
def test_polars_delta_io_manager(session_polars_delta_io_manager: PolarsDeltaIOManager, df: pl.DataFrame):
    # TODO: remove once pl.Time is supported
    assume(pl.Time not in df.schema.values())

    # TODO: remove once this is fixed:
    # delta is currently bugged with UInt8 and UInt16: https://github.com/pola-rs/polars/issues/9627"
    assume(not any(uint in df.schema.values() for uint in [pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64]))

    @asset(io_manager_def=session_polars_delta_io_manager, metadata={"overwrite_schema": True})
    def upstream() -> pl.DataFrame:
        return df

    @asset(io_manager_def=session_polars_delta_io_manager, metadata={"overwrite_schema": True})
    def downstream(upstream: pl.LazyFrame) -> pl.DataFrame:
        return upstream.collect(streaming=True)

    result = materialize(
        [upstream, downstream],
    )

    handled_output_events = list(filter(lambda evt: evt.is_handled_output, result.events_for_node("upstream")))

    saved_path = handled_output_events[0].event_specific_data.metadata["path"].value  # type: ignore[index,union-attr]
    assert isinstance(saved_path, str)
    pl_testing.assert_frame_equal(df, pl.read_delta(saved_path))
