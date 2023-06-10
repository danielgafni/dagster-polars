import polars as pl
import polars.testing as pl_testing
from dagster import IOManagerDefinition, asset, materialize
from deepdiff import DeepDiff
from hypothesis import given, settings
from polars.testing.parametric import dataframes


def test_polars_parquet_io_manager_stats_metadata(
    tmp_polars_parquet_io_manager: IOManagerDefinition,
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
def test_polars_parquet_io_manager(tmp_polars_parquet_io_manager: IOManagerDefinition, df: pl.DataFrame):
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
    tmp_polars_parquet_io_manager: IOManagerDefinition,
):
    df = pl.DataFrame(
        {
            "a": [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]],
            "b": [["a", "b"], ["c", "d"], ["e", "f"], ["g", "h"], ["i", "j"]],
            "c": [{"a": 0}, {"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}],
        }
    )

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


def test_polars_parquet_io_manager_legacy(
    tmp_polars_parquet_io_manager_legacy: IOManagerDefinition,
):
    df = pl.DataFrame(
        {
            "a": [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]],
            "b": [["a", "b"], ["c", "d"], ["e", "f"], ["g", "h"], ["i", "j"]],
            "c": [{"a": 0}, {"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}],
        }
    )

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
