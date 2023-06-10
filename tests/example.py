# I'm just using it to view the description in Dagit

import polars as pl
from dagster import Definitions, asset

from dagster_polars import PolarsParquetIOManager


@asset(io_manager_def=PolarsParquetIOManager(base_dir="/tmp/dagster"))
def my_asset() -> pl.DataFrame:
    return pl.DataFrame({"a": [0, 1, None], "b": ["a", "b", "c"]})


definitions = Definitions(assets=[my_asset])
