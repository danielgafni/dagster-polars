import os
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest
from _pytest.tmpdir import TempPathFactory
from dagster import IOManagerDefinition

from dagster_polars import PolarsParquetIOManager, polars_parquet_io_manager


@pytest.fixture(scope="session")
def dagster_home(tmp_path_factory: TempPathFactory):
    dagster_home = tmp_path_factory.mktemp("dagster")
    original_dagster_home = os.environ.get("DAGSTER_HOME")
    os.environ["DAGSTER_HOME"] = str(dagster_home)
    yield dagster_home
    if original_dagster_home is not None:
        os.environ["DAGSTER_HOME"] = original_dagster_home


@pytest.fixture(scope="session")
def tmp_polars_parquet_io_manager(dagster_home: Path) -> PolarsParquetIOManager:
    return PolarsParquetIOManager(base_dir=str(dagster_home))


@pytest.fixture(scope="session")
def tmp_polars_parquet_io_manager_legacy(dagster_home: Path) -> IOManagerDefinition:
    return polars_parquet_io_manager.configured({"base_dir": str(dagster_home)})


@pytest.fixture
def df():
    return pl.DataFrame(
        {
            "1": [0, 1, None],
            "2": [0.0, 1.0, None],
            "3": ["a", "b", None],
            "4": [[0, 1], [2, 3], None],
            "6": [{"a": 0}, {"a": 1}, None],
            "7": [datetime(2022, 1, 1), datetime(2022, 1, 2), None],
            "8": [date(2022, 1, 1), date(2022, 1, 2), None],
            "9": [timedelta(hours=1), timedelta(hours=2), None],
        }
    )
