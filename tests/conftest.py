from datetime import date, datetime, timedelta

import polars as pl
import pytest
from _pytest.tmpdir import TempPathFactory
from dagster import DagsterInstance, IOManagerDefinition

from dagster_polars import PolarsParquetIOManager, polars_parquet_io_manager


@pytest.fixture(scope="session")
def dagster_instance(tmp_path_factory: TempPathFactory):
    return DagsterInstance.ephemeral(tempdir=str(tmp_path_factory.mktemp("dagster_home")))


@pytest.fixture(scope="session")
def tmp_polars_parquet_io_manager(dagster_instance: DagsterInstance) -> PolarsParquetIOManager:
    return PolarsParquetIOManager(base_dir=dagster_instance.storage_directory())


@pytest.fixture(scope="session")
def tmp_polars_parquet_io_manager_legacy(dagster_instance: DagsterInstance) -> IOManagerDefinition:
    return polars_parquet_io_manager.configured({"base_dir": dagster_instance.storage_directory()})


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
