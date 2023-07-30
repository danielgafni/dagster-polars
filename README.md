# `dagster-polars`

[Polars](https://github.com/pola-rs/polars) integration library for [Dagster](https://github.com/dagster-io/dagster).

## Features
 - All IOManagers log various metadata about the DataFrame - size, schema, sample, stats, ...
 - For all IOManagers the `"columns"` input metadata key can be used to select a subset of columns to load
 - `BasePolarsUPathIOManager` is a base class for IO managers that work with Polars DataFrames. Shouldn't be used directly unless you want to implement your own `IOManager`.
   - returns the correct type (`polars.DataFrame` or `polars.LazyFrame`) based on the type annotation
   - inherits all the features of the `UPathIOManager` - works with local and remote filesystems (like S3),
       supports loading multiple partitions (use `dict[str, pl.DataFrame]` type annotation), ...
   - Implemented serialization formats:
     - `PolarsParquetIOManager` - for reading and writing files in Apache Parquet format. Supports reading partitioned Parquet datasets (for example, often produced by Spark). All read/write options can be set via metadata values.
     - `PolarsDeltaIOManager` - for reading and writing Delta Lake. All read/write options can be set via metadata values. `mode`, `overwrite_schema` and `version` can be set via config parameters. `partition_by` metadata value can be set to use native Delta Lake partitioning (it's passed to `delta_write_options` of `write_delta`). The IOManager won't manage partitioning in this case, and all the asset partitions will be stored in the same Delta Table directory. You are responsible for filtering correct partitions when reading the data in the downstream assets. Extra dependencies can be installed with `pip install 'dagster-polars[deltalake]'`.
 - `BigQueryPolarsIOManager` - for reading and writing data from/to [BigQuery](https://cloud.google.com/bigquery). Supports writing partitioned tables (`"partition_expr"` input metadata key must be specified). Extra dependencies can be installed with `pip install 'dagster-polars[gcp]'`.

## Quickstart

### Installation

```shell
pip install dagster-polars
```

To use the `BigQueryPolarsIOManager` you need to install the `gcp` extra:
```shell
pip install 'dagster-polars[gcp]'
```


### Usage & Examples
```python
import polars as pl
from dagster import AssetIn, Definitions, asset
from dagster_polars import PolarsDeltaIOManager, PolarsParquetIOManager


@asset(io_manager_key="polars_parquet_io_manager")
def upstream() -> pl.DataFrame:
    return pl.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})


@asset(
    io_manager_key="polars_parquet_io_manager",
    ins={"upstream": AssetIn(metadata={"columns": ["a"]})}  # explicitly specify which columns to load
)
def downstream(
    upstream: pl.LazyFrame  # the type annotation controls whether we load an eager or lazy DataFrame
) -> pl.DataFrame:
    df = ...  # some lazy operations with `upstream`
    return df.collect()


@asset(
    io_manager_key="polars_delta_io_manager",
    metadata={
        "mode": "append"  # append to the existing table instead of overwriting it
    }
)
def downstream_append(
    upstream: pl.DataFrame
) -> pl.DataFrame:
    return upstream


base_dir = "/remote/or/local/path"  # s3://my-bucket/... or gs://my-bucket/... also works!

definitions = Definitions(
    assets=[upstream, downstream, downstream_append],
    resources={
        "polars_parquet_io_manager": PolarsParquetIOManager(
            base_dir=base_dir
        ),
        "polars_delta_io_manager": PolarsDeltaIOManager(
            base_dir=base_dir
        ),
    }
)
```

## Development

### Installation
```shell
poetry install
poetry run pre-commit install
```

### Testing
```shell
poetry run pytest
```

## Ideas
 - Data validation like in [dagster-pandas](https://docs.dagster.io/integrations/pandas#validating-pandas-dataframes-with-dagster-types)
 - Maybe use `DagsterTypeLoader` ?
