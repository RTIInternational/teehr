"""Functions to query the joined timeseries data."""
from typing import Union
from pathlib import Path
from pyspark.sql import SparkSession
import pandas as pd


def get_joined_timeseries(
    spark: SparkSession,
    dirpath: Union[str, Path],
) -> pd.DataFrame:
    """Get the timeseries data.

    Needs filters to get specific data.
    """
    # Read all the files in the given directory
    joined_df = (
        spark.read.format("parquet")
        .load(str(dirpath))
    )
    return joined_df.toPandas()
