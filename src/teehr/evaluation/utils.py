"""Utility functions for the evaluation class."""
import logging
from pathlib import Path
from enum import Enum
from typing import Union
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


def get_spark_schema(
        spark: SparkSession,
        data_dir: Union[Path, str]
) -> StructType:
    """Get the schema of the dataset."""
    df = (
        spark.read.format("parquet")
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .load(str(data_dir))
    )
    return df.schema


def get_joined_timeseries_fields(
    spark: SparkSession,
    joined_timeseries_dir: Union[Path, str]
) -> Enum:
    """Get the field names from the joined timeseries table."""
    if len(list(Path(joined_timeseries_dir).glob("**/*.parquet"))) == 0:
        logger.error(f"No parquet files in {joined_timeseries_dir}.")
        raise FileNotFoundError
    else:
        logger.info(f"Reading fields from {joined_timeseries_dir}.")
        schema = get_spark_schema(spark, joined_timeseries_dir)
        fields_list = [field.name for field in schema.fields]
        return Enum("Fields", {field: field for field in fields_list})
