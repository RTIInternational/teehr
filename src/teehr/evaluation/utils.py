"""Utility functions for the evaluation class."""
import logging
from pathlib import Path
from typing import Union
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from teehr.fetching.const import NWM_VARIABLE_MAPPER, VARIABLE_NAME

logger = logging.getLogger(__name__)


def get_spark_schema(
        spark: SparkSession,
        data_dir: Union[Path, str]
) -> StructType:
    """Get the schema of the dataset."""
    df = (
        spark.read.format("parquet")
        # .option("recursiveFileLookup", "true")
        # .option("mergeSchema", "true")
        .load(str(data_dir))
    )
    return df.schema


def get_schema_variable_name(variable_name: str) -> str:
    """Get the variable name from the Evaluation schema."""
    logger.info(f"Getting schema variable name for {variable_name}.")
    return NWM_VARIABLE_MAPPER[VARIABLE_NAME]. \
        get(variable_name, variable_name)
