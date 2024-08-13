"""Functions to create joined timeseries."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, FloatType
from pathlib import Path
from typing import Union
import teehr.const as const
from teehr.loading.utils import (
    validate_dataset_structure,
)
import logging

logger = logging.getLogger(__name__)


def create_joined_timeseries(
        spark: SparkSession,
        dataset_dir: Union[str, Path]
):
    """Create joined timeseries.

    Parameters
    ----------
    spark: SparkSession
        SparkSession object
    eval_path: Union[str, Path]
        Eval directory path

    Returns
    -------
    None
    """
    if not validate_dataset_structure(dataset_dir):
        raise ValueError(f"Database structure is not valid: {dataset_dir}.")

    # Read all the files in the given directory
    primary_files = Path(
        dataset_dir,
        const.PRIMARY_TIMESERIES_DIR
    ).glob("**/*.parquet")
    primary_df = (
        spark.read.format("parquet")
        # .option("recursiveFileLookup", "true")
        .load(
            [str(p) for p in primary_files]
        )
    )
    secondary_files = Path(
        dataset_dir,
        const.SECONDARY_TIMESERIES_DIR
    ).glob("**/*.parquet")
    secondary_df = (
        spark.read.format("parquet")
        .option("recursiveFileLookup", "true")
        .load(
            [str(p) for p in secondary_files]
        )
    )
    crosswalk_files = Path(
        dataset_dir,
        const.LOCATION_CROSSWALKS_DIR
    ).glob("**/*.parquet")
    crosswalk_df = (
        spark.read.format("parquet")
        .option("recursiveFileLookup", "true")
        .load(
            [str(p) for p in crosswalk_files]
        )
    )
    location_attributes_files = Path(
        dataset_dir,
        const.LOCATION_ATTRIBUTES_DIR
    ).glob("**/*.parquet")
    location_attributes_df = (
        spark.read.format("parquet")
        .option("recursiveFileLookup", "true")
        .load(
            [str(p) for p in location_attributes_files]
        )
    )

    primary_df.createTempView("pf")
    secondary_df.createTempView("sf")
    crosswalk_df.createTempView("cf")

    # Join the primary and secondary timeseries based on the crosswalk table
    logger.info("Joining primary and secondary timeseries")
    joined = spark.sql("""
        SELECT
            sf.reference_time
            , sf.value_time as value_time
            , pf.location_id as primary_location_id
            , sf.location_id as secondary_location_id
            , pf.value as primary_value
            , sf.value as secondary_value
            , sf.configuration_name
            , sf.unit_name
            , sf.variable_name
        FROM sf sf
        JOIN cf cf
            on cf.secondary_location_id = sf.location_id
        JOIN pf pf
            on cf.primary_location_id = pf.location_id
            and sf.value_time = pf.value_time
            and sf.unit_name = pf.unit_name
            and sf.variable_name = pf.variable_name
    """)
    joined.createTempView("joined")

    # Add attributes
    logger.info("Adding attributes")

    # Get distinct attribute names
    distinct_attributes = (
        location_attributes_df.select("attribute_name")
        .distinct().rdd.flatMap(lambda x: x).collect()
    )

    # Pivot the table
    pivot_df = (
        location_attributes_df.groupBy("location_id")
        .pivot("attribute_name", distinct_attributes).agg({"value": "max"})
    )

    # Show the result
    pivot_df.createTempView("attrs")

    joined = spark.sql("""
        SELECT
            joined.*
            , attrs.*
        FROM joined
        JOIN attrs
            on joined.primary_location_id = attrs.location_id
    """)

    # Add user defined fields
    logger.info("Adding user defined fields")

    logger.info("Adding month from date")

    @udf(returnType=IntegerType())
    def month_from_date(date):
        return date.month

    joined = joined.withColumn(
        "month",
        month_from_date("value_time")
    )

    logger.info("Adding water year from date")

    @udf(returnType=IntegerType())
    def water_year_from_date(date):
        if date.month < 10:
            return date.year - 1
        else:
            return date.year

    joined = joined.withColumn(
        "water_year",
        water_year_from_date("value_time")
    )

    logger.info("Adding normalized flow")

    @udf(returnType=FloatType())
    def normalized_value(value: float, area: float):
        return float(value) / float(area)

    joined = joined.withColumn(
        "primary_normalized_flow",
        normalized_value("primary_value", "drainage_area")
    )

    joined = joined.withColumn(
        "secondary_normalized_flow",
        normalized_value("secondary_value", "drainage_area")
    )

    # Write the joined timeseries to the disk
    # Depending on the size of the data, repartitioning might be needed
    logger.info("Writing joined timeseries to disk")
    (
        joined.write
        .partitionBy("configuration_name", "variable_name")
        .parquet(
            str(Path(dataset_dir, const.JOINED_TIMESERIES_DIR)),
            mode="overwrite"
        )
    )
