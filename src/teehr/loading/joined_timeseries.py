"""Functions to create joined timeseries."""
from pyspark.sql import SparkSession, DataFrame
from pathlib import Path
from typing import Union
import teehr.const as const
from teehr.loading.utils import (
    validate_dataset_structure,
)
import logging
import sys

logger = logging.getLogger(__name__)


def create_joined_df(
    spark: SparkSession,
    dataset_dir: Union[str, Path]
) -> DataFrame:
    """Create an initial joined timeseries dataframe.

    Parameters
    ----------
    spark: SparkSession
        SparkSession object
    dataset_dir: Union[str, Path]
        Dataset directory path

    Returns
    -------
    DataFrame
        DataFrame with joined timeseries.
    """
    if not validate_dataset_structure(dataset_dir):
        raise ValueError(f"Database structure is not valid: {dataset_dir}.")

    # Read all the files in the given directory
    primary_df = (
        spark.read.format("parquet")
        # .option("recursiveFileLookup", "true")
        # .option("mergeSchema", "true")
        .load(
            str(Path(dataset_dir, const.PRIMARY_TIMESERIES_DIR))
        )
    )
    secondary_df = (
        spark.read.format("parquet")
        # .option("recursiveFileLookup", "true")
        # .option("mergeSchema", "true")
        .load(
            str(Path(dataset_dir, const.SECONDARY_TIMESERIES_DIR))
        )
    )
    crosswalk_df = (
        spark.read.format("parquet")
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .load(
            str(Path(dataset_dir, const.LOCATION_CROSSWALKS_DIR))
        )
    )

    primary_df.createTempView("pf")
    secondary_df.createTempView("sf")
    crosswalk_df.createTempView("cf")

    # Join the primary and secondary timeseries based on the crosswalk table
    logger.info("Joining primary and secondary timeseries")
    joined_df = spark.sql("""
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

    spark.catalog.dropTempView("pf")
    spark.catalog.dropTempView("sf")
    spark.catalog.dropTempView("cf")

    return joined_df


def add_attr_to_joined_df(
    spark: SparkSession,
    joined_df: DataFrame,
    dataset_dir: Union[str, Path]
) -> DataFrame:
    """Add attributes to the joined timeseries dataframe.

    Parameters
    ----------
    spark: SparkSession
        SparkSession object
    joined_df: DataFrame
        Joined timeseries DataFrame
    dataset_dir: Union[str, Path]
        Dataset directory path

    Returns
    -------
    DataFrame
        Joined timeseries DataFrame with attributes
    """
    dataset_dir = Path(dataset_dir)
    if len(list(
        Path(dataset_dir, const.LOCATION_ATTRIBUTES_DIR).glob("**/*.parquet")
    )) == 0:
        logger.info(
            f"No parquet files in {dataset_dir}.  Skipping add attributes."
        )
        return joined_df
    else:
        logger.info("Adding attributes")

        location_attributes_df = (
            spark.read.format("parquet")
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
            .load(
                str(Path(dataset_dir, const.LOCATION_ATTRIBUTES_DIR))
            )
        )

        joined_df.createTempView("joined")

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

        # Create a view
        pivot_df.createTempView("attrs")

        joined_df = spark.sql("""
            SELECT
                joined.*
                , attrs.*
            FROM joined
            JOIN attrs
                on joined.primary_location_id = attrs.location_id
        """)

        spark.catalog.dropTempView("joined")
        spark.catalog.dropTempView("attrs")

        return joined_df


def create_joined_timeseries_dataset(
    spark: SparkSession,
    dataset_dir: Union[str, Path],
    scripts_dir: Union[str, Path],
    execute_udf: bool = False,
):
    """Create joined timeseries.

    Parameters
    ----------
    spark: SparkSession
        SparkSession object
    eval_path: Union[str, Path]
        ev directory path

    Returns
    -------
    None
    """
    joined_df = create_joined_df(spark, dataset_dir)
    joined_df = add_attr_to_joined_df(spark, joined_df, dataset_dir)

    if execute_udf:
        # This is kinda hacky, but it works
        try:
            sys.path.append(str(Path(scripts_dir)))
            import user_defined_fields as udf # noqa
            joined_df = udf.add_user_defined_fields(joined_df)
        except ImportError:
            logger.info(
                f"No user-defined fields found in {scripts_dir}."
                "Not adding user-defined fields."
            )

    # Write the joined timeseries to the disk
    # Depending on the size of the data, repartitioning might be needed
    logger.info("Writing joined timeseries to disk")
    (
        joined_df.write
        .partitionBy("configuration_name", "variable_name")
        .format("parquet")
        .mode("overwrite")
        .save(
            str(Path(dataset_dir, const.JOINED_TIMESERIES_DIR)),
        )
    )
