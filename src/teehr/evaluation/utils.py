"""Utility functions for the evaluation class."""
# flake8: noqa
import logging
import fnmatch
from typing import List, Union
from pathlib import Path
import psutil
import shutil

from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark
from sedona.spark import SedonaContext

from teehr.utils.s3path import S3Path

logger = logging.getLogger(__name__)


PIPE = "│"
ELBOW = "└──"
TEE = "├──"
PIPE_PREFIX = "│   "
SPACE_PREFIX = "    "


# Note: Scala version: 2.13 in pyspark 4.0
SCALA_VERSION = "2.13"
PYSPARK_VERSION = "4.0"
ICEBERG_VERSION = "1.10.0"
SEDONA_VERSION = "1.8.0"

SPARK_HOME = pyspark.__path__[0]


def get_table_instance(ev, table_name: str):
    """Get a table instance from the catalog."""
    table = getattr(ev, table_name, None)
    return table


def copy_schema_dir(
    target_dir: Union[str, Path, S3Path]
):
    """Copy the schema directory from source to target."""
    shutil.copytree(
        src=Path(__file__).parent.parent / "migrations",
        dst=Path(target_dir, "migrations"),
        dirs_exist_ok=True
    )
    pass


def create_spark_session(
    warehouse_path: Union[str, Path, S3Path],
    catalog_name: str,
    catalog_type: str,
    catalog_uri: str,
    driver_memory: Union[str, int, float],
    driver_maxresultsize: Union[str, int, float],
    app_name: str,
) -> SparkSession:
    """Create and return a Spark session for evaluation."""
    memory_info = psutil.virtual_memory()
    if driver_memory is None:
        driver_memory = 0.75 * memory_info.available / (1024**3)
    if driver_maxresultsize is None:
        driver_maxresultsize = 0.5 * driver_memory

    # =================================================
    # Use the builder approach
    builder = SparkSession.builder.appName(app_name)

    # EMR comes with pre-installed jars, use compatible versions for PySpark 4
    builder = builder.config(
        "spark.jars.packages",
        f"org.apache.sedona:sedona-spark-shaded-{PYSPARK_VERSION}_{SCALA_VERSION}:{SEDONA_VERSION},"
        f"org.apache.iceberg:iceberg-spark-runtime-{PYSPARK_VERSION}_{SCALA_VERSION}:{ICEBERG_VERSION},"
        "org.datasyslab:geotools-wrapper:1.8.0-33.1,"  # for raster ops
        f"org.apache.iceberg:iceberg-spark-extensions-{PYSPARK_VERSION}_{SCALA_VERSION}:{ICEBERG_VERSION},"
        "org.apache.hadoop:hadoop-aws:3.4.1,"  # SEEMS TO CAUSE HIGH MEMORY USAGE? Also 3.4.2 seems to fail.
        "com.amazonaws:aws-java-sdk-bundle:1.12.791"
    )

    # Iceberg extensions (enable iceberg-specific SQL commands such as time travel, merge-into, etc.)
    builder = builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

    # Catalog configuration
    builder = builder.config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    builder = builder.config(f"spark.sql.catalog.{catalog_name}.type", catalog_type)
    builder = builder.config(f"spark.sql.catalog.{catalog_name}.uri", catalog_uri)
    builder = builder.config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
    builder = builder.config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    # builder = builder.config("spark.sql.defaultCatalog", catalog_name)

    # EMR optimizations
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    builder = builder.config("spark.sql.adaptive.skewJoin.enabled", "true")
    builder = builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    builder = builder.config("spark.driver.memory", f"{int(driver_memory)}g")
    builder = builder.config("spark.driver.maxResultSize", f"{int(driver_maxresultsize)}g")

    # # Other optimizations to consider
    # # Memory management
    # builder = builder.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
    # builder = builder.config("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", "0")

    # # I/O optimizations
    # builder = builder.config("spark.sql.files.maxPartitionBytes", "128MB")
    # builder = builder.config("spark.sql.files.openCostInBytes", "4MB")

    # # Dynamic allocation (useful for EMR)
    # builder = builder.config("spark.dynamicAllocation.enabled", "true")
    # builder = builder.config("spark.dynamicAllocation.minExecutors", "1")
    # builder = builder.config("spark.dynamicAllocation.maxExecutors", "8")

    # S3 stuff
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    builder = builder.config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

    spark = builder.getOrCreate()
    spark.catalog.setCurrentCatalog(catalogName=catalog_name)
    sedona_spark = SedonaContext.create(spark)
    logger.info("Spark session created for TEEHR Evaluation.")
    # =================================================

    # # The old way
    # conf = (
    #     SparkConf()
    #     .setAppName(app_name)
    #     .setMaster("local[*]")
    #     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  # can improve performance?
    #     .set("spark.driver.memory", f"{int(driver_memory)}g")
    #     .set("spark.driver.maxResultSize", f"{int(driver_maxresultsize)}g")
    #     .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    #     .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    #     .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    #     .set("spark.sql.execution.arrow.pyspark.enabled", "true")
    #     .set("spark.sql.session.timeZone", "UTC")
    #     # .set("spark.sql.parquet.enableVectorizedReader", "false")  # why was it set to false?
    #     .set(
    #         "spark.jars.packages",
    #         "org.apache.hadoop:hadoop-aws:3.4.1,"  # SEEMS TO CAUSE HIGH MEMORY USAGE? Also 3.4.2 seems to fail.
    #         "com.amazonaws:aws-java-sdk-bundle:1.12.791,"
    #         f"org.apache.sedona:sedona-spark-shaded-{PYSPARK_VERSION}_{SCALA_VERSION}:{SEDONA_VERSION},"
    #         f"org.apache.iceberg:iceberg-spark-runtime-{PYSPARK_VERSION}_{SCALA_VERSION}:{ICEBERG_VERSION},"
    #         "org.datasyslab:geotools-wrapper:1.8.0-33.1,"  # for raster ops
    #         f"org.apache.iceberg:iceberg-spark-extensions-{PYSPARK_VERSION}_{SCALA_VERSION}:{ICEBERG_VERSION},"
    #     )
    #     .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    #     .set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    #     .set(f"spark.sql.catalog.{catalog_name}.type", catalog_type)
    #     .set(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse_path}/{catalog_name}")
    #     .set(f"spark.sql.catalog.{catalog_name}.uri", catalog_uri)
    #     .set(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    #     .set("spark.sql.defaultCatalog", catalog_name)
    # )

    # spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # sedona_spark = SedonaContext.create(spark)
    # logger.info("Spark session created for TEEHR Evaluation.")

    return sedona_spark


def print_tree(
    path,
    prefix="",
    exclude_patterns: List[str] = [""],
    max_depth: int = -1,
    current_depth: int = 0
):
    """Print the directory tree structure."""
    if max_depth != -1 and current_depth > max_depth:
        return

    # Get all files and directories in the current path
    paths = list(Path(path).glob("*"))

    # Exclude specific files and directories
    filtered_files = [f for f in paths if not any(fnmatch.fnmatch(f.name, p) for p in exclude_patterns)]

    # Print the directory tree structure
    entries_count = len(filtered_files)

    for index, full_path in enumerate(filtered_files):
        name = full_path.relative_to(path)
        connector = ELBOW if index == entries_count - 1 else TEE

        if full_path.is_dir():
            print(f"{prefix}{connector} {name}")
            new_prefix = prefix + (PIPE_PREFIX if index != entries_count - 1 else SPACE_PREFIX)
            print_tree(
                path=full_path,
                prefix=new_prefix,
                exclude_patterns=exclude_patterns,
                max_depth=max_depth,
                current_depth=current_depth + 1
            )
        else:
            print(f"{prefix}{connector} {name}")
