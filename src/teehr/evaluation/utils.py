"""Utility functions for the evaluation class."""
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
# SEDONA_VERSION = "1.8.0"

SPARK_HOME = pyspark.__path__[0]


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
    catalog_name: str = "local",
    catalog_type: str = "hadoop",
    driver_memory: Union[str, int, float] = None,
    driver_maxresultsize: Union[str, int, float] = None
) -> SparkSession:
    """Create and return a Spark session for evaluation."""
    memory_info = psutil.virtual_memory()
    if driver_memory is None:
        driver_memory = 0.75 * memory_info.available / (1024**3)
    if driver_maxresultsize is None:
        driver_maxresultsize = 0.5 * driver_memory

    # TEMP! Copy in the sedona 1.8 snapshot jar for testing.
    dest_dir = f"{SPARK_HOME}/jars"
    sedona_jar_path = Path(__file__).parents[3] / "playground" / "iceberg" / "sedona-spark-shaded-4.0_2.13-1.8.1-SNAPSHOT.jar"
    shutil.copy(sedona_jar_path, dest_dir)

    conf = (
        SparkConf()
        .setAppName("TEEHR")
        .setMaster("local[*]")
        # .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.driver.host", "localhost")
        .set("spark.driver.bindAddress", "localhost")
        .set("spark.driver.memory", f"{int(driver_memory)}g")
        .set("spark.driver.maxResultSize", f"{int(driver_maxresultsize)}g")
        .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
        .set("spark.sql.execution.arrow.pyspark.enabled", "true")
        .set("spark.sql.session.timeZone", "UTC")
        .set(
            "spark.jars.repositories",
            "https://artifacts.unidata.ucar.edu/repository/unidata-all,"
            "https://repository.apache.org/content/repositories/snapshots,"
            "https://repository.apache.org/content/groups/snapshots"
        )
        .set(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,"
            # f"org.apache.sedona:sedona-spark-shaded-{pyspark_version}_{scala_version}:{sedona_version},"
            f"org.apache.iceberg:iceberg-spark-runtime-{PYSPARK_VERSION}_{SCALA_VERSION}:{ICEBERG_VERSION}-SNAPSHOT,"
            # "org.datasyslab:geotools-wrapper:1.8.0-33.1,"  IS THIS NEEDED?
            f"org.apache.iceberg:iceberg-spark-extensions-{PYSPARK_VERSION}_{SCALA_VERSION}:{ICEBERG_VERSION}-SNAPSHOT,"
        )
        # .set("spark.jars", f"{dest_dir}/sedona-spark-shaded-4.0_2.13-1.8.1-SNAPSHOT.jar")
        .set("spark.jars", "sedona-spark-shaded-4.0_2.13-1.8.1-SNAPSHOT.jar")
        .set("spark.sql.parquet.enableVectorizedReader", "false")
        .set(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .set(
            f"spark.sql.catalog.{catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .set(
            f"spark.sql.catalog.{catalog_name}.type", catalog_type
        )
        .set(
            f"spark.sql.catalog.{catalog_name}.warehouse",
            f"{warehouse_path}/{catalog_name}"
        )
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sedona_spark = SedonaContext.create(spark)
    logger.info("Spark session created for TEEHR Evaluation.")

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
