"""Module to create and configure a Spark session - minimal."""
from typing import Dict, List, Union, Any
from pathlib import Path
import psutil
import logging

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

import teehr.const as const

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Note: Scala version: 2.13 in pyspark 4.0
SCALA_VERSION = "2.13"
PYSPARK_VERSION = "4.0"
ICEBERG_VERSION = "1.10.0"
SEDONA_VERSION = "1.8.0"


def create_spark_session(
    # Existing parameters for backward compatibility
    local_warehouse_dir: Union[str, Path] = None,
    local_catalog_name: str = "local",
    local_catalog_type: str = "hadoop",
    remote_warehouse_dir: str = const.WAREHOUSE_S3_PATH,
    remote_catalog_name: str = "iceberg",
    remote_catalog_type: str = "rest",
    remote_catalog_uri: str = const.CATALOG_REST_URI,
    driver_memory: Union[str, int, float] = None,
    driver_maxresultsize: Union[str, int, float] = None,
    app_name: str = "TEEHR Evaluation",

    # New simple extensibility parameters
    extra_packages: List[str] = None,
    extra_configs: Dict[str, str] = None,
    config_preset: str = "default",
    debug_config: bool = True  # Log final config for debugging
) -> SparkSession:
    """Create and return a Spark session for evaluation."""
    # Get default configurations
    default_configs = _get_default_spark_configs(config_preset)

    # Handle memory settings
    memory_info = psutil.virtual_memory()
    if driver_memory is None:
        driver_memory = 0.75 * memory_info.available / (1024**3)
    if driver_maxresultsize is None:
        driver_maxresultsize = 0.5 * driver_memory

    # Build Spark session
    builder = SparkSession.builder.appName(app_name)

    # Apply default packages
    packages = default_configs["packages"].copy()
    if extra_packages:
        packages.extend(extra_packages)
    builder = builder.config("spark.jars.packages", ",".join(packages))

    # Apply default Spark configurations
    for key, value in default_configs["spark_configs"].items():
        builder = builder.config(key, value)

    # Apply memory settings
    builder = builder.config("spark.driver.memory", f"{int(driver_memory)}g")
    builder = builder.config("spark.driver.maxResultSize", f"{int(driver_maxresultsize)}g")

    # Apply catalog configurations
    _configure_catalogs(
        builder,
        local_warehouse_dir,
        local_catalog_name,
        local_catalog_type,
        remote_warehouse_dir,
        remote_catalog_name,
        remote_catalog_type,
        remote_catalog_uri
    )

    # Apply any extra user configurations (these override defaults)
    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    # Create session and set catalogs
    spark = builder.getOrCreate()

    # Can inspect final config after creation
    if debug_config:
        logger.info("Final Spark configuration:")
        for key, value in spark.conf.getAll():
            if key.startswith("spark."):
                logger.info(f"  {key}: {value}")



    if local_warehouse_dir is not None:
        spark.catalog.setCurrentCatalog(catalogName=local_catalog_name)
    spark.catalog.setCurrentCatalog(catalogName=remote_catalog_name)

    sedona_spark = SedonaContext.create(spark)
    logger.info("Spark session created for TEEHR Evaluation.")
    return sedona_spark


def _get_default_spark_configs(preset: str) -> Dict[str, Any]:
    """Get default Spark configurations based on preset."""
    base_packages = [
        f"org.apache.sedona:sedona-spark-shaded-{PYSPARK_VERSION}_{SCALA_VERSION}:{SEDONA_VERSION}",
        f"org.apache.iceberg:iceberg-spark-runtime-{PYSPARK_VERSION}_{SCALA_VERSION}:{ICEBERG_VERSION}",
        "org.datasyslab:geotools-wrapper:1.8.0-33.1",
        f"org.apache.iceberg:iceberg-spark-extensions-{PYSPARK_VERSION}_{SCALA_VERSION}:{ICEBERG_VERSION}",
        "org.apache.hadoop:hadoop-aws:3.4.2",
        "com.amazonaws:aws-java-sdk-bundle:1.12.791"
    ]

    base_configs = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    }

    if preset == "minimal":
        # Minimal config for testing
        return {
            "packages": base_packages[:2],  # Just Sedona and Iceberg
            "spark_configs": {
                "spark.sql.session.timeZone": "UTC",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
            }
        }
    elif preset == "s3_optimized":
        # S3-optimized config
        s3_configs = base_configs.copy()
        s3_configs.update({
            "spark.hadoop.fs.s3a.multipart.size": "268435456",  # 256MB
            "spark.hadoop.fs.s3a.connection.maximum": "200",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "268435456"
        })
        return {"packages": base_packages, "spark_configs": s3_configs}
    else:
        # Default config
        return {"packages": base_packages, "spark_configs": base_configs}


def _configure_catalogs(
    builder,
    local_warehouse_dir,
    local_catalog_name,
    local_catalog_type,
    remote_warehouse_dir,
    remote_catalog_name,
    remote_catalog_type,
    remote_catalog_uri
):
    """Configure Iceberg catalogs."""
    # Handle local warehouse directory
    if local_warehouse_dir is not None:
        local_warehouse_dir = Path(local_warehouse_dir) / local_catalog_name
        local_warehouse_dir = local_warehouse_dir.as_posix()

        builder.config(f"spark.sql.catalog.{local_catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        builder.config(f"spark.sql.catalog.{local_catalog_name}.type", local_catalog_type)
        builder.config(f"spark.sql.catalog.{local_catalog_name}.warehouse", local_warehouse_dir)

    # Handle remote warehouse
    if isinstance(remote_warehouse_dir, Path):
        remote_warehouse_dir = remote_warehouse_dir.as_posix()

    builder.config(f"spark.sql.catalog.{remote_catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    builder.config(f"spark.sql.catalog.{remote_catalog_name}.type", remote_catalog_type)
    builder.config(f"spark.sql.catalog.{remote_catalog_name}.uri", remote_catalog_uri)
    builder.config(f"spark.sql.catalog.{remote_catalog_name}.warehouse", remote_warehouse_dir)
    builder.config(f"spark.sql.catalog.{remote_catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")


if __name__ == "__main__":
    # Example 1: Default usage (unchanged)
    spark = create_spark_session(local_warehouse_dir="/tmp/warehouse")

    # Example 2: Add custom packages
    spark = create_spark_session(
        local_warehouse_dir="/tmp/warehouse",
        extra_packages=["com.example:my-package:1.0.0"]
    )

    # Example 3: Override specific settings
    spark = create_spark_session(
        local_warehouse_dir="/tmp/warehouse",
        extra_configs={
            "spark.sql.adaptive.enabled": "false",
            "spark.driver.maxResultSize": "8g",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "512MB"
        }
    )

    # Example 4: Use different preset
    spark = create_spark_session(
        local_warehouse_dir="/tmp/warehouse",
        config_preset="s3_optimized"
    )

    # Example 5: Combine everything
    spark = create_spark_session(
        local_warehouse_dir="/tmp/warehouse",
        config_preset="minimal",
        extra_packages=["org.postgresql:postgresql:42.7.8"],
        extra_configs={
            "spark.sql.adaptive.enabled": "true",
            "spark.eventLog.enabled": "true"
        }
    )