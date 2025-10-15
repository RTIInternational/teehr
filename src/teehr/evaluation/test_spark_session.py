"""Module to create and configure a Spark session for evaluation tasks."""
from dataclasses import dataclass, field
from typing import Dict, Optional, Union
from enum import Enum
from pathlib import Path
import psutil

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

import teehr.const as const

# Note: Scala version: 2.13 in pyspark 4.0
SCALA_VERSION = "2.13"
PYSPARK_VERSION = "4.0"
ICEBERG_VERSION = "1.10.0"
SEDONA_VERSION = "1.8.0"


class CatalogType(Enum):
    HADOOP = "hadoop"
    REST = "rest"
    HIVE = "hive"
    JDBC = "jdbc"


@dataclass
class CatalogConfig:
    """Configuration for a single catalog."""

    name: str
    catalog_type: CatalogType
    warehouse_dir: Optional[str] = None
    uri: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)


class SparkConfigBuilder:
    """Builder for Spark session configuration."""

    def __init__(self):
        self.app_name = "TEEHR Evaluation"
        self.catalogs: Dict[str, CatalogConfig] = {}
        self.spark_configs: Dict[str, str] = {}
        self.memory_configs = {}
        self.package_dependencies = []
        self.extensions = []

    def with_app_name(self, name: str) -> 'SparkConfigBuilder':
        self.app_name = name
        return self

    def add_catalog(self, config: CatalogConfig) -> 'SparkConfigBuilder':
        self.catalogs[config.name] = config
        return self

    def add_iceberg_catalog(
        self, name: str,
        warehouse_dir: str,
        catalog_type: CatalogType = CatalogType.HADOOP,
        uri: Optional[str] = None
    ) -> 'SparkConfigBuilder':
        config = CatalogConfig(
            name=name,
            catalog_type=catalog_type,
            warehouse_dir=warehouse_dir,
            uri=uri
        )
        return self.add_catalog(config)

    def with_memory_config(
        self,
        driver_memory: str = None,
        driver_max_result_size: str = None,
        executor_memory: str = None
    ) -> 'SparkConfigBuilder':
        if driver_memory:
            self.memory_configs["spark.driver.memory"] = driver_memory
        if driver_max_result_size:
            self.memory_configs["spark.driver.maxResultSize"] = driver_max_result_size
        if executor_memory:
            self.memory_configs["spark.executor.memory"] = executor_memory
        return self

    def add_package(self, package: str) -> 'SparkConfigBuilder':
        self.package_dependencies.append(package)
        return self

    def add_extension(self, extension: str) -> 'SparkConfigBuilder':
        self.extensions.append(extension)
        return self

    def with_config(self, key: str, value: str) -> 'SparkConfigBuilder':
        self.spark_configs[key] = value
        return self

    def enable_adaptive_query_execution(self) -> 'SparkConfigBuilder':
        aqe_configs = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true"
        }
        self.spark_configs.update(aqe_configs)
        return self

    def enable_s3_optimization(self) -> 'SparkConfigBuilder':
        s3_configs = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            "spark.hadoop.fs.s3a.multipart.size": "268435456",  # 256MB
            "spark.hadoop.fs.s3a.connection.maximum": "200"
        }
        self.spark_configs.update(s3_configs)
        return self

    def build(self) -> SparkSession:
        """Build the Spark session with all configurations."""
        builder = SparkSession.builder.appName(self.app_name)

        # Add packages
        if self.package_dependencies:
            builder = builder.config("spark.jars.packages", ",".join(self.package_dependencies))

        # Add extensions
        if self.extensions:
            builder = builder.config("spark.sql.extensions", ",".join(self.extensions))

        # Add memory configs
        for key, value in self.memory_configs.items():
            builder = builder.config(key, value)

        # Add custom configs
        for key, value in self.spark_configs.items():
            builder = builder.config(key, value)

        # Configure catalogs
        for catalog in self.catalogs.values():
            self._configure_catalog(builder, catalog)

        spark = builder.getOrCreate()

        # Set default catalog if any
        if self.catalogs:
            default_catalog = list(self.catalogs.keys())[0]
            spark.catalog.setCurrentCatalog(default_catalog)

        return SedonaContext.create(spark)

    def _configure_catalog(self, builder: SparkSession.Builder, catalog: CatalogConfig):
        """Configure a single catalog."""
        prefix = f"spark.sql.catalog.{catalog.name}"

        if catalog.catalog_type == CatalogType.HADOOP:
            builder.config(f"{prefix}", "org.apache.iceberg.spark.SparkCatalog")
            builder.config(f"{prefix}.type", "hadoop")
            if catalog.warehouse_dir:
                builder.config(f"{prefix}.warehouse", catalog.warehouse_dir)

        elif catalog.catalog_type == CatalogType.REST:
            builder.config(f"{prefix}", "org.apache.iceberg.spark.SparkCatalog")
            builder.config(f"{prefix}.type", "rest")
            if catalog.uri:
                builder.config(f"{prefix}.uri", catalog.uri)
            if catalog.warehouse_dir:
                builder.config(f"{prefix}.warehouse", catalog.warehouse_dir)

        # Add any custom properties
        for key, value in catalog.properties.items():
            builder.config(f"{prefix}.{key}", value)


class SparkPresets:
    """Predefined Spark configurations for common use cases."""

    @staticmethod
    def default_teehr_config() -> SparkConfigBuilder:
        """Default TEEHR configuration."""
        return (SparkConfigBuilder()
                .with_app_name("TEEHR Evaluation")
                .add_package(f"org.apache.sedona:sedona-spark-shaded-{PYSPARK_VERSION}_{SCALA_VERSION}:{SEDONA_VERSION}")
                .add_package(f"org.apache.iceberg:iceberg-spark-runtime-{PYSPARK_VERSION}_{SCALA_VERSION}:{ICEBERG_VERSION}")
                .add_package(f"org.apache.iceberg:iceberg-spark-extensions-{PYSPARK_VERSION}_{SCALA_VERSION}:{ICEBERG_VERSION}")
                .add_package("org.datasyslab:geotools-wrapper:1.8.0-33.1")
                .add_extension("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .enable_adaptive_query_execution()
                .with_config("spark.sql.session.timeZone", "UTC")
                .with_config("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))

    @staticmethod
    def local_development() -> SparkConfigBuilder:
        """Configuration for local development."""
        memory_info = psutil.virtual_memory()
        driver_memory = int(0.75 * memory_info.available / (1024**3))

        return (SparkPresets.default_teehr_config()
                .with_memory_config(
                    driver_memory=f"{driver_memory}g",
                    driver_max_result_size=f"{int(0.5 * driver_memory)}g"
                ))

    @staticmethod
    def s3_production() -> SparkConfigBuilder:
        """Configuration for S3-based production workloads."""
        return (SparkPresets.default_teehr_config()
                .enable_s3_optimization()
                .add_package("org.apache.hadoop:hadoop-aws:3.4.2")
                .add_package("com.amazonaws:aws-java-sdk-bundle:1.12.791")
                .with_memory_config(driver_memory="8g", driver_max_result_size="4g"))


def create_spark_session(
    config_builder: Optional[SparkConfigBuilder] = None,
    preset: Optional[str] = None,
    # Backward compatibility parameters
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
    additional_configs: Dict[str, str] = None
) -> SparkSession:
    """Create and return a Spark session for evaluation."""

    # Use builder if provided
    if config_builder:
        return config_builder.build()

    # Use preset if specified
    if preset:
        if preset == "local":
            builder = SparkPresets.local_development()
        elif preset == "s3":
            builder = SparkPresets.s3_production()
        else:
            raise ValueError(f"Unknown preset: {preset}")
    else:
        # Create default configuration
        builder = SparkPresets.local_development()

    # Apply backward compatibility parameters
    if local_warehouse_dir:
        local_warehouse_path = Path(local_warehouse_dir) / local_catalog_name
        builder.add_iceberg_catalog(
            local_catalog_name,
            str(local_warehouse_path),
            CatalogType(local_catalog_type)
        )

    builder.add_iceberg_catalog(
        remote_catalog_name,
        remote_warehouse_dir,
        CatalogType(remote_catalog_type),
        remote_catalog_uri
    )

    # Apply memory settings
    if driver_memory or driver_maxresultsize:
        memory_info = psutil.virtual_memory()
        if driver_memory is None:
            driver_memory = int(0.75 * memory_info.available / (1024**3))
        if driver_maxresultsize is None:
            driver_maxresultsize = int(0.5 * driver_memory)

        builder.with_memory_config(
            driver_memory=f"{int(driver_memory)}g",
            driver_max_result_size=f"{int(driver_maxresultsize)}g"
        )

    # Add any additional configs
    if additional_configs:
        for key, value in additional_configs.items():
            builder.with_config(key, value)

    return builder.build()


if __name__ == "__main__":

    # Example 1: Using presets
    spark = create_spark_session(preset="local")

    # Example 2: Using builder pattern
    config = (
        SparkConfigBuilder()
        .with_app_name("My Custom App")
        .add_iceberg_catalog("prod", "s3://my-bucket/warehouse", CatalogType.REST, "http://catalog:8080")
        .enable_s3_optimization()
        .with_memory_config(driver_memory="16g")
        .with_config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
    )

    spark = create_spark_session(config_builder=config)

    # Example 3: Backward compatibility
    spark = create_spark_session(
        local_warehouse_dir="/tmp/warehouse",
        driver_memory=8,
        additional_configs={"spark.sql.adaptive.enabled": "false"}
    )

    # Example 4: Custom configuration
    config = (SparkPresets.s3_production()
            .add_catalog(CatalogConfig(
                name="custom_catalog",
                catalog_type=CatalogType.JDBC,
                properties={
                    "uri": "jdbc:postgresql://localhost:5432/catalog",
                    "jdbc-user": "user",
                    "jdbc-password": "pass"
                }
            ))
            .with_config("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", "0"))

    spark = create_spark_session(config_builder=config)