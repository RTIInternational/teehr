"""Module to create and configure a Spark session - minimal."""
from typing import Dict, List, Union, Any
from pathlib import Path
import psutil
import logging
import os
import socket

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext
import pandas as pd

import teehr.const as const

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Note: Scala version: 2.13 in pyspark 4.0
SCALA_VERSION = "2.13"
PYSPARK_VERSION = "4.0"
ICEBERG_VERSION = "1.10.0"
SEDONA_VERSION = "1.8.0"

# EXECUTOR_CONTAINER_IMAGE = "935462133478.dkr.ecr.us-east-2.amazonaws.com/teehr-spark/teehr-spark-executor:v0.6-dev"
POD_TEMPLATE_PATH = "/opt/teehr/executor-pod-template.yaml"


def create_spark_session(
    # App name and catalog settings
    app_name: str = "TEEHR Evaluation",
    local_warehouse_dir: Union[str, Path] = None,
    local_catalog_name: str = "local",
    local_catalog_type: str = "hadoop",
    remote_warehouse_dir: str = const.WAREHOUSE_S3_PATH,
    remote_catalog_name: str = "iceberg",
    remote_catalog_type: str = "rest",
    remote_catalog_uri: str = const.CATALOG_REST_URI,
    # Spark K8'specific parameters
    start_spark_cluster: bool = False,
    executor_instances: int = 200,
    executor_memory: str = "1g",
    executor_cores: int = 1,
    driver_memory: str = None,
    driver_max_result_size: str = None,
    container_image: str = None,
    spark_namespace: str = "spark",
    pod_template_path: Union[str, Path] = POD_TEMPLATE_PATH,
    # Simple extensibility parameters
    extra_packages: List[str] = None,
    extra_configs: Dict[str, str] = None,
    debug_config: bool = False  # Log final config for debugging
) -> SparkSession:
    """Create and return a Spark session for evaluation."""
    # Get the base builder with common settings
    builder = _create_base_spark_builder(
        app_name=app_name,
        extra_packages=extra_packages,
        extra_configs=extra_configs,
        driver_memory=driver_memory,
        driver_maxresultsize=driver_max_result_size
    )

    if start_spark_cluster is False:
        spark = builder.getOrCreate()
        print("✅ Spark local session created successfully!")
    else:
        logger.info(f"🚀 Creating Spark session: {app_name}")
        logger.info(f"📦 Using container image: {container_image}")
        spark = _create_spark_cluster_base_session(
            builder=builder,
            executor_instances=executor_instances,
            executor_memory=executor_memory,
            executor_cores=executor_cores,
            container_image=container_image,
            spark_namespace=spark_namespace,
            pod_template_path=pod_template_path
        )

        print("✅ Spark cluster created successfully!")
        print(f"   - Application ID: {spark.sparkContext.applicationId}")
        print(f"   - Executor instances: {executor_instances}")
        print(f"   - Executor memory: {executor_memory}")
        print(f"   - Executor cores: {executor_cores}")

    # AWS credentials configuration -- Is this related to the cluster at all?
    # Try to get AWS credentials from default profile
    try:
        import boto3
        session = boto3.Session()
        credentials = session.get_credentials()

        if credentials:
            print("🔑 Found AWS credentials, setting for Spark")
            # Set explicit credentials for Spark/Hadoop
            spark.conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.access-key-id", credentials.access_key)
            spark.conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.secret-access-key", credentials.secret_key)

            # Handle session token if present (for temporary credentials)
            if credentials.token:
                spark.conf.set("spark.hadoop.fs.s3a.session.token", credentials.token)
                print("   - Using temporary credentials with session token")
            else:
                print("   - Using long-term credentials")
        else:
            print("⚠️  No AWS credentials found, falling back to default provider chain")
            spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    except ImportError:
        print("⚠️  boto3 not available, using default AWS credentials provider")
        spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    except Exception as e:
        print(f"⚠️  Error getting AWS credentials: {e}")
        print("   Falling back to default provider chain")
        spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

    # # Apply catalog configurations
    # _configure_catalogs_via_conf(
    #     spark,
    #     local_warehouse_dir,
    #     local_catalog_name,
    #     local_catalog_type,
    #     remote_warehouse_dir,
    #     remote_catalog_name,
    #     remote_catalog_type,
    #     remote_catalog_uri
    # )

    # if local_warehouse_dir is not None:
    #     spark.catalog.setCurrentCatalog(catalogName=local_catalog_name)
    # spark.catalog.setCurrentCatalog(catalogName=remote_catalog_name)

    sedona_spark = SedonaContext.create(spark)
    logger.info("Spark session created for TEEHR Evaluation.")

    if debug_config:
        _log_final_config(spark)

    print("🎉 Good to go!")

    return sedona_spark


def _create_base_spark_builder(
    app_name: str,
    extra_packages: List[str] = None,
    extra_configs: Dict[str, str] = None,
    driver_memory: float = None,
    driver_maxresultsize: float = None
):
    """Create a base Spark builder.

    These settings are considered immutable after session creation
    and are common to both local and cluster modes.
    """
    # Handle default local memory settings
    memory_info = psutil.virtual_memory()
    driver_memory_int = int(0.75 * memory_info.available / (1024**3))
    if driver_memory is None:
        driver_memory = f"{driver_memory_int}g"
    if driver_maxresultsize is None:
        driver_maxresultsize = f"{int(0.5 * driver_memory_int)}g"

    builder = SparkSession.builder.appName(app_name)

    # Set packages at builder level
    packages = _get_spark_defaults()["packages"].copy()
    if extra_packages:
        packages.extend(extra_packages)

    # Set base configs at builder level
    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    builder = builder.config("spark.jars.packages", ",".join(packages))
    builder = builder.config("spark.driver.memory", f"{driver_memory}")
    builder = builder.config("spark.driver.maxResultSize", f"{driver_maxresultsize}")

    return builder


def _create_spark_cluster_base_session(
    builder: SparkSession.Builder,
    executor_instances: int,
    executor_memory: str,
    executor_cores: int,
    container_image: str,
    spark_namespace: str,
    pod_template_path: Union[str, Path]
):
    """Configure Spark for Kubernetes cluster mode.

    These settings are considered immutable after session creation.
    """
    # Default container image - use the same image as the current pod
    if container_image is None:
        container_image = os.environ["TEEHR_SPARK_IMAGE"]

    # Get Kubernetes API server - use HTTPS port specifically
    k8s_host = os.environ.get('KUBERNETES_SERVICE_HOST', 'kubernetes.default.svc.cluster.local')
    k8s_port_https = os.environ.get('KUBERNETES_SERVICE_PORT_HTTPS', '443')
    k8s_api_server = f"https://{k8s_host}:{k8s_port_https}"

    # Detect current namespace if running in a pod
    current_namespace = "default"
    namespace_file = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    if os.path.exists(namespace_file):
        with open(namespace_file, 'r') as f:
            current_namespace = f.read().strip()

    logger.info(f"🔍 Connecting to Kubernetes API: {k8s_api_server}")
    logger.info(f"🏠 Current namespace: {current_namespace}")
    logger.info(f"🎯 Executor namespace: {spark_namespace}")
    logger.info(f"🔐 Driver service account: default (in {current_namespace})")
    logger.info(f"🔐 Executor service account: spark (in {spark_namespace})")

    # Create Spark configuration
    # builder = builder.config("spark.master", f"k8s://{k8s_api_server}")
    builder = builder.master(f"k8s://{k8s_api_server}")

    # Basic Kubernetes settings
    builder = builder.config("spark.executor.instances", str(executor_instances))
    builder = builder.config("spark.executor.memory", executor_memory)
    builder = builder.config("spark.executor.cores", str(executor_cores))

    builder = builder.config("spark.kubernetes.container.image", container_image)
    builder = builder.config("spark.kubernetes.namespace", spark_namespace)
    builder = builder.config("spark.kubernetes.authenticate.executor.serviceAccountName", "spark")
    builder = builder.config("spark.kubernetes.container.image.pullPolicy", "Always")

    if os.path.exists(pod_template_path):
        builder = builder.config("spark.kubernetes.executor.podTemplateFile", pod_template_path)
    else:
        print(f"⚠️  Executor pod template not found: {pod_template_path}")
        print("    You must provide a valid pod template for executors to launch correctly.")
        raise FileNotFoundError(f"Executor pod template not found: {pod_template_path}")

    builder = builder.config("spark.kubernetes.executor.deleteOnTermination", "true")

    # Authentication - use service account token if available
    token_file = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    ca_file = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    if os.path.exists(token_file) and os.path.exists(ca_file):
        print("🔐 Using in-cluster authentication")
        builder = builder.config("spark.kubernetes.authenticate.submission.oauthTokenFile", token_file)
        builder = builder.config("spark.kubernetes.authenticate.submission.caCertFile", ca_file)
        builder = builder.config("spark.kubernetes.authenticate.driver.oauthTokenFile", token_file)
        builder = builder.config("spark.kubernetes.authenticate.executor.oauthTokenFile", token_file)

        # Critical: Set the CA cert file for SSL validation
        builder = builder.config("spark.kubernetes.authenticate.caCertFile", ca_file)
    else:
        print("⚠️  No service account tokens found - may have authentication issues")
        print(f"   Checked: {token_file}")
        print(f"   Checked: {ca_file}")

    # Driver binding configuration - use pod IP for Kubernetes
    builder = builder.config("spark.driver.bindAddress", "0.0.0.0")
    builder = builder.config("spark.driver.port", "0")  # Let Spark choose an available port

    # Get pod IP and set as driver host so executors can connect back
    pod_ip = os.environ.get('POD_IP')
    if not pod_ip:
        try:
            hostname = socket.gethostname()
            pod_ip = socket.gethostbyname(hostname)
        except:
            pod_ip = None

    if pod_ip:
        print(f"🔗 Setting driver host to pod IP: {pod_ip}")
        builder = builder.config("spark.driver.host", pod_ip)
    else:
        print("⚠️  Could not determine pod IP - using default driver host")

    # Return a spark session
    return builder.getOrCreate()


def _log_final_config(spark: SparkSession):
    """Log the final Spark configuration for debugging."""
    logger.info("Final Spark configuration:")
    df = pd.DataFrame(list(spark.conf.getAll.items()), columns=["Key", "Value"])
    gps = df.groupby(by="Key")
    for key, group in gps:
        value = ",".join(group["Value"].tolist())
        values = value.split(",")
        if key.startswith("spark."):
            if len(values) > 1:
                logger.info(f" {key}: ")
                for val in values:
                    logger.info(f"    {val}")
            else:
                logger.info(f" {key}: {value}")


def _get_spark_defaults() -> Dict[str, Any]:
    """Get default Spark configurations based on preset.

    These are common to local and cluster sessions.
    """
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
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    return {"packages": base_packages, "spark_configs": base_configs}


def _configure_catalogs_via_conf(
    spark: SparkSession,
    local_warehouse_dir: Union[str, Path],
    local_catalog_name: str,
    local_catalog_type: str,
    remote_warehouse_dir: Union[str, Path],
    remote_catalog_name: str,
    remote_catalog_type: str,
    remote_catalog_uri: str
):
    """Configure Iceberg catalogs through spark.conf.set()."""
    logger.info("Configuring Iceberg catalogs...")

    # Local catalog configuration
    if local_warehouse_dir is not None:
        local_warehouse_path = Path(local_warehouse_dir) / local_catalog_name
        catalog_configs = {
            f"spark.sql.catalog.{local_catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{local_catalog_name}.type": local_catalog_type,
            f"spark.sql.catalog.{local_catalog_name}.warehouse": local_warehouse_path.as_posix()
        }

        for key, value in catalog_configs.items():
            spark.conf.set(key, value)
            logger.debug(f"Local catalog: {key}: {value}")

    # Remote catalog configuration
    remote_configs = {
        f"spark.sql.catalog.{remote_catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{remote_catalog_name}.type": remote_catalog_type,
        f"spark.sql.catalog.{remote_catalog_name}.uri": remote_catalog_uri,
        f"spark.sql.catalog.{remote_catalog_name}.warehouse": str(remote_warehouse_dir),
        f"spark.sql.catalog.{remote_catalog_name}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
    }

    for key, value in remote_configs.items():
        spark.conf.set(key, value)
        logger.debug(f"Remote catalog: {key}: {value}")


def remove_or_update_configs(
    spark: SparkSession,
    remove_configs: List[str] = None,
    update_configs: Dict[str, str] = None
) -> Dict[str, str]:
    """Add, remove, or update Spark configurations."""
    # Remove specified configs
    if remove_configs is not None:
        for key in remove_configs:
            try:
                current_value = spark.conf.get(key)
                logger.info(f"Removing config: {key} (was: {current_value})")
                spark.conf.unset(key)
            except Exception as e:
                logger.warning(f"Could not remove config {key}: {e}")

    # Update or add specified configs
    if update_configs is not None:
        for key, value in update_configs.items():
            spark.conf.set(key, value)

    return


if __name__ == "__main__":
    # Example 1: Default usage (unchanged)
    spark = create_spark_session(local_warehouse_dir="/tmp/warehouse")

    # Example 2: Add custom packages
    spark = create_spark_session(
        local_warehouse_dir="/tmp/warehouseEXAMPLE2",
        extra_packages=["com.example:my-package:1.0.0"],
    )

    # Example 3: Override specific settings
    spark = create_spark_session(
        local_warehouse_dir="/tmp/warehouse",
        extra_configs={
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "512MB"
        }
    )

    # # Example 4: Use different preset
    # spark = create_spark_session(
    #     local_warehouse_dir="/tmp/warehouse",
    #     config_preset="s3_optimized"
    # )

    # Example 5: Combine everything
    spark = create_spark_session(
        local_warehouse_dir="/tmp/warehouse",
        # config_preset="minimal",
        extra_packages=["org.postgresql:postgresql:42.7.8"],
        extra_configs={
            "spark.sql.adaptive.enabled": "true",
            "spark.eventLog.enabled": "true"   # Requires a /tmp/spark-events directory?
        }
    )

    # Try updating the config after session creation
    remove_or_update_configs(
        spark,
        remove_configs=["spark.sql.adaptive.enabled"],
        update_configs={
            # "spark.driver.maxResultSize": "16g",  # Can't change this after session creation
            "spark.sql.adaptive.enabled": "false"
        }
    )

    logger.info("=================================================")
    _log_final_config(spark)

    pass