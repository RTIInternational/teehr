"""Module to create and configure a Spark session."""
# flake8: noqa
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


def create_spark_session(
    # App name and catalog settings
    app_name: str = "TEEHR Evaluation",
    dir_path: Union[str, Path] = None,
    local_catalog_name: str = const.LOCAL_CATALOG_NAME,
    local_catalog_type: str = const.LOCAL_CATALOG_TYPE,
    local_namespace_name: str = const.LOCAL_NAMESPACE_NAME,
    remote_warehouse_dir: str = const.WAREHOUSE_S3_PATH,
    remote_catalog_name: str = const.REMOTE_CATALOG_NAME,
    remote_catalog_type: str = const.REMOTE_CATALOG_TYPE,
    remote_catalog_uri: str = const.CATALOG_REST_URI,
    remote_namespace_name: str = const.REMOTE_NAMESPACE_NAME,
    # Spark K8'specific parameters
    start_spark_cluster: bool = False,
    executor_instances: int = 2,
    executor_memory: str = "1g",
    executor_cores: int = 1,
    executor_image: str = None,
    executor_namespace: str = None,
    driver_memory: str = None,
    driver_max_result_size: str = None,
    pod_template_path: Union[str, Path] = const.POD_TEMPLATE_PATH,
    # AWS credential parameters
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_session_token: str = None,
    aws_region: str = const.AWS_REGION,
    allow_anonymous: bool = False,
    # Simple extensibility parameters
    extra_packages: List[str] = None,
    extra_configs: Dict[str, str] = None,
    debug_config: bool = False
) -> SparkSession:
    """Create and return a Spark session for evaluation.

    Parameters
    ----------
    app_name : str
        Name of the Spark application. Default is "TEEHR Evaluation".
    dir_path : Union[str, Path]
        The path to the evaluation directory.
    local_catalog_name : str
        Name of the local Iceberg catalog. Default is "local".
    local_catalog_type : str
        Type of the local Iceberg catalog. Default is "hadoop".
    local_namespace_name : str
        Namespace for the local Iceberg catalog. Default is "teehr".
    remote_warehouse_dir : str
        Remote warehouse directory for Iceberg catalog. Default is TEEHR
        warehouse S3 path.
    remote_catalog_name : str
        Name of the remote Iceberg catalog. Default is "iceberg".
    remote_catalog_type : str
        Type of the remote Iceberg catalog. Default is "rest".
    remote_catalog_uri : str
        URI for the remote Iceberg catalog. Default is TEEHR catalog REST URI.
    remote_namespace_name : str
        Namespace for the remote Iceberg catalog. Default is "teehr".
    start_spark_cluster : bool
        Whether to start a Spark cluster (Kubernetes mode).
        Default is False (local mode).
    executor_instances : int
        Number of executor instances for the Spark cluster. Default is 2.
    executor_memory : str
        Memory allocation for each executor. Default is "1g".
    executor_cores : int
        Number of CPU cores for each executor. Default is 1.
    executor_image : str
        Container image for Spark executors. Default is None.
    executor_namespace : str
        Kubernetes namespace for Spark executors. Default is None.
    driver_memory : str
        Memory allocation for the Spark driver. Default is None.
    driver_max_result_size : str
        Maximum result size for the Spark driver. Default is None.
    pod_template_path : Union[str, Path]
        Path to the pod template file for Spark executors.
        Default is "/opt/teehr/executor-pod-template.yaml".
    aws_access_key_id : str
        AWS access key ID for S3 access. Default is None.
    aws_secret_access_key : str
        AWS secret access key for S3 access. Default is None.
    aws_session_token : str
        AWS session token for temporary credentials. Default is None.
    aws_region : str
        AWS region name. Default is "us-east-2".
    allow_anonymous : bool
        Whether to allow anonymous access to S3. Default is False.
    extra_packages : List[str]
        Additional Spark packages to include. Default is None.
    extra_configs : Dict[str, str]
        Additional Spark configurations to set. Default is None.
    debug_config : bool
        Whether to log the final Spark configuration for debugging.
        Default is False.

    Returns
    -------
    SparkSession
        Configured Spark session.
    """
    # Get the base builder with common settings
    builder = _create_base_spark_builder(
        app_name=app_name,
        extra_packages=extra_packages,
        extra_configs=extra_configs,
        driver_memory=driver_memory,
        driver_maxresultsize=driver_max_result_size,
        aws_region=aws_region,
    )

    if start_spark_cluster is False:
        spark = builder.getOrCreate()
        logger.info("✅ Spark local session created successfully!")
    else:
        logger.info(f"🚀 Creating Spark session: {app_name}")
        logger.info(f"📦 Using container image: {executor_image}")
        spark = _create_spark_cluster_base_session(
            builder=builder,
            executor_instances=executor_instances,
            executor_memory=executor_memory,
            executor_cores=executor_cores,
            container_image=executor_image,
            spark_namespace=executor_namespace,
            pod_template_path=pod_template_path
        )
        logger.info("✅ Spark cluster created successfully!")
        logger.info(f"   - Application ID: {spark.sparkContext.applicationId}")
        logger.info(f"   - Executor instances: {executor_instances}")
        logger.info(f"   - Executor memory: {executor_memory}")
        logger.info(f"   - Executor cores: {executor_cores}")

    # Set AWS credentials if available
    _set_aws_credentials_in_spark(
        spark=spark,
        remote_catalog_name=remote_catalog_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        aws_region=aws_region,
        allow_anonymous=allow_anonymous
    )

    # Set catalog metadata in Spark configuration
    _set_catalog_metadata(
        spark=spark,
        dir_path=dir_path,
        local_catalog_name=local_catalog_name,
        local_catalog_type=local_catalog_type,
        remote_catalog_name=remote_catalog_name,
        remote_catalog_type=remote_catalog_type,
        remote_catalog_uri=remote_catalog_uri,
        remote_warehouse_dir=remote_warehouse_dir,
        local_namespace_name=local_namespace_name,
        remote_namespace_name=remote_namespace_name
    )

    # Apply catalog configurations
    _configure_iceberg_catalogs(
        spark=spark,
        local_warehouse_dir=dir_path,
        local_catalog_name=local_catalog_name,
        local_catalog_type=local_catalog_type,
        remote_warehouse_dir=remote_warehouse_dir,
        remote_catalog_name=remote_catalog_name,
        remote_catalog_type=remote_catalog_type,
        remote_catalog_uri=remote_catalog_uri
    )

    if dir_path is not None:
        spark.catalog.setCurrentCatalog(catalogName=local_catalog_name)
    spark.catalog.setCurrentCatalog(catalogName=remote_catalog_name)

    sedona_spark = SedonaContext.create(spark)
    logger.info("Spark session created for TEEHR Evaluation.")

    if debug_config:
        log_session_config(spark)

    logger.info("🎉 Spark session created successfully!")

    return sedona_spark


def _set_catalog_metadata(
    spark: SparkSession,
    dir_path: Union[str, Path],
    local_namespace_name: str,
    local_catalog_name: str,
    local_catalog_type: str,
    remote_catalog_name: str,
    remote_catalog_type: str,
    remote_catalog_uri: str,
    remote_warehouse_dir: str,
    remote_namespace_name: str
):
    """Set catalog metadata in Spark configuration."""
    if dir_path is None:
        local_warehouse_path = None
    else:
        local_warehouse_path = (
            Path(dir_path) / local_catalog_name
        ).as_posix()
    metadata_configs = {
        "local_warehouse_dir": local_warehouse_path,
        "local_catalog_name": local_catalog_name,
        "local_namespace_name": local_namespace_name,
        "local_catalog_type": local_catalog_type,
        "remote_warehouse_dir": remote_warehouse_dir,
        "remote_catalog_name": remote_catalog_name,
        "remote_namespace_name": remote_namespace_name,
        "remote_catalog_type": remote_catalog_type,
        "remote_catalog_uri": remote_catalog_uri
    }
    for key, value in metadata_configs.items():
        spark.conf.set(key, value)
        logger.debug(f"Metadata config: {key}: {value}")


def _set_aws_credentials_in_spark(
    spark: SparkSession,
    remote_catalog_name: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_session_token: str,
    aws_region: str,
    allow_anonymous: bool
):
    """Set AWS credentials in Spark configuration with multiple options."""
    # Priority 1: Explicit credentials provided by user
    if aws_access_key_id and aws_secret_access_key:
        logger.info("🔑 Using user-provided AWS credentials")
        spark.conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.access-key-id", aws_access_key_id)
        spark.conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.secret-access-key", aws_secret_access_key)
        spark.conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)

        if aws_session_token:
            spark.conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.session-token", aws_session_token)
            spark.conf.set("spark.hadoop.fs.s3a.session.token", aws_session_token)
            logger.info("   - Using temporary credentials with session token")
        else:
            logger.info("   - Using long-term credentials")

        if aws_region:
            spark.conf.set("spark.hadoop.fs.s3a.endpoint.region", aws_region)

        return
    # Priority 2: Environment variables
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_session_token = os.environ.get("AWS_SESSION_TOKEN")
    aws_region = os.environ.get("AWS_REGION")
    if aws_access_key_id and aws_secret_access_key:
        logger.info("🔑 Using user-provided AWS credentials")
        spark.conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.access-key-id", aws_access_key_id)
        spark.conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.secret-access-key", aws_secret_access_key)
        spark.conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)

        if aws_session_token:
            spark.conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.session-token", aws_session_token)
            spark.conf.set("spark.hadoop.fs.s3a.session.token", aws_session_token)
            logger.info("   - Using temporary credentials with session token")
        else:
            logger.info("   - Using long-term credentials")

        if aws_region:
            spark.conf.set("spark.hadoop.fs.s3a.endpoint.region", aws_region)

        return
    # Priority 3: Use anonymous access if allowed otherwise try default provider
    # Note. Could use boto3 here to first check if default credentials are available
    # and fallback to anonymous if not?
    if allow_anonymous:
        logger.info("🔑 Using anonymous AWS credentials for S3 access")
        spark.conf.set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
        )
        # Also might need to set empty remote catalog credentials to avoid errors?
        # spark.conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.access-key-id", "")
        # spark.conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.secret-access-key", "")
        # spark.conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.session-token", "")
        return
    # Fallback: Use Hadoop's default provider chain (system)
    logger.info("🔑 Falling back to Hadoop's default AWS credentials provider")
    spark.conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )


def _create_base_spark_builder(
    app_name: str,
    aws_region: str,
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

    builder = builder.config("spark.driver.extraJavaOptions", f"-Daws.region={aws_region}")
    builder = builder.config("spark.executor.extraJavaOptions", f"-Daws.region={aws_region}")

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

    # First try getting it from environment variable
    if spark_namespace is None:
        spark_namespace = os.environ["TEEHR_NAMESPACE"]
    logger.info(f"🔍 Initial spark namespace from ENV: {spark_namespace}")

    if spark_namespace is None:
        # Then get it from here
        namespace_file = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
        if os.path.exists(namespace_file):
            with open(namespace_file, 'r') as f:
                spark_namespace = f.read().strip()

    # Finally get it here if still None
    if spark_namespace is None:
        spark_namespace = "default"  # last resort, will probably fail

    logger.info(f"🔍 Connecting to Kubernetes API: {k8s_api_server}")
    logger.info(f"🎯 Executor namespace: {spark_namespace}")
    logger.info(f"🔐 Executor service account: spark (in {spark_namespace})")

    # Create Spark configuration
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
        logger.info(f"⚠️  Executor pod template not found: {pod_template_path}")
        logger.info("    You must provide a valid pod template for executors to launch correctly.")
        raise FileNotFoundError(f"Executor pod template not found: {pod_template_path}")

    builder = builder.config("spark.kubernetes.executor.deleteOnTermination", "true")

    # Authentication - use service account token if available
    token_file = const.SERVICE_ACCOUNT_TOKEN_PATH
    ca_file = const.CA_CERTIFICATE_PATH
    if os.path.exists(token_file) and os.path.exists(ca_file):
        logger.info("🔐 Using in-cluster authentication")
        builder = builder.config("spark.kubernetes.authenticate.submission.oauthTokenFile", token_file)
        builder = builder.config("spark.kubernetes.authenticate.submission.caCertFile", ca_file)
        builder = builder.config("spark.kubernetes.authenticate.driver.oauthTokenFile", token_file)
        builder = builder.config("spark.kubernetes.authenticate.executor.oauthTokenFile", token_file)

        # Critical: Set the CA cert file for SSL validation
        builder = builder.config("spark.kubernetes.authenticate.caCertFile", ca_file)
    else:
        logger.info("⚠️  No service account tokens found - may have authentication issues")
        logger.info(f"   Checked: {token_file}")
        logger.info(f"   Checked: {ca_file}")

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
        logger.info(f"🔗 Setting driver host to pod IP: {pod_ip}")
        builder = builder.config("spark.driver.host", pod_ip)
    else:
        logger.info("⚠️  Could not determine pod IP - using default driver host")

    # Return a spark session
    return builder.getOrCreate()


def log_session_config(spark: SparkSession):
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
        # "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    return {"packages": base_packages, "spark_configs": base_configs}


def _configure_iceberg_catalogs(
    spark: SparkSession,
    local_warehouse_dir: Union[str, Path],
    local_catalog_name: str,
    local_catalog_type: str,
    remote_warehouse_dir: str,
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
        f"spark.sql.catalog.{remote_catalog_name}.warehouse": remote_warehouse_dir,
        f"spark.sql.catalog.{remote_catalog_name}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
    }

    # These are needed only if running local kind cluster against MinIO for testing
    # or something similar
    if os.environ.get("IN_CLUSTER", "false").lower() == "true":
        logger.info("⚠️  Configuring remote catalog for MinIO access")
        remote_configs[f"spark.sql.catalog.{remote_catalog_name}.s3.endpoint"] = "http://minio:9000"
        remote_configs[f"spark.sql.catalog.{remote_catalog_name}.s3.path-style-access"] = "true"

    for key, value in remote_configs.items():
        spark.conf.set(key, value)
        logger.debug(f"Remote catalog: {key}: {value}")

    # # Metadata configuration for tracking catalog properties
    # metadata_configs = {
    #     "local_catalog_name": local_catalog_name,

    #     "remote_catalog_name": remote_catalog_name
    # }
    # for key, value in metadata_configs.items():
    #     spark.conf.set(key, value)
    #     logger.debug(f"Metadata config: {key}: {value}")


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
