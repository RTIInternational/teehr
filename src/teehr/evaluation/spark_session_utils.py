"""Module to create and configure Spark sessions and Polaris auth helpers."""
# flake8: noqa
import base64
import json
import logging
import os
import socket
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urlsplit, urlunsplit

import psutil
import requests

from pyspark import SparkConf
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext
import pandas as pd
import botocore.session

import teehr.const as const

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Note: Scala version: 2.13 in pyspark 4.0
SCALA_VERSION = "2.13"
PYSPARK_VERSION = "4.0"
ICEBERG_VERSION = "1.10.1"
SEDONA_VERSION = "1.8.0"



def create_spark_session(
    # App name and catalog settings
    app_name: str = "TEEHR Evaluation",
    local_catalog_name: str = const.LOCAL_CATALOG_NAME,
    local_catalog_type: str = const.LOCAL_CATALOG_TYPE,
    remote_warehouse_dir: str = const.REMOTE_WAREHOUSE_S3_PATH,
    remote_catalog_name: str = const.REMOTE_CATALOG_NAME,
    remote_catalog_type: str = const.REMOTE_CATALOG_TYPE,
    remote_catalog_uri: str = const.REMOTE_CATALOG_REST_URI,
    # Spark K8'specific parameters
    start_spark_cluster: bool = False,
    force_recreate_session: bool = False,
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
    aws_profile: str = None,
    # GCS credential parameters
    enable_gcs: bool = False,
    gcs_project_id: str = None,
    gcs_service_account_key_file: str = None,
    # Simple extensibility parameters
    add_jars: List[str] = None,
    add_packages: List[str] = None,
    update_configs: Dict[str, str] = None,
    debug_config: bool = False,
    # Polaris authentication
    polaris_token: Optional[str] = None,
    use_authmanager: Optional[bool] = None,
) -> SparkSession:
    """Create and return a Spark session for evaluation.

    Parameters
    ----------
    app_name : str
        Name of the Spark application. Default is "TEEHR Evaluation".
    local_catalog_name : str
        Name of the local Iceberg catalog. Default is "local".
    local_catalog_type : str
        Type of the local Iceberg catalog. Default is "jdbc".
    remote_warehouse_dir : str
        Remote warehouse directory for Iceberg catalog. Default is TEEHR
        warehouse S3 path.
    remote_catalog_name : str
        Name of the remote Iceberg catalog. Default is "iceberg".
    remote_catalog_type : str
        Type of the remote Iceberg catalog. Default is "rest".
    remote_catalog_uri : str
        URI for the remote Iceberg catalog. Default is TEEHR catalog REST URI.
    start_spark_cluster : bool
        Whether to start a Spark cluster (Kubernetes mode).
        Default is False (local mode).
    force_recreate_session : bool
        Whether to stop an existing Spark session before creating a new one.
        Default is False.
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
    aws_profile : str
        AWS profile name to use from ~/.aws/credentials. Only reads credentials
        file if this parameter is explicitly provided. Default is None.
    enable_gcs : bool
        Whether to add GCS (Google Cloud Storage) connector support.
        Default is False.
    gcs_project_id : str
        GCS project ID. Used for billing and quota tracking. When accessing
        public buckets without credentials, set to "anonymous" or any
        non-empty string. Default is None (will be set to "anonymous" when
        enable_gcs is True and no service account key is provided).
    gcs_service_account_key_file : str
        Path to a GCS service account JSON key file. When provided,
        authenticated access is used. When None, unauthenticated
        (public-bucket) access is used. Default is None.
    add_packages : List[str]
        Provided Spark packages will be added if they do not already exist.
        Default is None.
        >>> add_packages=["com.example:my-package:1.0.0"]
    add_jars : List[str]
        Provided local jar paths will be added if they do not already exist.
        Default is None.
        >>> add_jars=["/opt/spark/jars/teehr-authmanager.jar"]
    update_configs : Dict[str, str]
        Provided Spark configurations will be added if they do not already
        exist, or overwritten if they do exist. Default is None.
        >>> update_configs={"spark.sql.shuffle.partitions": "100"}
    debug_config : bool
        Whether to log the final Spark configuration for debugging.
        Default is False.

    Returns
    -------
    SparkSession
        Configured Spark session.
    """
    logger.info(f"🚀 Creating Spark session: {app_name}")

    # AuthManager requires a fresh JVM per user session to avoid static state leakage
    resolved_use_authmanager = (
        use_authmanager if use_authmanager is not None
        else _as_bool_str(os.getenv("POLARIS_USE_AUTHMANAGER", "false")) == "true"
    )
    if force_recreate_session or resolved_use_authmanager:
        existing_session = SparkSession.getActiveSession()
        if existing_session is not None:
            logger.info("♻️ Stopping the active Spark session before recreation")
            existing_session.stop()

    # Get the base configuration with common settings
    conf = _create_spark_base_session(
        conf=SparkConf(),
        aws_region=aws_region,
        driver_memory=driver_memory,
        driver_maxresultsize=driver_max_result_size
    )

    if start_spark_cluster is False:
        logger.info("✅ Spark local configuration successful!")
    else:
        logger.info(f"📦 Configuring Spark cluster with container image: {executor_image}")
        _set_spark_cluster_configuration(
            conf=conf,
            executor_instances=executor_instances,
            executor_memory=executor_memory,
            executor_cores=executor_cores,
            container_image=executor_image,
            spark_namespace=executor_namespace,
            pod_template_path=pod_template_path
        )
        logger.info("✅ Spark cluster configuration successful!")
        logger.info(f"   - Executor instances: {executor_instances}")
        logger.info(f"   - Executor memory: {executor_memory}")
        logger.info(f"   - Executor cores: {executor_cores}")

    # Set AWS credentials if available
    _set_aws_credentials_in_spark(
        conf=conf,
        remote_catalog_name=remote_catalog_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        aws_region=aws_region,
        aws_profile=aws_profile,
    )

    # Set GCS configuration if available
    if enable_gcs:
        _set_gcs_configuration(
            conf=conf,
            gcs_project_id=gcs_project_id,
            gcs_service_account_key_file=gcs_service_account_key_file,
        )

    # Set catalog metadata in Spark configuration
    _set_catalog_metadata(
        conf=conf,
        local_catalog_name=local_catalog_name,
        local_catalog_type=local_catalog_type,
        remote_catalog_name=remote_catalog_name,
        remote_catalog_type=remote_catalog_type,
        remote_catalog_uri=remote_catalog_uri,
        remote_warehouse_dir=remote_warehouse_dir
    )

    # Apply catalog configurations
    _configure_iceberg_catalogs(
        conf=conf,
        local_catalog_name=local_catalog_name,
        local_catalog_type=local_catalog_type,
        remote_warehouse_dir=remote_warehouse_dir,
        remote_catalog_name=remote_catalog_name,
        remote_catalog_type=remote_catalog_type,
        remote_catalog_uri=remote_catalog_uri
    )

    # Build Polaris auth configs and merge with caller-provided update_configs.
    # Auth configs are the base; caller's configs take precedence.
    polaris_auth_configs = _build_polaris_auth_configs(polaris_token, use_authmanager)
    effective_configs: Dict[str, str] = {**polaris_auth_configs}
    if update_configs:
        effective_configs.update(update_configs)

    # Update configs and packages if provided
    _update_configs_and_packages(
        conf=conf,
        update_configs=effective_configs or None,
        add_jars=add_jars,
        add_packages=add_packages
    )

    logger.info("⚙️ All settings applied. Creating Spark session...")
    spark = SparkSession.builder.appName(app_name).config(conf=conf).getOrCreate()
    sedona_spark = SedonaContext.create(spark)

    # Apply runtime-settable configs to the live session (e.g., auth tokens)
    if effective_configs:
        _apply_runtime_spark_configs(sedona_spark, effective_configs)

    if debug_config:
        log_session_config(sedona_spark)

    logger.info("🎉 Spark session created successfully!")

    return sedona_spark


def _create_spark_base_session(
    conf: SparkConf,
    aws_region: str,
    driver_memory: float = None,
    driver_maxresultsize: float = None
):
    """Create a base Spark builder."""
    conf.setMaster("local[*]")

    # Set base packages
    base_packages = [
        f"org.apache.sedona:sedona-spark-shaded-{PYSPARK_VERSION}_{SCALA_VERSION}:{SEDONA_VERSION}",
        f"org.apache.iceberg:iceberg-spark-runtime-{PYSPARK_VERSION}_{SCALA_VERSION}:{ICEBERG_VERSION}",
        f"org.apache.iceberg:iceberg-core:{ICEBERG_VERSION}",
        "org.datasyslab:geotools-wrapper:1.8.0-33.1",
        f"org.apache.iceberg:iceberg-spark-extensions-{PYSPARK_VERSION}_{SCALA_VERSION}:{ICEBERG_VERSION}",
        "org.apache.hadoop:hadoop-aws:3.4.1",  # Note. Need 3.4.1 for compatibility
        "com.amazonaws:aws-java-sdk-bundle:1.12.791",
        "org.xerial:sqlite-jdbc:3.42.0.0"
    ]
    conf.set("spark.jars.packages", ",".join(base_packages))

    # Set configurations
    conf.set("spark.driver.extraJavaOptions", f"-Daws.region={aws_region}")
    conf.set("spark.executor.extraJavaOptions", f"-Daws.region={aws_region}")
    conf.set("spark.sql.session.timeZone", "UTC")
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Enable Spark decommissioning for graceful spot instance handling
    conf.set("spark.decommission.enabled", "true")
    conf.set("spark.storage.decommission.enabled", "true")
    conf.set("spark.storage.decommission.rddBlocks.enabled", "true")
    conf.set("spark.storage.decommission.shuffleBlocks.enabled", "true")
    # Grace period for executors to decommission before being terminated
    conf.set("spark.kubernetes.executor.decommission.gracePeriodSeconds", "30")

    # Memory settings
    memory_info = psutil.virtual_memory()
    driver_memory_int = int(0.75 * memory_info.available / (1024**3))
    if driver_memory is None:
        driver_memory = f"{driver_memory_int}g"
    if driver_maxresultsize is None:
        driver_maxresultsize = f"{int(0.5 * driver_memory_int)}g"
    conf.set("spark.driver.memory", f"{driver_memory}")
    conf.set("spark.driver.maxResultSize", f"{driver_maxresultsize}")

    # Driver binding configuration for local mode
    conf.set("spark.driver.bindAddress", "127.0.0.1")
    conf.set("spark.driver.host", "127.0.0.1")
    conf.set("spark.driver.port", "0")  # Let Spark choose an available port

    # Default shuffle partitions: 2x local CPU cores
    local_cores = os.cpu_count() or 1
    default_shuffle_partitions = 2 * local_cores
    conf.set("spark.sql.shuffle.partitions", str(default_shuffle_partitions))

    return conf


def _set_spark_cluster_configuration(
    conf: SparkConf,
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
        spark_namespace = os.environ.get("TEEHR_NAMESPACE", "")
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
    conf.setMaster(f"k8s://{k8s_api_server}")

    # Check project ID to specify appropriate node group name.
    teehr_project_id = os.environ.get("TEEHR_PROJECT_ID", "")
    if teehr_project_id != "TEEHR" and teehr_project_id != "":
        conf.set(
            "spark.kubernetes.executor.node.selector.teehr-hub/nodegroup-name",
            f"spark-r5-4xlarge-spot-{teehr_project_id.lower()}"
        )

    # Basic Kubernetes settings
    conf.set("spark.executor.instances", str(executor_instances))
    conf.set("spark.executor.memory", executor_memory)
    conf.set("spark.executor.cores", str(executor_cores))
    conf.set("spark.kubernetes.container.image", container_image)
    conf.set("spark.kubernetes.namespace", spark_namespace)
    conf.set("spark.kubernetes.authenticate.executor.serviceAccountName", "spark")
    conf.set("spark.kubernetes.container.image.pullPolicy", "Always")

    # Enable Spark's dynamic allocation and decommissioning features for better
    # handling of spot instances and resource management.
    conf.set("spark.decommission.enabled", "true")
    conf.set("spark.executor.decommission.signal", "SIGTERM")
    conf.set("spark.storage.decommission.enabled", "true")

    # Set pod name prefix for executors in TEEHR-HUB for easy identification in cluster.
    # Truncated to 46 chars since K8s pod names have a 63-char limit and Spark appends suffixes.
    jupyterhub_user = os.environ.get("JUPYTERHUB_USER", None)
    if jupyterhub_user is not None:
        conf.set("spark.kubernetes.executor.podNamePrefix", jupyterhub_user[:46])

    if os.path.exists(pod_template_path):
        conf.set("spark.kubernetes.executor.podTemplateFile", pod_template_path)
    else:
        logger.info(f"⚠️  Executor pod template not found: {pod_template_path}")
        logger.info("    You must provide a valid pod template for executors to launch correctly.")
        raise FileNotFoundError(f"Executor pod template not found: {pod_template_path}")

    conf.set("spark.kubernetes.executor.deleteOnTermination", "true")

    # Default shuffle partitions: 2x total executor cores
    total_executor_cores = executor_cores * executor_instances
    default_shuffle_partitions = 2 * total_executor_cores
    conf.set("spark.sql.shuffle.partitions", str(default_shuffle_partitions))

    # Authentication - use service account token if available
    token_file = const.SERVICE_ACCOUNT_TOKEN_PATH
    ca_file = const.CA_CERTIFICATE_PATH
    if os.path.exists(token_file) and os.path.exists(ca_file):
        logger.info("🔐 Using in-cluster authentication")
        conf.set("spark.kubernetes.authenticate.submission.oauthTokenFile", token_file)
        conf.set("spark.kubernetes.authenticate.submission.caCertFile", ca_file)
        conf.set("spark.kubernetes.authenticate.driver.oauthTokenFile", token_file)
        conf.set("spark.kubernetes.authenticate.executor.oauthTokenFile", token_file)

        # Critical: Set the CA cert file for SSL validation
        conf.set("spark.kubernetes.authenticate.caCertFile", ca_file)
    else:
        logger.info("⚠️  No service account tokens found - may have authentication issues")
        logger.info(f"   Checked: {token_file}")
        logger.info(f"   Checked: {ca_file}")

    # Driver binding configuration - use pod IP for Kubernetes
    conf.set("spark.driver.bindAddress", "0.0.0.0")
    conf.set("spark.driver.port", "0")  # Let Spark choose an available port

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
        conf.set("spark.driver.host", pod_ip)
    else:
        logger.info("⚠️  Could not determine pod IP - using default driver host")

    return


def _set_aws_credentials_in_spark(
    conf: SparkConf,
    remote_catalog_name: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_session_token: str,
    aws_region: str,
    aws_profile: str = None,
):
    """Set AWS credentials in Spark configuration with multiple options."""
    logger.info("Setting Hadoop's default AWS credentials provider and AWS region")
    conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    conf.set("spark.hadoop.fs.s3a.endpoint.region", aws_region)

    # Priority 1: Explicit credentials provided by user
    if aws_access_key_id and aws_secret_access_key:
        logger.info("🔑 Using user-provided AWS credentials")
        conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.access-key-id", aws_access_key_id)
        conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.secret-access-key", aws_secret_access_key)
        conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
        conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
        return

    # Priority 2: Explicit token
    if aws_session_token:
        logger.info("🔑 Using user-provided AWS session token")
        conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.session-token", aws_session_token)
        conf.set("spark.hadoop.fs.s3a.session.token", aws_session_token)
        return

    # Priority 3: Check ~/.aws/credentials file only if profile explicitly specified (full access)
    if aws_profile:
        aws_credentials_file = Path.home() / ".aws" / "credentials"
        if aws_credentials_file.exists():
            try:
                import configparser
                config = configparser.ConfigParser()
                config.read(aws_credentials_file)

                if config.has_section(aws_profile):
                    if config.has_option(aws_profile, "aws_access_key_id") and config.has_option(aws_profile, "aws_secret_access_key"):
                        creds_access_key = config.get(aws_profile, "aws_access_key_id")
                        creds_secret_key = config.get(aws_profile, "aws_secret_access_key")
                        creds_session_token = config.get(aws_profile, "aws_session_token", fallback=None)

                        logger.info(f"🔑 Using AWS credentials from ~/.aws/credentials profile '{aws_profile}")
                        conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.access-key-id", creds_access_key)
                        conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.secret-access-key", creds_secret_key)
                        conf.set("spark.hadoop.fs.s3a.access.key", creds_access_key)
                        conf.set("spark.hadoop.fs.s3a.secret.key", creds_secret_key)

                        if creds_session_token:
                            conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.session-token", creds_session_token)
                            conf.set("spark.hadoop.fs.s3a.session.token", creds_session_token)
                        return
            except Exception as e:
                logger.debug(f"Could not read ~/.aws/credentials: {e}")

    session = botocore.session.Session()
    credentials = session.get_credentials()

    # Priority 4: Check boto token
    if credentials and credentials.token:
        logger.info("🔑 Using AWS session token from boto3")
        conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.session-token", credentials.token)
        conf.set("spark.hadoop.fs.s3a.session.token", credentials.token)
        return

    # Priority 5: Check boto credentials
    if credentials and credentials.access_key and credentials.secret_key:
        logger.info("🔑 Using AWS credentials from boto3")
        conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.access-key-id", credentials.access_key)
        conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.secret-access-key", credentials.secret_key)
        conf.set("spark.hadoop.fs.s3a.access.key", credentials.access_key)
        conf.set("spark.hadoop.fs.s3a.secret.key", credentials.secret_key)
        return

    # Priority 6: Fall back to anonymous or default provider
    logger.info("🔑 Using anonymous AWS credentials for S3 access")
    conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
    )
    return

def _set_gcs_configuration(
    conf: SparkConf,
    gcs_project_id: str = None,
    gcs_service_account_key_file: str = None,
):
    """Configure Spark for Google Cloud Storage (GCS) access.

    Parameters
    ----------
    conf : SparkConf
        The Spark configuration object to update.
    gcs_project_id : str
        GCS project ID for billing/quota. Defaults to "anonymous" when
        no service account key is provided (public bucket access).
    gcs_service_account_key_file : str
        Path to a GCS service account JSON key file. When None,
        unauthenticated access is used (suitable for public buckets).
    """
    GCS_CONNECTOR_VERSION = "hadoop3-2.2.32"
    gcs_package = f"com.google.cloud.bigdataoss:gcs-connector:{GCS_CONNECTOR_VERSION}"

    # Add the GCS connector package
    current_packages = conf.get("spark.jars.packages").split(",")
    if gcs_package not in current_packages:
        current_packages.append(gcs_package)
    conf.set("spark.jars.packages", ",".join(current_packages))

    # Register GCS filesystem implementations
    conf.set(
        "spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )
    conf.set(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    )

    if gcs_service_account_key_file:
        key_path = Path(gcs_service_account_key_file)
        if not key_path.exists():
            raise FileNotFoundError(
                f"GCS service account key file not found: {gcs_service_account_key_file}"
            )
        logger.info(f"🔑 Using GCS service account key: {gcs_service_account_key_file}")
        conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        conf.set(
            "spark.hadoop.google.cloud.auth.service.account.keyfile",
            str(key_path)
        )
        if gcs_project_id:
            conf.set("spark.hadoop.fs.gs.project.id", gcs_project_id)
    else:
        # Unauthenticated access for public GCS buckets
        logger.info("🔑 Using unauthenticated GCS access (public buckets)")
        # Set both old-style (fs.gs.*) and new-style (google.cloud.*) auth
        # properties so the connector picks up UNAUTHENTICATED regardless of
        # which configuration namespace it checks first.
        conf.set("spark.hadoop.fs.gs.auth.type", "UNAUTHENTICATED")
        conf.set("spark.hadoop.google.cloud.auth.type", "UNAUTHENTICATED")
        # Explicitly disable service account auth to prevent the connector from
        # attempting to contact the GCE metadata server (http://metadata.google.internal/)
        # for credentials.  Without this, the connector may hang on machines
        # that are not running on GCP.
        conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "false")
        # Enable null (anonymous) credentials — required for the connector to
        # accept a configuration with no active credential source.
        conf.set("spark.hadoop.fs.gs.auth.null.enable", "true")
        conf.set(
            "spark.hadoop.fs.gs.project.id",
            gcs_project_id if gcs_project_id else "anonymous"
        )


def _set_catalog_metadata(
    conf: SparkConf,
    local_catalog_name: str,
    local_catalog_type: str,
    remote_catalog_name: str,
    remote_catalog_type: str,
    remote_catalog_uri: str,
    remote_warehouse_dir: str,
):
    """Set catalog metadata in Spark configuration."""
    metadata_configs = {
        "local_catalog_name": local_catalog_name,
        "local_catalog_type": local_catalog_type,
        "remote_warehouse_dir": remote_warehouse_dir,
        "remote_catalog_name": remote_catalog_name,
        "remote_catalog_type": remote_catalog_type,
        "remote_catalog_uri": remote_catalog_uri
    }
    for key, value in metadata_configs.items():
        conf.set(key, value)
        logger.debug(f"Metadata config: {key}: {value}")


def _configure_iceberg_catalogs(
    conf: SparkConf,
    local_catalog_name: str,
    local_catalog_type: str,
    remote_warehouse_dir: str,
    remote_catalog_name: str,
    remote_catalog_type: str,
    remote_catalog_uri: str
):
    """Configure Iceberg catalogs through conf.set()."""
    logger.info("Configuring Iceberg catalogs...")
    # Local catalog configuration
    conf.set(f"spark.sql.catalog.{local_catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{local_catalog_name}.type", local_catalog_type)
    conf.set(f"spark.sql.catalog.{local_catalog_name}.jdbc.driver", "org.sqlite.JDBC")
    conf.set(f"spark.sql.catalog.{local_catalog_name}.jdbc.initialize", "true")
    conf.set(f"spark.sql.catalog.{local_catalog_name}.jdbc.schema-version", "V1")
    # conf.set("spark.sql.catalog.local.jdbc.user", "user")
    # conf.set("spark.sql.catalog.local.jdbc.password", "password")

    # Remote catalog configuration
    conf.set(f"spark.sql.catalog.{remote_catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{remote_catalog_name}.type", remote_catalog_type)
    conf.set(f"spark.sql.catalog.{remote_catalog_name}.uri", remote_catalog_uri)
    conf.set(f"spark.sql.catalog.{remote_catalog_name}.warehouse", remote_warehouse_dir)
    conf.set(f"spark.sql.catalog.{remote_catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    # S3 end point and path style access
    if os.environ.get("REMOTE_CATALOG_S3_PATH_STYLE_ACCESS", "false").lower() == "true":
        conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.endpoint", os.environ.get("REMOTE_CATALOG_S3_ENDPOINT"))
        conf.set(f"spark.sql.catalog.{remote_catalog_name}.s3.path-style-access", os.environ.get("REMOTE_CATALOG_S3_PATH_STYLE_ACCESS").lower())


def _update_configs_and_packages(
    conf: SparkConf,
    update_configs: Dict[str, str],
    add_jars: List[str],
    add_packages: List[str]
) -> Dict[str, str]:
    """Update Spark configurations and packages."""
    # Add specified local jars
    if add_jars is not None:
        current_jars = conf.get("spark.jars").split(",") if conf.contains("spark.jars") else []
        for jar_path in add_jars:
            if jar_path not in current_jars:
                current_jars.append(jar_path)
        if current_jars:
            conf.set("spark.jars", ",".join(current_jars))

    # Add specified packages
    if add_packages is not None:
        current_packages = conf.get("spark.jars.packages").split(",")
        for package in add_packages:
            if package not in current_packages:
                current_packages.append(package)
        conf.set("spark.jars.packages", ",".join(current_packages))

    # Update or add specified configs
    if update_configs is not None:
        for key, value in update_configs.items():
            conf.set(key, value)
    return


def log_session_config(spark: SparkSession):
    """Log the current Spark session configuration for debugging.

    Parameters
    ----------
    spark : SparkSession
        The Spark session whose configuration should be logged.

    Notes
    -----
    This function logs all Spark configuration properties to the
    logger at INFO level for troubleshooting purposes.
    """
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


def remove_or_update_configs(
    spark: SparkSession,
    remove_configs: List[str] = None,
    update_configs: Dict[str, str] = None
) -> Dict[str, str]:
    """Add, remove, or update Spark configurations.

    Parameters
    ----------
    spark : SparkSession
        The Spark session whose configuration should be logged.
    remove_configs : List[str]
        List of configuration keys to remove from the Spark session.
        Default is None.
        >>> remove_configs=["spark.sql.shuffle.partitions"]
    update_configs : Dict[str, str]
        Provided Spark configurations will be added if they do not already
        exist, or overwritten if they do exist. Default is None.
        >>> update_configs={"spark.sql.shuffle.partitions": "100"}

    Notes
    -----
    This function allows for dynamic modification of the Spark session's
    configuration by removing specified keys and updating or adding new
    key-value pairs.
    """
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


def _decode_jwt_claims(token: str) -> Dict[str, object]:
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload.encode()))


def _token_expires_soon(token: str, refresh_window_seconds: int = 120) -> bool:
    try:
        claims = _decode_jwt_claims(token)
    except Exception:
        return True
    exp = int(claims.get("exp", 0))
    now = int(time.time())
    return exp <= now + max(refresh_window_seconds, 1)


def _request_oauth_tokens(
    data: Dict[str, str],
    token_endpoint: Optional[str] = None,
    timeout_seconds: int = 20,
) -> Tuple[str, Optional[str]]:
    endpoint = token_endpoint or os.getenv("POLARIS_OAUTH2_SERVER_URI")
    if not endpoint:
        raise RuntimeError("POLARIS_OAUTH2_SERVER_URI is required to mint or refresh a user token")

    resp = requests.post(endpoint, data=data, timeout=timeout_seconds)
    resp.raise_for_status()
    payload = resp.json()

    access_token = payload.get("access_token")
    if not access_token:
        raise RuntimeError("Token endpoint did not return access_token")

    return access_token, payload.get("refresh_token")


def mint_polaris_user_token(
    username: Optional[str],
    password: Optional[str],
    client_id: str,
    client_secret: Optional[str] = None,
    token_endpoint: Optional[str] = None,
) -> str:
    if not username or not password:
        raise RuntimeError("username and password are required for password grant token minting")

    data = {
        "grant_type": "password",
        "client_id": client_id,
        "username": username,
        "password": password,
        "scope": "openid profile email",
    }
    if client_secret:
        data["client_secret"] = client_secret

    access_token, _ = _request_oauth_tokens(data=data, token_endpoint=token_endpoint)
    return access_token


def refresh_polaris_user_token(
    refresh_token: str,
    client_id: str,
    client_secret: Optional[str] = None,
    token_endpoint: Optional[str] = None,
) -> Tuple[str, Optional[str]]:
    if not refresh_token:
        raise RuntimeError("refresh_token is required for refresh grant")

    data = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "refresh_token": refresh_token,
    }
    if client_secret:
        data["client_secret"] = client_secret

    return _request_oauth_tokens(data=data, token_endpoint=token_endpoint)


def ensure_fresh_polaris_user_token(
    current_token: Optional[str],
    username: Optional[str],
    password: Optional[str],
    client_id: str,
    client_secret: Optional[str] = None,
    refresh_token: Optional[str] = None,
    allow_password_fallback: bool = True,
    refresh_window_seconds: int = 120,
    token_endpoint: Optional[str] = None,
) -> Tuple[str, Optional[str], bool]:
    if current_token and not _token_expires_soon(current_token, refresh_window_seconds):
        return current_token, refresh_token, False

    if refresh_token:
        try:
            refreshed_access, refreshed_refresh = refresh_polaris_user_token(
                refresh_token=refresh_token,
                client_id=client_id,
                client_secret=client_secret,
                token_endpoint=token_endpoint,
            )
            return refreshed_access, (refreshed_refresh or refresh_token), True
        except requests.RequestException:
            if not allow_password_fallback:
                raise

    if allow_password_fallback and username and password:
        minted = mint_polaris_user_token(
            username=username,
            password=password,
            client_id=client_id,
            client_secret=client_secret,
            token_endpoint=token_endpoint,
        )
        return minted, refresh_token, True

    raise RuntimeError(
        "Unable to obtain a fresh Polaris user token. "
        "Provide POLARIS_REFRESH_TOKEN or enable password-grant fallback credentials."
    )


def apply_polaris_token_to_spark(
    spark,
    token: str,
    catalog_name: str = "iceberg",
    realm: Optional[str] = None,
) -> None:
    active_realm = realm or os.getenv("POLARIS_DEFAULT_REALM", "teehr")
    base = f"spark.sql.catalog.{catalog_name}"
    spark.conf.set(f"{base}.rest.auth.type", "oauth2")
    spark.conf.set(f"{base}.token", token)
    spark.conf.set(f"{base}.rest.auth.oauth2.token", token)
    spark.conf.set(f"{base}.header.X-Polaris-Realm", active_realm)
    spark.conf.set(f"{base}.rest.transport.header.X-Polaris-Realm", active_realm)


def _as_bool_str(value: str, default: str = "true") -> str:
    normalized = (value or default).strip().lower()
    return "true" if normalized in ("1", "true", "t", "yes", "y", "on") else "false"


def _apply_runtime_spark_configs(spark, configs: Dict[str, str]) -> None:
    immutable_after_start = {
        "spark.jars",
        "spark.driver.extraClassPath",
        "spark.executor.extraClassPath",
    }
    for key, value in configs.items():
        if not key.startswith("spark."):
            continue
        if key in immutable_after_start:
            continue
        spark.conf.set(key, value)


def _is_http_error_with_status(exc: Exception, status_code: int) -> bool:
    if not isinstance(exc, requests.HTTPError):
        return False
    response = getattr(exc, "response", None)
    return bool(response is not None and response.status_code == status_code)


def _normalize_internal_broker_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme != "https":
        return url
    if parsed.hostname != "teehr-api":
        return url

    host = parsed.hostname
    port = parsed.port or 8000
    netloc = f"{host}:{port}"
    return urlunsplit(("http", netloc, parsed.path, parsed.query, parsed.fragment))


def _broker_session_endpoint_from_token_endpoint(token_endpoint: str) -> str:
    normalized = _normalize_internal_broker_url(token_endpoint)
    parsed = urlsplit(normalized)
    path = parsed.path or ""

    if path.endswith("/auth/polaris-token/session"):
        return normalized

    if path.endswith("/auth/polaris-token"):
        session_path = path[:-len("/auth/polaris-token")] + "/auth/polaris-token/session"
        return urlunsplit((parsed.scheme, parsed.netloc, session_path, parsed.query, parsed.fragment))

    raise RuntimeError(
        "POLARIS_BROKER_URL must end with /auth/polaris-token "
        "(or /auth/polaris-token/session if already session-scoped)"
    )


def ensure_broker_session_token(
    *,
    user_id: str,
    session_id: str,
    realm: str,
    refresh_token: str,
    bearer_token: Optional[str] = None,
    catalog: str = "iceberg",
    audience: Optional[str] = None,
    broker_url: Optional[str] = None,
    timeout_seconds: int = 20,
) -> str:
    endpoint = broker_url or os.getenv("POLARIS_BROKER_URL", "http://teehr-api:8000/auth/polaris-token")
    endpoint = _normalize_internal_broker_url(endpoint)
    session_endpoint = _broker_session_endpoint_from_token_endpoint(endpoint).replace(
        "/auth/polaris-token/session",
        "/auth/polaris-session",
    )
    active_audience = audience or os.getenv("POLARIS_BROKER_AUDIENCE", "account")
    subject_token = bearer_token or os.getenv("POLARIS_USER_TOKEN", "")

    if not subject_token:
        raise RuntimeError("A valid bearer subject token is required to create a broker session")
    if not refresh_token:
        raise RuntimeError("POLARIS_REFRESH_TOKEN is required to create a broker session")

    resp = requests.post(
        session_endpoint,
        headers={"Authorization": f"Bearer {subject_token}"},
        json={
            "user_id": user_id,
            "session_id": session_id,
            "realm": realm,
            "catalog": catalog,
            "audience": active_audience,
            "refresh_token": refresh_token,
        },
        timeout=timeout_seconds,
    )
    resp.raise_for_status()
    payload = resp.json()
    broker_session_token = payload.get("broker_session_token")
    if not broker_session_token:
        raise RuntimeError("Broker session endpoint did not return broker_session_token")
    os.environ["POLARIS_BROKER_SESSION_TOKEN"] = broker_session_token
    return broker_session_token


def _build_polaris_auth_configs(
    polaris_token: Optional[str],
    use_authmanager: Optional[bool],
) -> Dict[str, str]:
    """Build Spark configs for Polaris catalog authentication.

    Handles three auth paths:
    1. AuthManager (use_authmanager=True or POLARIS_USE_AUTHMANAGER=true env var)
       Uses the teehr-api broker for token management — required for JupyterHub
       where tokens must be refreshed transparently during long sessions.
    2. Direct user token (polaris_token provided)
       Passes the JWT directly to the Iceberg REST catalog.
    3. Service account client credentials (POLARIS_CLIENT_ID + POLARIS_CLIENT_SECRET)
       Used by Prefect batch jobs and other non-interactive service accounts.

    Returns an empty dict if none of the above are configured.
    """
    polaris_realm = os.getenv("POLARIS_DEFAULT_REALM", "teehr")

    resolved_use_authmanager = (
        use_authmanager if use_authmanager is not None
        else _as_bool_str(os.getenv("POLARIS_USE_AUTHMANAGER", "false")) == "true"
    )

    configs: Dict[str, str] = {}

    if not resolved_use_authmanager and not polaris_token and not os.getenv("POLARIS_CLIENT_ID"):
        return configs

    # Realm headers are required for all Polaris auth paths
    configs["spark.sql.catalog.iceberg.header.X-Polaris-Realm"] = polaris_realm
    configs["spark.sql.catalog.iceberg.rest.transport.header.X-Polaris-Realm"] = polaris_realm

    if resolved_use_authmanager:
        broker_url = os.getenv("POLARIS_BROKER_URL", "http://teehr-api:8000/auth/polaris-token")
        broker_url = _normalize_internal_broker_url(broker_url)
        broker_session_url = _broker_session_endpoint_from_token_endpoint(broker_url)
        authmanager_user_id = os.getenv("JUPYTERHUB_USER", "admin")
        authmanager_session_id = (
            os.getenv("JUPYTERHUB_SERVER_NAME", "").strip() or authmanager_user_id
        )
        broker_audience = os.getenv("POLARIS_BROKER_AUDIENCE", "account")
        refresh_token = os.getenv("POLARIS_REFRESH_TOKEN", "")
        current_user_token = polaris_token or os.getenv("POLARIS_USER_TOKEN", "")
        broker_session_token = os.getenv("POLARIS_BROKER_SESSION_TOKEN", "")
        if broker_session_token and _token_expires_soon(broker_session_token, 300):
            broker_session_token = ""
        authmanager_jar = os.getenv(
            "POLARIS_AUTHMANAGER_JAR",
            "/opt/spark/jars/teehr-authmanager.jar",
        )

        current_user_token, refresh_token, _ = ensure_fresh_polaris_user_token(
            current_token=current_user_token,
            username=os.getenv("POLARIS_USERNAME"),
            password=os.getenv("POLARIS_PASSWORD"),
            client_id=os.getenv("POLARIS_CLIENT_ID", "jupyterhub"),
            client_secret=os.getenv("POLARIS_CLIENT_SECRET"),
            refresh_token=refresh_token,
            allow_password_fallback=False,
            refresh_window_seconds=300,
            token_endpoint=os.getenv("POLARIS_OAUTH2_TOKEN_ENDPOINT"),
        )
        os.environ["POLARIS_USER_TOKEN"] = current_user_token
        if refresh_token:
            os.environ["POLARIS_REFRESH_TOKEN"] = refresh_token

        if not broker_session_token:
            try:
                broker_session_token = ensure_broker_session_token(
                    user_id=authmanager_user_id,
                    session_id=authmanager_session_id,
                    realm=polaris_realm,
                    refresh_token=refresh_token,
                    bearer_token=current_user_token,
                    catalog="iceberg",
                    audience=broker_audience,
                    broker_url=broker_url,
                )
            except requests.HTTPError as exc:
                if not _is_http_error_with_status(exc, 401):
                    raise
                current_user_token, refresh_token, _ = ensure_fresh_polaris_user_token(
                    current_token=None,
                    username=os.getenv("POLARIS_USERNAME"),
                    password=os.getenv("POLARIS_PASSWORD"),
                    client_id=os.getenv("POLARIS_CLIENT_ID", "jupyterhub"),
                    client_secret=os.getenv("POLARIS_CLIENT_SECRET"),
                    refresh_token=refresh_token,
                    allow_password_fallback=True,
                    refresh_window_seconds=300,
                    token_endpoint=os.getenv("POLARIS_OAUTH2_TOKEN_ENDPOINT"),
                )
                os.environ["POLARIS_USER_TOKEN"] = current_user_token
                if refresh_token:
                    os.environ["POLARIS_REFRESH_TOKEN"] = refresh_token
                broker_session_token = ensure_broker_session_token(
                    user_id=authmanager_user_id,
                    session_id=authmanager_session_id,
                    realm=polaris_realm,
                    refresh_token=refresh_token,
                    bearer_token=current_user_token,
                    catalog="iceberg",
                    audience=broker_audience,
                    broker_url=broker_url,
                )
            os.environ["POLARIS_BROKER_SESSION_TOKEN"] = broker_session_token

        configs["spark.sql.catalog.iceberg.rest.auth.type"] = (
            "org.teehr.iceberg.auth.TeehrBrokerAuthManager"
        )
        configs["spark.sql.catalog.iceberg.rest.auth.teehr.broker.url"] = broker_session_url
        configs["spark.sql.catalog.iceberg.rest.auth.teehr.user-id"] = authmanager_user_id
        configs["spark.sql.catalog.iceberg.rest.auth.teehr.session-id"] = authmanager_session_id
        configs["spark.sql.catalog.iceberg.rest.auth.teehr.realm"] = polaris_realm
        configs["spark.sql.catalog.iceberg.rest.auth.teehr.catalog"] = "iceberg"
        configs["spark.sql.catalog.iceberg.rest.auth.teehr.audience"] = broker_audience
        configs["spark.sql.catalog.iceberg.rest.auth.teehr.broker-session-token-env"] = (
            "POLARIS_BROKER_SESSION_TOKEN"
        )
        configs["spark.jars"] = authmanager_jar

    elif polaris_token:
        configs["spark.sql.catalog.iceberg.rest.auth.type"] = "oauth2"
        configs["spark.sql.catalog.iceberg.token"] = polaris_token
        configs["spark.sql.catalog.iceberg.rest.auth.oauth2.token"] = polaris_token

    else:
        # Service account / client credentials path.
        # Set POLARIS_CLIENT_ID and POLARIS_CLIENT_SECRET for the service account
        # (e.g. prefect-polaris for Prefect batch jobs).
        oauth_server_uri = os.getenv("POLARIS_OAUTH2_SERVER_URI")
        polaris_client_id = os.getenv("POLARIS_CLIENT_ID")
        polaris_client_secret = os.getenv("POLARIS_CLIENT_SECRET")

        configs["spark.sql.catalog.iceberg.rest.auth.type"] = "oauth2"
        configs["spark.sql.catalog.iceberg.scope"] = "openid"
        configs["spark.sql.catalog.iceberg.rest.auth.oauth2.scope"] = "openid"
        if oauth_server_uri:
            configs["spark.sql.catalog.iceberg.oauth2-server-uri"] = oauth_server_uri
            configs["spark.sql.catalog.iceberg.rest.auth.oauth2.server-uri"] = oauth_server_uri
        if polaris_client_id and polaris_client_secret:
            credential = f"{polaris_client_id}:{polaris_client_secret}"
            configs["spark.sql.catalog.iceberg.credential"] = credential
            configs["spark.sql.catalog.iceberg.rest.auth.oauth2.credential"] = credential

    return configs


def create_minio_spark_session(
    polaris_token: Optional[str] = None,
    force_recreate_session: bool = False,
    update_configs: Optional[Dict[str, str]] = None,
    use_authmanager: Optional[bool] = None,
) -> SparkSession:
    """Start a Spark session with MinIO credentials for local KinD development.

    Thin wrapper around create_spark_session() that injects MinIO-specific S3
    configuration. All Polaris auth (AuthManager, direct token, client credentials)
    is handled by create_spark_session() based on the parameters and environment.

    For remote deployments using AWS S3, call create_spark_session() directly with
    appropriate AWS credentials and catalog configuration.
    """
    s3_endpoint = os.getenv("REMOTE_CATALOG_S3_ENDPOINT", "http://minio:9000")
    s3_path_style = _as_bool_str(os.getenv("REMOTE_CATALOG_S3_PATH_STYLE_ACCESS", "true"))
    s3_region = os.getenv("AWS_REGION", "us-east-2")
    polaris_realm = os.getenv("POLARIS_DEFAULT_REALM", "teehr")
    remote_catalog_uri = os.getenv("REMOTE_CATALOG_REST_URI", "http://polaris:8181/api/catalog")

    # Polaris REST expects the catalog name as warehouse identifier, not an S3 URI
    remote_warehouse_dir = (
        polaris_realm
        if remote_catalog_uri.rstrip("/").endswith("/api/catalog")
        else os.getenv("REMOTE_WAREHOUSE_S3_PATH", "s3://warehouse/")
    )

    minio_configs: Dict[str, str] = {
        "spark.sql.catalog.iceberg.s3.endpoint": s3_endpoint,
        "spark.sql.catalog.iceberg.s3.path-style-access": s3_path_style,
        "spark.sql.catalog.iceberg.s3.region": s3_region,
        "spark.hadoop.fs.s3a.endpoint": s3_endpoint,
        "spark.hadoop.fs.s3a.path.style.access": s3_path_style,
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    }
    if update_configs:
        minio_configs.update(update_configs)

    return create_spark_session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
        remote_catalog_uri=remote_catalog_uri,
        remote_warehouse_dir=remote_warehouse_dir,
        polaris_token=polaris_token,
        force_recreate_session=force_recreate_session,
        update_configs=minio_configs,
        use_authmanager=use_authmanager,
    )




def request_broker_polaris_token(
    *,
    user_id: str,
    session_id: str,
    bearer_token: str,
    realm: Optional[str] = None,
    catalog: str = "iceberg",
    requested_ttl_seconds: int = 600,
    audience: Optional[str] = None,
    broker_url: Optional[str] = None,
    timeout_seconds: int = 20,
) -> Tuple[str, int, str]:
    endpoint = broker_url or os.getenv("POLARIS_BROKER_URL", "http://teehr-api:8000/auth/polaris-token")
    active_realm = realm or os.getenv("POLARIS_DEFAULT_REALM", "teehr")
    active_audience = audience or os.getenv("POLARIS_BROKER_AUDIENCE", "account")

    if not bearer_token:
        raise RuntimeError("bearer_token is required to request a broker token")

    resp = requests.post(
        endpoint,
        headers={"Authorization": f"Bearer {bearer_token}"},
        json={
            "user_id": user_id,
            "session_id": session_id,
            "realm": active_realm,
            "catalog": catalog,
            "requested_ttl_seconds": requested_ttl_seconds,
            "audience": active_audience,
        },
        timeout=timeout_seconds,
    )
    resp.raise_for_status()
    payload = resp.json()
    access_token = payload.get("access_token")
    expires_at = int(payload.get("expires_at_epoch_seconds", 0))
    trace_id = payload.get("trace_id", "")

    if not access_token or expires_at <= 0:
        raise RuntimeError("Broker response missing access_token or expires_at_epoch_seconds")

    return access_token, expires_at, trace_id


def ensure_fresh_polaris_token_via_broker(
    *,
    current_token: Optional[str],
    user_id: str,
    session_id: str,
    bearer_token: str,
    realm: Optional[str] = None,
    catalog: str = "iceberg",
    refresh_window_seconds: int = 120,
    requested_ttl_seconds: int = 600,
    audience: Optional[str] = None,
    broker_url: Optional[str] = None,
) -> Tuple[str, bool]:
    if current_token and not _token_expires_soon(current_token, refresh_window_seconds):
        return current_token, False

    refreshed_token, _, _ = request_broker_polaris_token(
        user_id=user_id,
        session_id=session_id,
        bearer_token=bearer_token,
        realm=realm,
        catalog=catalog,
        requested_ttl_seconds=requested_ttl_seconds,
        audience=audience,
        broker_url=broker_url,
    )
    return refreshed_token, True