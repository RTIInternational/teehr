"""Download and extract the e0_2_location_example Evaluation dataset from S3."""
from typing import Union
import botocore.session
from botocore import UNSIGNED
from botocore.config import Config
import tarfile
from pathlib import Path
import os
import teehr
from teehr.evaluation.spark_session_utils import create_spark_session
from teehr import const
import shutil

session = botocore.session.Session()


def download_e0_2_example(temp_dir: Union[str, Path]) -> teehr.Evaluation:
    """Download and extract the e0_2_location_example Evaluation dataset from S3."""
    if not Path(temp_dir).is_dir():
        os.makedirs(temp_dir, exist_ok=True)

    s3_client = session.create_client(
        's3',
        config=Config(signature_version=UNSIGNED)
    )

    bucket_name = "ciroh-rti-public-data"  # Replace with actual bucket
    key = "teehr-data-warehouse/v0_4_evaluations/e0_2_location_example.tar.gz"  # Replace with actual key
    local_path = Path(temp_dir, "e0_2_location_example.tar.gz")
    temp_extract_dir = Path(temp_dir, "e0_2_location_example")


    # Use get_object instead of download_file
    response = s3_client.get_object(Bucket=bucket_name, Key=key)

    # Write the response body to file
    with open(local_path, 'wb') as f:
        f.write(response['Body'].read())

    print(f"✅ Downloaded to {local_path}")

    print("Extracting archive...")
    with tarfile.open(local_path, 'r:gz') as tar:
        tar.extractall(path=temp_dir)
    print("✅ Extraction complete")

    os.remove(local_path)
    print(f"✅ Removed archive {local_path}")

    # Initialize Spark with new tmpdir location
    (Path(temp_dir) / "local").mkdir(parents=True, exist_ok=True)

    # Create a new session for test isolation
    spark = create_spark_session()
    spark.conf.set(
        f"spark.sql.catalog.local.warehouse",
        (Path(temp_dir) / "local").as_posix()
    )
    spark.conf.set(
        f"spark.sql.catalog.local.uri",
        f"jdbc:sqlite:{(Path(temp_dir) / 'local').as_posix()}/{const.LOCAL_CATALOG_DB_NAME}"
    )
    spark.conf.set(
        f"spark.sql.catalog.local.uri",
        f"jdbc:sqlite:{(Path(temp_dir) / 'local').as_posix()}/{const.LOCAL_CATALOG_DB_NAME}"
    )
    # Create the database
    spark.sql("CREATE DATABASE IF NOT EXISTS local.teehr")

    # Define tables to recreate
    tables_to_recreate = [
        "primary_timeseries",
        "secondary_timeseries",
        "joined_timeseries",
        "locations",
        "location_attributes",
        "location_crosswalks",
        "units",
        "variables",
        "attributes",
        "configurations"
    ]

    # For each table, read the parquet data files and recreate the Iceberg table
    for table_name in tables_to_recreate:
        old_table_dir = temp_extract_dir / "local" / "teehr" / table_name / "data"
        # Read all parquet files for this table
        df = spark.read.parquet(str(old_table_dir))
        # Create the Iceberg table
        df.writeTo(f"local.teehr.{table_name}").using("iceberg").create()
        # Removed expensive .count() call for better performance

    # Clean up temp extraction directory
    shutil.rmtree(temp_extract_dir)

    ev = teehr.Evaluation(
        Path(temp_dir),
        create_dir=False,
        spark=spark,
        check_evaluation_version=False
    )

    return ev