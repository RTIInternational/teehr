"""Test creating a spark session."""
from teehr.evaluation.spark_session_utils import (
    create_spark_session,
    _update_configs_and_packages
)


def test_create_spark_session():
    """Test creating a new spark session configuration."""
    spark = create_spark_session(
        app_name="Test Spark Session",
        add_packages=None,  # the default
        update_configs={
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
            "spark.sql.shuffle.partitions": "4",
        }
    )

    assert spark is not None
    assert spark.sparkContext.appName == "Test Spark Session"
    conf = spark.sparkContext.getConf()
    assert conf.get("spark.hadoop.fs.s3a.aws.credentials.provider") == \
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
    assert conf.get("spark.sql.shuffle.partitions") == "4"

    _update_configs_and_packages(
        conf=conf,
        update_configs=None,
        add_packages=["com.example:my-package:1.0.0"]
    )
    updated_packages = conf.get("spark.jars.packages").split(",")
    assert "com.example:my-package:1.0.0" in updated_packages

    spark.stop()


if __name__ == "__main__":
    test_create_spark_session()
