"""Test creating a spark session."""
from teehr.evaluation.spark_session_utils import (
    create_spark_session,
    _update_configs_and_packages
)


def test_create_spark_session(spark_shared_session):
    """Test creating a new spark session configuration."""
    spark = spark_shared_session
    assert spark is not None
    conf = spark.sparkContext.getConf()

    _update_configs_and_packages(
        conf=conf,
        update_configs=None,
        add_packages=["com.example:my-package:1.0.0"]
    )
    updated_packages = conf.get("spark.jars.packages").split(",")
    assert "com.example:my-package:1.0.0" in updated_packages

