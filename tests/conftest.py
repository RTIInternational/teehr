"""Defines pytest fixtures for all tests."""
import pytest

from teehr.evaluation.spark_session_utils import create_spark_session


@pytest.fixture(scope="session")  # use 'session' for access across all tests
def spark_session():
    """Fixture for Spark session."""
    spark = create_spark_session()
    yield spark
    spark.stop()