"""This module tests the filter functions on primary_timeseries.

This module tests the filter functions on primary_timeseries. It
should apply to all tables.
"""
from teehr import Evaluation
from teehr.evaluation.spark_session_utils import create_spark_session
import pytest


SPARK_SESSION = create_spark_session(app_name="test_sql_query")

@pytest.mark.read_only_test_warehouse
def test_sql_query(read_only_test_warehouse):
    """Test Evaluation sql query."""
    ev = read_only_test_warehouse
    sdf = ev.sql("""
        SELECT pt.*, u.*, c.* FROM primary_timeseries pt
        JOIN units u ON pt.unit_name = u.name
        JOIN configurations c ON pt.configuration_name = c.name
        LIMIT 10;
    """, create_temp_views=["primary_timeseries", "units", "configurations"])
    sdf_cols = sorted(sdf.columns)
    expected_cols = [
        'configuration_name',
        'description',
        'location_id',
        'long_name',
        'name',
        'name',
        'reference_time',
        'type',
        'unit_name',
        'value',
        'value_time',
        'variable_name'
    ]
    assert sdf_cols == expected_cols

@pytest.mark.read_only_evaluation_template
def test_sql_query_on_empty_tables(read_only_evaluation_template):
    """Test sql query on empty table."""
    ev = read_only_evaluation_template
    sdf = ev.sql("""
        SELECT pt.*, u.*, c.* FROM primary_timeseries pt
        JOIN units u ON pt.unit_name = u.name
        JOIN configurations c ON pt.configuration_name = c.name
        LIMIT 10;
    """, create_temp_views=["primary_timeseries", "units", "configurations"])
    assert sdf.isEmpty()
    sdf = ev.sql("""
        SELECT * FROM primary_timeseries;
    """, create_temp_views=["primary_timeseries"])
    assert sdf.isEmpty()
    sdf = ev.sql("""
        SELECT * FROM secondary_timeseries;
    """, create_temp_views=["secondary_timeseries"])
    assert sdf.isEmpty()
    # Drop temp views after test
    ev.spark.catalog.dropTempView("primary_timeseries")
    ev.spark.catalog.dropTempView("units")
    ev.spark.catalog.dropTempView("configurations")
    ev.spark.catalog.dropTempView("secondary_timeseries")