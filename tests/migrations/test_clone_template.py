"""Tests for Iceberg."""
import pytest


@pytest.mark.read_only_evaluation_template
def test_clone_template(read_only_evaluation_template):
    """Test creating a new study."""
    ev = read_only_evaluation_template

    assert ev.spark.sql("SELECT * FROM attributes").count() == 0
    assert ev.spark.sql("SELECT * FROM locations").count() == 0
    assert ev.spark.sql("SELECT * FROM location_attributes").count() == 0
    assert ev.spark.sql("SELECT * FROM location_crosswalks").count() == 0
    assert ev.spark.sql("SELECT * FROM units").count() == 4
    assert ev.spark.sql("SELECT * FROM variables").count() == 4
    assert ev.spark.sql("SELECT * FROM configurations").count() == 0
    assert ev.spark.sql("SELECT * FROM primary_timeseries").count() == 0
    assert ev.spark.sql("SELECT * FROM secondary_timeseries").count() == 0
