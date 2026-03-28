"""Tests new properties field in locations, location_crosswalks, and location_attributes tables."""
import pandas as pd
import pytest


@pytest.mark.function_scope_test_warehouse
def test_location_properties_spark(function_scope_test_warehouse):
    """Test creating a new study."""
    ev = function_scope_test_warehouse

    # Check that properties column exists in locations table
    sdf = ev.locations.to_sdf()
    assert "properties" in sdf.columns

    # Check that properties column exists in location_crosswalks table
    sdf = ev.location_crosswalks.to_sdf()
    assert "properties" in sdf.columns

    # Check that properties column exists in location_attributes table
    sdf = ev.location_attributes.to_sdf()
    assert "properties" in sdf.columns

    # insert a new location with properties and check that it can be read back
    # start with a dataframe with the new properties column
    from shapely.geometry import Point
    df = ev.spark.createDataFrame([{
        "id": "loc_1",
        "name": "Location 1",
        "geometry": Point(0, 0).wkb,
        "properties": {"key1": "value1", "key2": "value2"}
    }])
    ev.locations.load_dataframe(df, write_mode="append")

    sdf = ev.locations.to_sdf()
    assert "properties" in sdf.columns
    result = sdf.filter("id = 'loc_1'").select("properties").toPandas().iloc[0]["properties"]
    assert result == {"key1": "value1", "key2": "value2"}


@pytest.mark.function_scope_test_warehouse
def test_location_properties_pandas(function_scope_test_warehouse):
    """Test creating a new study."""
    ev = function_scope_test_warehouse

    # Check that properties column exists in locations table
    sdf = ev.locations.to_sdf()
    assert "properties" in sdf.columns

    # Check that properties column exists in location_crosswalks table
    sdf = ev.location_crosswalks.to_sdf()
    assert "properties" in sdf.columns

    # Check that properties column exists in location_attributes table
    sdf = ev.location_attributes.to_sdf()
    assert "properties" in sdf.columns

    # insert a new location with properties and check that it can be read back
    # start with a dataframe with the new properties column
    from shapely.geometry import Point

    df = pd.DataFrame(
        {
            "id": ["loc_1", "loc_2", "loc_3"],
            "name": ["Location 1", "Location 2", "Location 3"],
            "geometry": [Point(0, 0).wkb, Point(1, 1).wkb, Point(2, 2).wkb],
            "properties": [
                {"key1": "value1", "key2": "value2"},
                {"key1": "value3", "key2": "value4"},
                {"key1": "value5", "key2": "value6"}
                ]
        }
    )
    ev.locations.load_dataframe(df, write_mode="append")

    sdf = ev.locations.to_sdf()
    assert "properties" in sdf.columns
    result = sdf.filter("id = 'loc_1'").select("properties").toPandas().iloc[0]["properties"]
    assert result == {"key1": "value1", "key2": "value2"}
