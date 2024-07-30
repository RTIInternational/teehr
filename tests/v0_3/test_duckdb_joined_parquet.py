"""Tests for the TEEHR dataset queries on exported joined parquet file(s)."""
from pathlib import Path
# import numpy as np
# import pandas as pd
# import geopandas as gpd

from teehr_v0_3.classes.duckdb_joined_parquet import DuckDBJoinedParquet


# Test data
TEST_STUDY_DIR = Path("tests", "data", "test_study")
JOINED_PARQUET_FILEPATH = Path(
    TEST_STUDY_DIR, "timeseries", "test_joined_timeseries_*.parquet"
)
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")


def test_metrics_query():
    """Test the get_metrics method."""
    tds = DuckDBJoinedParquet(
        JOINED_PARQUET_FILEPATH,
        GEOMETRY_FILEPATH
    )

    # Get metrics
    filters = [
        {
            "column": "primary_location_id",
            "operator": "=",
            "value": "gage-A",
        },
        {
            "column": "reference_time",
            "operator": "=",
            "value": "2022-01-01 00:00:00",
        }
    ]

    group_by = ["primary_location_id"]
    order_by = ["primary_location_id"]
    include_metrics = "all"

    df = tds.get_metrics(
        group_by=group_by,
        order_by=order_by,
        include_metrics=include_metrics,
        filters=filters,
        include_geometry=True,
    )

    # print(df)
    assert df.index.size == 1
    assert df.columns.size == 34

    pass


def test_get_joined_timeseries():
    """Test the get_joined_timeseries method."""
    tds = DuckDBJoinedParquet(
        JOINED_PARQUET_FILEPATH,
        GEOMETRY_FILEPATH
    )

    # Get joined timeseries
    order_by = ["primary_location_id"]

    df = tds.get_joined_timeseries(
        order_by=order_by,
        return_query=False,
        include_geometry=True,
    )

    assert df.index.size == 216
    assert df.columns.size == 12
    pass


def test_timeseries_query():
    """Test the get timeseries query."""
    tds = DuckDBJoinedParquet(JOINED_PARQUET_FILEPATH)

    filters = [
        {
            "column": "configuration",
            "operator": "=",
            "value": "test_short",
        },
    ]

    order_by = ["primary_location_id"]

    df = tds.get_timeseries(
        order_by=order_by,
        timeseries_name="primary",
        filters=filters,
        return_query=False
    )

    assert df.index.size == 26 * 3
    pass


def test_timeseries_char_query():
    """Test the get timeseries char query."""
    tds = DuckDBJoinedParquet(JOINED_PARQUET_FILEPATH)

    filters = [
        {
            "column": "configuration",
            "operator": "=",
            "value": "test_short",
        },
    ]
    group_by = ["primary_location_id"]
    order_by = ["primary_location_id"]
    timeseries_name = "primary"  # "primary, secondary"

    df = tds.get_timeseries_chars(
        order_by=order_by,
        group_by=group_by,
        timeseries_name=timeseries_name,
        filters=filters
    )

    assert df.index.size == 3
    pass


def test_unique_field_values():
    """Test the unique field values query."""
    tds = DuckDBJoinedParquet(JOINED_PARQUET_FILEPATH)

    df = tds.get_unique_field_values(
        field_name="primary_location_id"
    )
    assert sorted(df["unique_primary_location_id_values"].tolist()) == [
        "gage-A",
        "gage-B",
        "gage-C",
    ]
    pass


if __name__ == "__main__":
    test_metrics_query()
    test_get_joined_timeseries()
    test_timeseries_query()
    test_timeseries_char_query()
    test_unique_field_values()
