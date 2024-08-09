"""Tests for the TEEHR dataset API."""
from pathlib import Path

from teehr_v0_3.classes.duckdb_database_api import DuckDBDatabaseAPI
from teehr_v0_3.models.queries_database import (
    MetricQuery,
    JoinedTimeseriesFieldName,
    JoinedTimeseriesQuery
)

# Test data
TEST_STUDY_DIR = Path("tests", "v0_3", "data", "test_study")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*short_obs.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_fcast.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")
ATTRIBUTES_FILEPATH = Path(TEST_STUDY_DIR, "geo", "test_attr_*.parquet")
DATABASE_FILEPATH = Path("tests", "v0_3", "data", "test_study", "temp_test_api.db")


def test_unique_field_values():
    """Test the unique field values query."""
    tds = DuckDBDatabaseAPI(DATABASE_FILEPATH)

    jtn = JoinedTimeseriesFieldName.model_validate(
        {"field_name": "primary_location_id"}
    )

    df = tds.get_unique_field_values(jtn)
    assert sorted(df["unique_primary_location_id_values"].tolist()) == [
        "gage-A",
        "gage-B",
        "gage-C",
    ]


def test_metrics_query():
    """Test the metrics query."""
    tds = DuckDBDatabaseAPI(DATABASE_FILEPATH)

    # Get metrics
    order_by = ["primary_location_id"]
    group_by = ["primary_location_id"]
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

    mq = MetricQuery.model_validate(
        {
            "group_by": group_by,
            "order_by": order_by,
            "include_metrics": "all",
            "filters": filters,
            "include_geometry": True,
        },
    )
    df = tds.get_metrics(mq)
    assert df.index.size == 1


def test_describe_inputs():
    """Test the describe inputs query."""
    tds = DuckDBDatabaseAPI(DATABASE_FILEPATH)
    df = tds.describe_inputs(PRIMARY_FILEPATH, SECONDARY_FILEPATH)
    assert df.index.size == 7


def test_get_joined_timeseries():
    """Test the get joined timeseries query."""
    tds = DuckDBDatabaseAPI(DATABASE_FILEPATH)
    order_by = ["primary_location_id"]
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

    jtq = JoinedTimeseriesQuery.model_validate(
        {
            "order_by": order_by,
            "filters": filters,
            "include_geometry": False,
        },
    )

    df = tds.get_joined_timeseries(jtq)

    assert df.index.size == 24


if __name__ == "__main__":
    test_unique_field_values()
    test_describe_inputs()
    test_metrics_query()
    test_get_joined_timeseries()
