"""Tests for the TEEHR dataset queries on exported joined parquet file(s)."""
from pathlib import Path
# import numpy as np
# import pandas as pd
# import geopandas as gpd

from teehr.database.teehr_dataset import TEEHRDatasetJoinedParquet


# Test data
TEST_STUDY_DIR = Path("tests", "data", "test_study")
JOINED_PARQUET_FILEPATH = Path(
    TEST_STUDY_DIR, "timeseries", "test_joined_timeseries_*.parquet"
)


def test_metrics_query():
    """Test the get_metrics method."""
    tds = TEEHRDatasetJoinedParquet(JOINED_PARQUET_FILEPATH)

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
        },
        # {
        #     "column": "lead_time",
        #     "operator": "<=",
        #     "value": "10 hours"
        # },
    ]

    # TODO: Why doesn't lead_time filter work?

    group_by = ["primary_location_id"]
    order_by = ["primary_location_id"]
    include_metrics = "all"

    df = tds.get_metrics(
        group_by=group_by,
        order_by=order_by,
        include_metrics=include_metrics,
        filters=filters,
        include_geometry=False,
    )

    # print(df)

    pass


def test_get_joined_timeseries():
    """Test the get_joined_timeseries method."""
    tds = TEEHRDatasetJoinedParquet(JOINED_PARQUET_FILEPATH)

    # Get joined timeseries
    order_by = ["primary_location_id"]

    df = tds.get_joined_timeseries(
        order_by=order_by,
        return_query=False,
        include_geometry=False,
    )


if __name__ == "__main__":
    # test_metrics_query()
    test_get_joined_timeseries()

