"""Tests for the TEEHR-DuckDB queries on un-joined parquet file(s)."""
from pathlib import Path
# import numpy as np
# import pandas as pd
# import geopandas as gpd

from teehr.classes.teehr_duckdb import DuckDBParquet

# Test data
TEST_STUDY_DIR = Path("tests", "data", "test_study")
PRIMARY_FILEPATH_DUPS = Path(TEST_STUDY_DIR, "timeseries", "*dup_obs.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_fcast.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")


def test_metrics_query():
    """Test the get_metrics method."""
    tds = DuckDBParquet(
        primary_filepath=PRIMARY_FILEPATH_DUPS,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
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


if __name__ == "__main__":
    test_metrics_query()