"""Test pandas metric queries."""
import pandas as pd
import geopandas as gpd
# import pytest
# from pydantic import ValidationError
import teehr_v0_3.queries.pandas as tqk
from pathlib import Path

TEST_STUDY_DIR = Path("tests", "data", "test_study")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*short_obs.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_fcast.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")


def test_metric_query_df():
    """Test metric query df."""
    query_df = tqk.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        group_by=["primary_location_id"],
        order_by=["primary_location_id"],
        include_metrics="all",
        return_query=False,
    )
    # print(query_df)
    assert len(query_df) == 3
    assert isinstance(query_df, pd.DataFrame)


def test_metric_query_df2():
    """Test metric query df v2."""
    query_df = tqk.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        group_by=["primary_location_id", "reference_time"],
        order_by=["primary_location_id"],
        include_metrics="all",
        return_query=False,
    )
    # print(query_df)
    assert len(query_df) == 9
    assert isinstance(query_df, pd.DataFrame)


def test_metric_query_filter_df():
    """Test metric query df with filter."""
    query_df = tqk.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        group_by=["primary_location_id", "reference_time"],
        order_by=["primary_location_id"],
        include_metrics="all",
        return_query=False,
        filters=[
            {
                "column": "primary_location_id",
                "operator": "=",
                "value": "gage-A"
            },
        ]
    )
    # print(query_df)
    assert len(query_df) == 3
    assert isinstance(query_df, pd.DataFrame)


def test_metric_query_gdf():
    """Test metric query gdf."""
    query_df = tqk.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=["primary_location_id"],
        order_by=["primary_location_id"],
        include_metrics="all",
        return_query=False,
        include_geometry=True,
    )
    # print(query_df)
    assert len(query_df) == 3
    assert isinstance(query_df, gpd.GeoDataFrame)


def test_metric_query_df_limit_metrics():
    """Test metric query df limit metrics."""
    include_metrics = [
        "mean_absolute_error",
        "root_mean_squared_error",
        "nash_sutcliffe_efficiency",
        "kling_gupta_efficiency",
        "mean_error",
        "mean_squared_error",
        ]
    group_by = ["primary_location_id"]
    query_df = tqk.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=group_by,
        order_by=["primary_location_id"],
        include_metrics=include_metrics,
        return_query=False,
        include_geometry=False,
    )
    # print(query_df)
    assert len(query_df) == 3
    assert len(query_df.columns) == len(group_by) + len(include_metrics)
    assert isinstance(query_df, pd.DataFrame)


def test_metric_query_df_time_metrics():
    """Test metric query df time metrics."""
    include_metrics = [
        "primary_max_value_time",
        "secondary_max_value_time",
        "max_value_timedelta"
    ]
    group_by = ["primary_location_id", "reference_time"]
    query_df = tqk.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=group_by,
        order_by=["primary_location_id", "reference_time"],
        include_metrics=include_metrics,
        return_query=False,
        include_geometry=False,
    )
    # print(query_df)
    assert len(query_df) == 9
    assert len(query_df.columns) == len(group_by) + len(include_metrics)
    assert isinstance(query_df, pd.DataFrame)


if __name__ == "__main__":
    test_metric_query_df()
    test_metric_query_df2()
    test_metric_query_filter_df()
    test_metric_query_gdf()
    test_metric_query_df_limit_metrics()
    test_metric_query_df_time_metrics()
    pass
