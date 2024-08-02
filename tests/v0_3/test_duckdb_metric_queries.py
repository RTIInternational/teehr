"""Test duckdb metric queries."""
import pandas as pd
import geopandas as gpd
import pytest
from pydantic import ValidationError
import teehr_v0_3.queries.duckdb as tqd
from pathlib import Path

TEST_STUDY_DIR = Path("tests", "v0_3", "data", "test_study")
PRIMARY_FILEPATH_DUPS = Path(TEST_STUDY_DIR, "timeseries", "*dup_obs.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_fcast.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")


def test_metric_query_str():
    """Test return metric query as a string."""
    query_str = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH_DUPS,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=["primary_location_id"],
        order_by=["primary_location_id"],
        include_metrics="all",
        return_query=True,
        remove_duplicates=True
    )
    # print(query_str)
    assert isinstance(query_str, str)


def test_metric_query_df():
    """Test return metric query as a dataframe."""
    query_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH_DUPS,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        group_by=["primary_location_id"],
        order_by=["primary_location_id"],
        include_metrics="all",
        return_query=False,
        remove_duplicates=True
    )
    # print(query_df)
    assert len(query_df) == 3
    assert isinstance(query_df, pd.DataFrame)


def test_metric_query_gdf():
    """Test return metric query as a geodataframe."""
    query_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH_DUPS,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=["primary_location_id"],
        order_by=["primary_location_id"],
        include_metrics="all",
        return_query=False,
        include_geometry=True,
        remove_duplicates=True
    )
    # print(query_df)
    assert len(query_df) == 3
    assert isinstance(query_df, gpd.GeoDataFrame)


def test_metric_query_gdf_2():
    """Test return metric query as a geodataframe v2."""
    query_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH_DUPS,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=["primary_location_id", "reference_time"],
        order_by=["primary_location_id"],
        include_metrics="all",
        return_query=False,
        include_geometry=True,
        remove_duplicates=True
    )
    # print(query_df)
    assert len(query_df) == 9
    assert isinstance(query_df, gpd.GeoDataFrame)


def test_metric_query_gdf_no_geom():
    """Test metric query no geometry."""
    with pytest.raises(ValidationError):
        tqd.get_metrics(
            primary_filepath=PRIMARY_FILEPATH_DUPS,
            secondary_filepath=SECONDARY_FILEPATH,
            crosswalk_filepath=CROSSWALK_FILEPATH,
            group_by=["primary_location_id", "reference_time"],
            order_by=["primary_location_id"],
            include_metrics="all",
            return_query=False,
            include_geometry=True,
            remove_duplicates=True
        )


def test_metric_query_gdf_missing_group_by():
    """Test metric query missing group by."""
    with pytest.raises(ValidationError):
        tqd.get_metrics(
            primary_filepath=PRIMARY_FILEPATH_DUPS,
            secondary_filepath=SECONDARY_FILEPATH,
            crosswalk_filepath=CROSSWALK_FILEPATH,
            geometry_filepath=GEOMETRY_FILEPATH,
            group_by=["reference_time"],
            order_by=["primary_location_id"],
            include_metrics="all",
            return_query=False,
            include_geometry=True,
            remove_duplicates=True
        )


def test_metric_query_df_2():
    """Test metric query as a dataframe v2."""
    include_metrics = [
        "primary_count",
        "secondary_count",
        "primary_minimum",
        "secondary_minimum",
        "primary_maximum",
        "secondary_maximum",
        "primary_average",
        "secondary_average",
        "primary_sum",
        "secondary_sum",
        "primary_variance",
        "secondary_variance",
        "max_value_delta",
        "mean_absolute_error",
        "nash_sutcliffe_efficiency",
        "nash_sutcliffe_efficiency_normalized",
        "kling_gupta_efficiency",
        "mean_error",
        "mean_squared_error",
        "root_mean_squared_error",
        "relative_bias",
        "multiplicative_bias",
        "mean_absolute_relative_error",
        "pearson_correlation",
        "r_squared"
    ]
    group_by = ["primary_location_id"]
    query_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH_DUPS,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        group_by=group_by,
        order_by=["primary_location_id"],
        include_metrics=include_metrics,
        return_query=False,
        include_geometry=False,
        remove_duplicates=True,
    )
    # print(query_df)
    assert len(query_df) == 3
    assert len(query_df.columns) == len(group_by) + len(include_metrics)
    assert isinstance(query_df, pd.DataFrame)


def test_metric_query_df_time_metrics():
    """Test metric query with time metrics."""
    include_metrics = [
        "primary_max_value_time",
        "secondary_max_value_time",
        "max_value_timedelta",
    ]
    group_by = ["primary_location_id", "reference_time"]
    query_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH_DUPS,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=group_by,
        order_by=["primary_location_id", "reference_time"],
        include_metrics=include_metrics,
        return_query=False,
        include_geometry=False,
        remove_duplicates=True
    )
    # print(query_df)
    assert len(query_df) == 9
    assert len(query_df.columns) == len(group_by) + len(include_metrics)
    assert isinstance(query_df, pd.DataFrame)


def test_metric_query_df_all():
    """Test metric query all metrics."""
    group_by = ["primary_location_id", "reference_time"]
    query_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH_DUPS,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=group_by,
        order_by=["primary_location_id", "reference_time"],
        include_metrics="all",
        return_query=False,
        include_geometry=False,
        remove_duplicates=True
    )
    # print(query_df)
    assert len(query_df) == 9
    assert len(query_df.columns) == len(group_by) + 32
    assert isinstance(query_df, pd.DataFrame)


def test_metric_query_value_time_filter():
    """Test metric query value time filter."""
    group_by = ["primary_location_id", "reference_time"]
    query_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH_DUPS,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=group_by,
        order_by=["primary_location_id", "reference_time"],
        include_metrics="all",
        return_query=False,
        include_geometry=False,
        filters=[
            {
                "column": "value_time",
                "operator": ">=",
                "value": f"{'2022-01-01 13:00:00'}",
            },
            {
                "column": "reference_time",
                "operator": ">=",
                "value": f"{'2022-01-01 02:00:00'}",
            },
        ],
        remove_duplicates=True
    )
    # print(query_df)
    assert len(query_df) == 3
    assert query_df["primary_count"].iloc[0] == 13
    assert isinstance(query_df, pd.DataFrame)


def test_metric_query_config_filter():
    """Test metric query config filter."""
    group_by = ["primary_location_id", "reference_time"]
    query_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH_DUPS,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=group_by,
        order_by=["primary_location_id", "reference_time"],
        include_metrics="all",
        return_query=False,
        include_geometry=False,
        filters=[
            {
                "column": "configuration",
                "operator": "=",
                "value": "test_short",
            },
        ],
        remove_duplicates=True
    )
    # print(query_df)
    assert len(query_df) == 9
    assert isinstance(query_df, pd.DataFrame)


if __name__ == "__main__":

    test_metric_query_str()
    test_metric_query_df()
    test_metric_query_gdf()
    test_metric_query_gdf_2()
    test_metric_query_gdf_no_geom()
    test_metric_query_gdf_missing_group_by()
    test_metric_query_df_2()
    test_metric_query_df_time_metrics()
    test_metric_query_df_all()
    test_metric_query_value_time_filter()
    pass
