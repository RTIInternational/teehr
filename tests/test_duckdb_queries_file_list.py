"""Test duckdb metric queries."""
import pandas as pd
import numpy as np
import geopandas as gpd
import pytest
from pydantic import ValidationError
import teehr.queries.duckdb as tqd
import teehr.queries.duckdb_database as tqbd
from pathlib import Path

TEST_STUDY_DIR = Path("tests", "data", "test_study")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "test_short_obs*.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "test_short_fcast*.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")
PRIMARY_FILEPATH_LIST = [
    Path(TEST_STUDY_DIR, "timeseries", "test_short_obs.parquet"),
    Path(TEST_STUDY_DIR, "timeseries", "test_short_obs_2.parquet"),
]
SECONDARY_FILEPATH_LIST = [
    Path(TEST_STUDY_DIR, "timeseries", "test_short_fcast.parquet"),
    Path(TEST_STUDY_DIR, "timeseries", "test_short_fcast_2.parquet"),
]


def test_metric_query_df():
    """Test return metric query as a dataframe."""
    query_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        group_by=["primary_location_id", "reference_time"],
        order_by=["primary_location_id", "reference_time"],
        include_metrics="all",
        return_query=False,
        remove_duplicates=True
    )
    # print(query_df)

    query_df_list = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH_LIST,
        secondary_filepath=SECONDARY_FILEPATH_LIST,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        group_by=["primary_location_id", "reference_time"],
        order_by=["primary_location_id", "reference_time"],
        include_metrics="all",
        return_query=False,
        remove_duplicates=True
    )
    # print(query_df_list)

    assert len(query_df) == 18
    assert len(query_df_list) == 18
    assert isinstance(query_df, pd.DataFrame)
    assert isinstance(query_df_list, pd.DataFrame)


def test_metric_compare_1():
    """Test metric compare v1."""
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
        "kling_gupta_efficiency_mod1",
        "kling_gupta_efficiency_mod2",
        "mean_error",
        "mean_squared_error",
        "root_mean_squared_error",
        "relative_bias",
        "multiplicative_bias",
        "mean_absolute_relative_error",
        "pearson_correlation",
        "r_squared",
        "annual_peak_relative_bias",
        "spearman_correlation",
    ]
    group_by = [
        "primary_location_id",
        "reference_time"
    ]
    order_by = [
        "primary_location_id",
        "reference_time"
    ]

    wildcard_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=group_by,
        order_by=order_by,
        include_metrics=include_metrics,
        return_query=False
    )

    list_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH_LIST,
        secondary_filepath=SECONDARY_FILEPATH_LIST,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=group_by,
        order_by=order_by,
        include_metrics=include_metrics,
        return_query=False
    )

    for m in include_metrics:
        # print(m)
        wildcard_np = wildcard_df[m].to_numpy()
        list_np = list_df[m].to_numpy()
        assert np.allclose(wildcard_np, list_np)


def test_metric_query_gdf():
    """Test return metric query as a geodataframe."""
    query_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH_LIST,
        secondary_filepath=SECONDARY_FILEPATH_LIST,
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
        primary_filepath=PRIMARY_FILEPATH_LIST,
        secondary_filepath=SECONDARY_FILEPATH_LIST,
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
    assert len(query_df) == 18
    assert isinstance(query_df, gpd.GeoDataFrame)


def test_metric_query_gdf_no_geom():
    """Test metric query no geometry."""
    with pytest.raises(ValidationError):
        tqd.get_metrics(
            primary_filepath=PRIMARY_FILEPATH_LIST,
            secondary_filepath=SECONDARY_FILEPATH_LIST,
            crosswalk_filepath=CROSSWALK_FILEPATH,
            group_by=["primary_location_id", "reference_time"],
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
        primary_filepath=PRIMARY_FILEPATH_LIST,
        secondary_filepath=SECONDARY_FILEPATH_LIST,
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
        primary_filepath=PRIMARY_FILEPATH_LIST,
        secondary_filepath=SECONDARY_FILEPATH_LIST,
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
    assert len(query_df) == 18
    assert len(query_df.columns) == len(group_by) + len(include_metrics)
    assert isinstance(query_df, pd.DataFrame)


def test_metric_query_df_all():
    """Test metric query all metrics."""
    group_by = ["primary_location_id", "reference_time"]
    query_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH_LIST,
        secondary_filepath=SECONDARY_FILEPATH_LIST,
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
    assert len(query_df) == 18
    assert len(query_df.columns) == len(group_by) + 32
    assert isinstance(query_df, pd.DataFrame)


def test_metric_query_value_time_filter():
    """Test metric query value time filter."""
    group_by = ["primary_location_id", "reference_time"]
    query_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH_LIST,
        secondary_filepath=SECONDARY_FILEPATH_LIST,
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
            {
                "column": "reference_time",
                "operator": "<",
                "value": f"{'2023-01-01 00:00:00'}",
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
        primary_filepath=PRIMARY_FILEPATH_LIST,
        secondary_filepath=SECONDARY_FILEPATH_LIST,
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


def test_get_joined_timeseries():
    """Test get joined timeseries."""
    df = tqd.get_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH_LIST,
        secondary_filepath=SECONDARY_FILEPATH_LIST,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        order_by=["primary_location_id", "value_time"],
        filters=[
            {
                "column": "primary_location_id",
                "operator": "=",
                "value": "gage-A",
            },
        ],
        remove_duplicates=True,
    )
    # print(df)
    assert len(df) == 144


def test_get_timeseries_chars():
    """Test get timeseries chars."""
    df = tqd.get_timeseries_chars(
        timeseries_filepath=SECONDARY_FILEPATH_LIST,
        order_by=["location_id", "reference_time"],
        group_by=["location_id", "reference_time"],
        filters=[
            {
                "column": "location_id",
                "operator": "=",
                "value": "fcst-1",
            },
        ]
    )
    # print(df)
    assert len(df) == 6


def test_describe_timeseries():
    """Test describe timeseries."""
    d = tqbd.describe_timeseries(
        timeseries_filepath=PRIMARY_FILEPATH_LIST
    )
    # print(d)
    expected = {
        'Number of unique location IDs': 3,
        'Total number of rows': 156,
        'Start Date': pd.Timestamp('2022-01-01 00:00:00'),
        'End Date': pd.Timestamp('2023-01-02 01:00:00'),
        'Number of duplicate rows': 0,
        'Number of location IDs with duplicate value times': 0,
        'Number of location IDs with missing time steps': 3
    }
    assert d == expected


if __name__ == "__main__":
    test_metric_query_df()
    test_metric_compare_1()
    test_metric_query_gdf()
    test_metric_query_gdf_2()
    test_metric_query_gdf_no_geom()
    test_metric_query_df_2()
    test_metric_query_df_time_metrics()
    test_metric_query_df_all()
    test_metric_query_value_time_filter()
    test_get_joined_timeseries()
    test_get_timeseries_chars()
    test_describe_timeseries()
    pass
