import pandas as pd
import geopandas as gpd
import pytest
from pydantic import ValidationError
import teehr.queries.duckdb as tqd
from pathlib import Path

TEST_STUDY_DIR = Path("tests", "data", "test_study")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_obs.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_fcast.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")


def test_metric_query_str():
    query_str = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=["primary_location_id"],
        order_by=["primary_location_id"],
        include_metrics="all",
        return_query=True
    )
    # print(query_str)
    assert type(query_str) == str


def test_metric_query_df():

    query_df = tqd.get_metrics(
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


def test_metric_query_gdf():

    query_df = tqd.get_metrics(
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


def test_metric_query_gdf_2():

    query_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=["primary_location_id", "reference_time"],
        order_by=["primary_location_id"],
        include_metrics="all",
        return_query=False,
        include_geometry=True,
    )
    # print(query_df)
    assert len(query_df) == 9
    assert isinstance(query_df, gpd.GeoDataFrame)


def test_metric_query_gdf_no_geom():
    with pytest.raises(ValidationError):
        tqd.get_metrics(
            primary_filepath=PRIMARY_FILEPATH,
            secondary_filepath=SECONDARY_FILEPATH,
            crosswalk_filepath=CROSSWALK_FILEPATH,
            group_by=["primary_location_id", "reference_time"],
            order_by=["primary_location_id"],
            include_metrics="all",
            return_query=False,
            include_geometry=True,
        )


def test_metric_query_gdf_missing_group_by():
    with pytest.raises(ValidationError):
        tqd.get_metrics(
            primary_filepath=PRIMARY_FILEPATH,
            secondary_filepath=SECONDARY_FILEPATH,
            crosswalk_filepath=CROSSWALK_FILEPATH,
            geometry_filepath=GEOMETRY_FILEPATH,
            group_by=["reference_time"],
            order_by=["primary_location_id"],
            include_metrics="all",
            return_query=False,
            include_geometry=True,
        )


def test_metric_query_df_2():

    query_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        group_by=["primary_location_id"],
        order_by=["primary_location_id"],
        include_metrics="all",
        return_query=False,
        include_geometry=False,
    )
    print(query_df)
    assert len(query_df) == 3
    assert isinstance(query_df, pd.DataFrame)


if __name__ == "__main__":
    test_metric_query_str()
    test_metric_query_df()
    test_metric_query_gdf()
    test_metric_query_gdf_2()
    test_metric_query_gdf_no_geom()
    test_metric_query_gdf_missing_group_by()
    test_metric_query_df_2()
    pass
