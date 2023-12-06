import pandas as pd
import geopandas as gpd
import pytest
from pydantic import ValidationError
import teehr.queries.duckdb as tqu
from pathlib import Path

# TEST_STUDY_DIR = Path("tests", "data", "test_study")
# PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_obs.parquet")
# SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_fcast.parquet")
# CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
# GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")

# TODO: REMOVE. Katie's Hilary event
TEST_STUDY_DIR = Path("/mnt/data/ciroh/2023_hilary")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "forcing_analysis_assim_extend", "*.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "forcing_medium_range", "*.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "huc10_huc10_crosswalk.conus.parquet")
ATTRIBUTES_FILEPATH = Path(TEST_STUDY_DIR, "attrs/*.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR,  "huc10_geometry.conus.parquet")
# DATABASE_FILEPATH = Path(TEST_STUDY_DIR, "hilary_post_event.db")


def test_metric_query_str():
    query_str = tqu.get_metrics(
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
    assert isinstance(query_str, str)


def test_metric_query_df():

    query_df = tqu.get_metrics(
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

    query_df = tqu.get_metrics(
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

    query_df = tqu.get_metrics(
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
        tqu.get_metrics(
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
        tqu.get_metrics(
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
        "bias",
        "nash_sutcliffe_efficiency",
        "kling_gupta_efficiency",
        "mean_error",
        "mean_squared_error",
        "root_mean_squared_error",
    ]
    group_by = ["primary_location_id"]
    query_df = tqu.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
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
    include_metrics = [
        "primary_max_value_time",
        "secondary_max_value_time",
        "max_value_timedelta"
    ]
    group_by = ["primary_location_id", "reference_time"]
    query_df = tqu.get_metrics(
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


def test_metric_query_df_all():
    group_by = ["primary_location_id", "reference_time"]
    query_df = tqu.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=group_by,
        order_by=["primary_location_id", "reference_time"],
        include_metrics="all",
        return_query=False,
        include_geometry=False,
        deduplicate_primary=False
    )
    print(query_df)
    # assert len(query_df) == 9
    # assert len(query_df.columns) == len(group_by) + 22
    # assert isinstance(query_df, pd.DataFrame)


def test_metric_query_value_time_filter():
    group_by = ["primary_location_id", "reference_time"]
    query_df = tqu.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
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
                "value": f"{'2022-01-01 13:00:00'}"
            },
            {
                "column": "reference_time",
                "operator": ">=",
                "value": f"{'2022-01-01 02:00:00'}"
            }
        ],
    )
    # print(query_df)
    assert len(query_df) == 3
    assert query_df["primary_count"].iloc[0] == 13
    assert isinstance(query_df, pd.DataFrame)


if __name__ == "__main__":

    import time
    t1 = time.time()


    # test_metric_query_str()
    # test_metric_query_df()
    # test_metric_query_gdf()
    # test_metric_query_gdf_2()
    # test_metric_query_gdf_no_geom()
    # test_metric_query_gdf_missing_group_by()
    # test_metric_query_df_2()
    # test_metric_query_df_time_metrics()
    test_metric_query_df_all()
    # test_metric_query_value_time_filter()

    print(f"Elapsed: {time.time() - t1:.2f} s")
    pass
