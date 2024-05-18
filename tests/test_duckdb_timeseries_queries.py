"""Tests for duckdb timeseries queries."""
import pandas as pd
import geopandas as gpd
import teehr.queries.duckdb as tqd
from pathlib import Path
from datetime import datetime

TEST_STUDY_DIR = Path("tests", "data", "test_study")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*short_obs.parquet")
PRIMARY_FILEPATH_DUPS = Path(TEST_STUDY_DIR, "timeseries", "*dup_obs.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_fcast.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")


def test_joined_timeseries_query_df():
    """Test joined timeseries query as a dataframe."""
    query_df = tqd.get_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH_DUPS,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        order_by=["primary_location_id", "lead_time"],
        return_query=False,
        remove_duplicates=True
    )

    # print(query_df.info())
    assert len(query_df) == 3 * 3 * 24
    assert isinstance(query_df, pd.DataFrame)


def test_joined_timeseries_with_dups_query_df():
    """Test joined timeseries query with duplicates as a dataframe."""
    query_df = tqd.get_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH_DUPS,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        order_by=["primary_location_id", "lead_time"],
        return_query=False,
        remove_duplicates=False
    )

    # print(query_df.info())
    assert len(query_df) == 231
    assert isinstance(query_df, pd.DataFrame)


def test_joined_timeseries_query_gdf():
    """Test joined timeseries query as a geodataframe."""
    query_df = tqd.get_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH_DUPS,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        order_by=["primary_location_id", "lead_time"],
        return_query=False,
        include_geometry=True,
    )

    # print(query_df.info())
    assert len(query_df) == 3 * 3 * 24
    assert isinstance(query_df, gpd.GeoDataFrame)


def test_joined_timeseries_query_df_filter():
    """Test joined timeseries with a filter query as a dataframe."""
    query_df = tqd.get_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH_DUPS,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        order_by=["primary_location_id", "lead_time"],
        return_query=False,
        filters=[
            {
                "column": "reference_time",
                "operator": "=",
                "value": "2022-01-01 00:00:00",
            },
            {
                "column": "primary_location_id",
                "operator": "=",
                "value": "gage-A",
            },
        ],
    )

    # print(query_df.info())
    assert len(query_df) == 24
    assert isinstance(query_df, pd.DataFrame)
    # Make sure the correct duplicate value was selected
    query_df.set_index("value_time", inplace=True)
    assert query_df.loc["2022-01-01T10:00:00"].primary_value == 0.9


def test_timeseries_query_df():
    """Test get timeseries query as a dataframe."""
    query_df = tqd.get_timeseries(
        timeseries_filepath=PRIMARY_FILEPATH,
        order_by=["location_id"],
        return_query=False,
    )
    # print(query_df)
    assert len(query_df) == 26 * 3


def test_timeseries_query_df2():
    """Test get secondary timeseries query as a dataframe."""
    query_df = tqd.get_timeseries(
        timeseries_filepath=SECONDARY_FILEPATH,
        order_by=["location_id"],
        return_query=False,
    )
    assert len(query_df) == 24 * 3 * 3


def test_timeseries_query_one_site_df():
    """Test get timeseries query as a dataframe one site."""
    query_df = tqd.get_timeseries(
        timeseries_filepath=PRIMARY_FILEPATH,
        order_by=["location_id"],
        filters=[
            {"column": "location_id", "operator": "=", "value": "gage-C"}
        ],
        return_query=False,
    )
    assert len(query_df) == 26


def test_timeseries_query_one_site_one_ref_df():
    """Test get timeseries query as a dataframe one site and ref time."""
    query_df = tqd.get_timeseries(
        timeseries_filepath=SECONDARY_FILEPATH,
        order_by=["value_time"],
        filters=[
            {"column": "location_id", "operator": "=", "value": "fcst-1"},
            {
                "column": "reference_time",
                "operator": "=",
                "value": datetime(2022, 1, 1),
            },
        ],
        return_query=False,
    )
    assert len(query_df) == 24


def test_timeseries_char_query_df():
    """Test timeseries char query as a dataframe."""
    query_df = tqd.get_timeseries_chars(
        timeseries_filepath=PRIMARY_FILEPATH,
        group_by=["location_id"],
        order_by=["location_id"],
        return_query=False,
    )
    df = pd.DataFrame(
        {
            "location_id": {0: "gage-A", 1: "gage-B", 2: "gage-C"},
            "count": {0: 26, 1: 26, 2: 26},
            "min": {0: 0.1, 1: 10.1, 2: 0.0},
            "max": {0: 5.0, 1: 15.0, 2: 180.0},
            "average": {
                0: 1.2038461538461542,
                1: 11.203846153846156,
                2: 100.38461538461539,
            },
            "sum": {0: 31.300000000000008, 1: 291.30000000000007, 2: 2610.0},
            "variance": {
                0: 1.9788313609467447,
                1: 1.9788313609467456,
                2: 2726.7751479289923,
            },
            "max_value_time": {
                0: pd.Timestamp("2022-01-01 15:00:00"),
                1: pd.Timestamp("2022-01-01 15:00:00"),
                2: pd.Timestamp("2022-01-01 06:00:00"),
            },
        }
    )
    diff_df = df.compare(query_df)
    assert diff_df.index.size == 0
    # assert df.equals(query_df)
    # difference in max_value_time dtype (us vs. ns)


def test_timeseries_char_query_df2():
    """Test timeseries char query as a dataframe v2."""
    query_df = tqd.get_timeseries_chars(
        timeseries_filepath=SECONDARY_FILEPATH,
        group_by=["location_id", "reference_time"],
        order_by=["location_id"],
        return_query=False,
    )
    # print(query_df)
    assert len(query_df) == 9


def test_timeseries_char_query_filter_df():
    """Test timeseries char query as a dataframe with filter."""
    query_df = tqd.get_timeseries_chars(
        timeseries_filepath=SECONDARY_FILEPATH,
        group_by=["location_id"],
        order_by=["location_id"],
        return_query=False,
        filters=[
            {"column": "location_id", "operator": "=", "value": "fcst-1"},
            {
                "column": "reference_time",
                "operator": "=",
                "value": datetime(2022, 1, 1),
            },
        ],
    )
    assert len(query_df) == 1


if __name__ == "__main__":
    test_joined_timeseries_query_df()
    test_joined_timeseries_with_dups_query_df()
    test_joined_timeseries_query_gdf()
    test_joined_timeseries_query_df_filter()
    test_timeseries_query_df()
    test_timeseries_query_df2()
    test_timeseries_query_one_site_one_ref_df()
    test_timeseries_char_query_df()
    test_timeseries_char_query_df2()
    test_timeseries_char_query_filter_df()
    pass
