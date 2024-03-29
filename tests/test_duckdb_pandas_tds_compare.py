"""Tests for comparing pandas and duckdb query methods."""
from pathlib import Path

import numpy as np
import pandas as pd

import teehr.queries.pandas as tqk
import teehr.queries.duckdb as tqu
from teehr.database.teehr_dataset import TEEHRDatasetDB

TEST_STUDY_DIR = Path("tests", "data", "test_study")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*short_obs.parquet")
PRIMARY_FILEPATH_DUPS = Path(TEST_STUDY_DIR, "timeseries", "*dup_obs.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_fcast.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")
DATABASE_FILEPATH = Path("tests", "data", "temp", "temp_test.db")

if DATABASE_FILEPATH.is_file():
    DATABASE_FILEPATH.unlink()

TDS = TEEHRDatasetDB(DATABASE_FILEPATH)

# Perform the join and insert into duckdb database
TDS.insert_joined_timeseries(
    primary_filepath=PRIMARY_FILEPATH,
    secondary_filepath=SECONDARY_FILEPATH,
    crosswalk_filepath=CROSSWALK_FILEPATH,
    drop_added_fields=True,
)


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
        "mean_error",
        "mean_squared_error",
        "root_mean_squared_error",
        "relative_bias",
        "multiplicative_bias",
        "mean_absolute_relative_error",
        "pearson_correlation",
        "r_squared"
    ]
    group_by = [
        "primary_location_id",
        "reference_time"
    ]
    args = {
        "primary_filepath": PRIMARY_FILEPATH,
        "secondary_filepath": SECONDARY_FILEPATH,
        "crosswalk_filepath": CROSSWALK_FILEPATH,
        "geometry_filepath": GEOMETRY_FILEPATH,
        "group_by": group_by,
        "order_by": ["primary_location_id", "reference_time"],
        "include_metrics": include_metrics,
        "return_query": False
    }

    order_by = ["primary_location_id", "reference_time"]
    tds_df = TDS.get_metrics(group_by=group_by,
                             order_by=order_by,
                             include_metrics=include_metrics)

    pandas_df = tqk.get_metrics(**args)

    args["primary_filepath"] = PRIMARY_FILEPATH_DUPS
    args["remove_duplicates"] = True
    duckdb_df = tqu.get_metrics(**args)

    pandas_df["primary_count"] = pandas_df.primary_count.astype(int)
    pandas_df["secondary_count"] = pandas_df.secondary_count.astype(int)

    for m in include_metrics:
        # print(m)
        duckdb_np = duckdb_df[m].to_numpy()
        pandas_np = pandas_df[m].to_numpy()
        tds_np = tds_df[m].to_numpy()
        assert np.allclose(duckdb_np, pandas_np)
        assert np.allclose(tds_np, pandas_np)
        assert np.allclose(tds_np, duckdb_np)


def test_metric_compare_time_metrics():
    """Test metric compare time metrics."""
    include_metrics = [
        "primary_max_value_time",
        "secondary_max_value_time",
        "max_value_timedelta",
    ]
    group_by = [
        "primary_location_id",
        "reference_time"
    ]
    args = {
        "primary_filepath": PRIMARY_FILEPATH,
        "secondary_filepath": SECONDARY_FILEPATH,
        "crosswalk_filepath": CROSSWALK_FILEPATH,
        "geometry_filepath": GEOMETRY_FILEPATH,
        "group_by": group_by,
        "order_by": ["primary_location_id", "reference_time"],
        "include_metrics": include_metrics,
        "return_query": False
    }

    order_by = ["primary_location_id", "reference_time"]
    tds_df = TDS.get_metrics(group_by=group_by,
                             order_by=order_by,
                             include_metrics=include_metrics)

    pandas_df = tqk.get_metrics(**args)
    duckdb_df = tqu.get_metrics(**args)

    diff_df1 = pandas_df[include_metrics].compare(duckdb_df[include_metrics])
    assert diff_df1.index.size == 0
    diff_df2 = pandas_df[include_metrics].compare(tds_df[include_metrics])
    assert diff_df2.index.size == 0


def test_primary_timeseries_compare():
    """Test primary timeseries compare."""
    query_df = tqu.get_timeseries(
        timeseries_filepath=PRIMARY_FILEPATH,
        order_by=["location_id"],
        return_query=False,
    )

    tds_df = TDS.get_timeseries(
        order_by=["primary_location_id"],
        timeseries_name="primary",
    )

    # print(query_df)
    assert len(query_df) == 26 * 3
    assert len(query_df) == len(tds_df)


def test_secondary_timeseries_compare():
    """Test secondary timeseries compare."""
    query_df = tqu.get_timeseries(
        timeseries_filepath=SECONDARY_FILEPATH,
        order_by=["location_id"],
        return_query=False,
    )
    tds_df = TDS.get_timeseries(
        order_by=["secondary_location_id"],
        timeseries_name="secondary"
    )
    assert len(query_df) == 24 * 3 * 3
    assert len(query_df) == len(tds_df)


def test_primary_timeseries_char_compare():
    """Test primary timeseries char compare."""
    query_df = tqu.get_timeseries_chars(
        timeseries_filepath=PRIMARY_FILEPATH,
        group_by=["location_id"],
        order_by=["location_id"],
        return_query=False,
    )

    tds_df = TDS.get_timeseries_chars(
        order_by=["primary_location_id"],
        group_by=["primary_location_id"],
        timeseries_name="primary",
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
    # Check if contents are the same
    diff_df = df.compare(query_df)
    assert diff_df.index.size == 0

    for i, col in enumerate(df.columns):
        # print(m)
        if (col == "location_id") or (col == "max_value_time"):
            assert (df[col] == tds_df.iloc[:, i]).all()
            continue
        df_np = df[col].to_numpy()
        tds_np = tds_df.iloc[:, i].to_numpy()
        assert np.allclose(tds_np, df_np)


def test_secondary_timeseries_char_compare():
    """Test secondary timeseries char compare."""
    query_df = tqu.get_timeseries_chars(
        timeseries_filepath=SECONDARY_FILEPATH,
        group_by=["location_id"],
        order_by=["location_id"],
        return_query=False,
    )

    tds_df = TDS.get_timeseries_chars(
        order_by=["secondary_location_id"],
        group_by=["secondary_location_id"],
        timeseries_name="secondary"
    )

    # Check if contents are the same
    for i, col in enumerate(query_df.columns):
        # print(m)
        if (col == "location_id") or (col == "max_value_time"):
            assert (query_df[col] == tds_df.iloc[:, i]).all()
            continue
        df_np = query_df[col].to_numpy()
        tds_np = tds_df.iloc[:, i].to_numpy()
        assert np.allclose(tds_np, df_np)


if __name__ == "__main__":
    test_metric_compare_1()
    test_metric_compare_time_metrics()
    test_primary_timeseries_compare()
    test_secondary_timeseries_compare()
    test_primary_timeseries_char_compare()
    test_secondary_timeseries_char_compare()
    pass
