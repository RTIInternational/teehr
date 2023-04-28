import numpy as np
import teehr.queries.pandas as tqk
import teehr.queries.duckdb as tqu
from pathlib import Path

TEST_STUDY_DIR = Path("tests", "data", "test_study")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_obs.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_fcast.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")


def test_metric_compare_1():
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
        "order_by": ["primary_location_id"],
        "include_metrics": include_metrics,
        "return_query": False
    }
    pandas_df = tqk.get_metrics(**args)
    duckdb_df = tqu.get_metrics(**args)

    for m in include_metrics:
        # print(m)
        duckdb_np = duckdb_df[m].to_numpy()
        pandas_np = pandas_df[m].to_numpy()
        assert np.allclose(duckdb_np, pandas_np)


def test_metric_compare_time_metrics():
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
        "order_by": ["primary_location_id"],
        "include_metrics": include_metrics,
        "return_query": False
    }
    pandas_df = tqk.get_metrics(**args)
    duckdb_df = tqu.get_metrics(**args)

    for m in include_metrics:
        duckdb_np = duckdb_df[m].astype('int64').to_numpy()
        pandas_np = pandas_df[m].astype('int64').to_numpy()
        assert np.allclose(duckdb_np, pandas_np)


if __name__ == "__main__":
    test_metric_compare_1()
    test_metric_compare_time_metrics()
    pass
