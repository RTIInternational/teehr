import pandas as pd
import geopandas as gpd
import numpy as np
# from pydantic import ValidationError
import teehr.queries.pandas as tqk
import teehr.queries.duckdb as tqd
from pathlib import Path

TEST_STUDY_DIR = Path("tests", "data", "test_study")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_obs.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_fcast.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")


def test_metric_compare_1():
    include_metrics = [
        "secondary_count",
        "primary_count",
        # "secondary_minimum",
        # "primary_minimum",
        # "secondary_maximum",
        # "primary_maximum",
        # "secondary_average",
        # "primary_average",
        # "secondary_sum",
        # "primary_sum",
        # "secondary_variance",
        # "primary_variance",
        # "max_value_delta",
        "bias",
        # "nash_sutcliffe_efficiency",
        # "kling_gupta_efficiency",
        # "mean_squared_error",
        "root_mean_squared_error"
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
        "return_query": True
    }
    # pandas_df = tqk.get_metrics(**args)
    # print(pandas_df)
    duckdb_df = tqd.get_metrics2(**args)
    print(duckdb_df)

    # for m in include_metrics:
    #     print(m)
    #     duckdb_np = duckdb_df[m].to_numpy()
    #     pandas_np = pandas_df[m].to_numpy()
    #     print(duckdb_np)
    #     print(pandas_np)
    #     assert np.allclose(duckdb_np, pandas_np)


if __name__ == "__main__":
    test_metric_compare_1()
    pass

