"""Test duckdb metric queries."""
import numpy as np
import teehr_v0_3.queries.duckdb as tqd
import teehr_v0_3.queries.pandas as tqp
from pathlib import Path

TEST_STUDY_DIR = Path("tests", "v0_3", "data", "test_study")
TEST_RETRO_DIR = Path("tests", "v0_3", "data", "retro")


def test_spearmans_corr_1():
    """Tests the newly added spearmans_corr."""
    PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries/test_short_obs.parquet")
    SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries/test_short_fcast.parquet")
    CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo/crosswalk.parquet")

    include_metrics = ['spearman_correlation']
    group_by = [
        "primary_location_id",
        "configuration",
        "reference_time"
    ]

    pandas_df = tqp.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        include_metrics=include_metrics,
        group_by=group_by,
        order_by=group_by,
    )
    # print(pandas_df)

    duckdb_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        include_metrics=include_metrics,
        group_by=group_by,
        order_by=group_by,
    )
    # print(duckdb_df)

    for m in include_metrics:
        duckdb_np = duckdb_df[m].to_numpy()
        pandas_np = pandas_df[m].to_numpy()
        assert np.allclose(duckdb_np, pandas_np)


def test_spearmans_corr_2():
    """Tests the newly added spearmans_corr."""
    PRIMARY_FILEPATH = Path(TEST_RETRO_DIR, "primary_obs.parquet")
    SECONDARY_FILEPATH = Path(TEST_RETRO_DIR, "secondary_sim.parquet")
    CROSSWALK_FILEPATH = Path(TEST_RETRO_DIR, "xwalk.parquet")

    include_metrics = ['spearman_correlation']
    group_by = [
        "primary_location_id",
        "configuration"
    ]

    pandas_df = tqp.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        include_metrics=include_metrics,
        group_by=group_by,
        order_by=group_by,
    )
    # print(pandas_df)

    duckdb_df = tqd.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        include_metrics=include_metrics,
        group_by=group_by,
        order_by=group_by,
    )
    # print(duckdb_df)

    for m in include_metrics:
        duckdb_np = duckdb_df[m].to_numpy()
        pandas_np = pandas_df[m].to_numpy()
        assert np.allclose(duckdb_np, pandas_np)


if __name__ == "__main__":
    # run_sql()
    test_spearmans_corr_1()
    test_spearmans_corr_2()
    pass
