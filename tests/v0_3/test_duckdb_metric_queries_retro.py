"""Test duckdb metric queries."""
import pandas as pd
import geopandas as gpd
import pytest
from pydantic import ValidationError
import teehr_v0_3.queries.duckdb as tqu
from pathlib import Path

TEST_STUDY_DIR = Path("tests", "data", "retro")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "primary_obs.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "secondary_sim.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "xwalk.parquet")


def test_metric_query_df():
    """Test return metric query as a dataframe."""
    query_df = tqu.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        group_by=["primary_location_id", "configuration"],
        order_by=["primary_location_id", "configuration"],
        include_metrics="all",
        return_query=False,
        remove_duplicates=True
    )
    # print(query_df)
    assert len(query_df) == 6
    assert len(query_df.columns) == 34
    assert isinstance(query_df, pd.DataFrame)


if __name__ == "__main__":

    test_metric_query_df()
    pass
