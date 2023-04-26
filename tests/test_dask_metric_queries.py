# import pandas as pd
# import geopandas as gpd
# import pytest
# from pydantic import ValidationError
import teehr.queries.dataframe as tqk
from pathlib import Path

TEST_STUDY_DIR = Path("tests", "data", "test_study")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_obs.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_fcast.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")


def test_metric_query_df():

    query_df = tqk.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        group_by=["primary_location_id", "reference_time"],
        order_by=["primary_location_id"],
        return_query=False,
    )
    print(query_df)
    # assert len(query_df) == 3
    # assert isinstance(query_df, pd.DataFrame)


if __name__ == "__main__":
    test_metric_query_df()
    pass
