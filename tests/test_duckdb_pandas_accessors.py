"""Tests for the teehr accessors extending pandas objects in DuckDB classes."""
from pathlib import Path

from teehr.classes.duckdb_joined_parquet import DuckDBJoinedParquet


# Test data
TEST_STUDY_DIR = Path("tests", "data", "test_study")
JOINED_PARQUET_FILEPATH = Path(
    TEST_STUDY_DIR, "timeseries", "test_joined_timeseries_*.parquet"
)
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")


def test_metrics_query_accessors():
    """Test the pandas accessors with the get_metrics method."""
    tds = DuckDBJoinedParquet(
        JOINED_PARQUET_FILEPATH,
        GEOMETRY_FILEPATH
    )

    # Get some metrics.
    group_by = ["primary_location_id"]
    order_by = ["primary_location_id"]
    include_metrics = "all"

    df = tds.get_metrics(
        group_by=group_by,
        order_by=order_by,
        include_metrics=include_metrics,
        include_geometry=True,
    )

    # Call the summarize_metrics method using the teehr accessor.
    summary_df = df.teehr.summarize_metrics(
        group_by=group_by,
        percentiles=[0.25, 0.5, 0.75]
    )

    assert summary_df.index.size == 32

    pass


if __name__ == "__main__":
    test_metrics_query_accessors()
