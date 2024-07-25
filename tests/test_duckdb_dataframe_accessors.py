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


def test_timeseries_query_accessors():
    """Test the pandas accessors with the timeseries methods."""
    tds = DuckDBJoinedParquet(
        JOINED_PARQUET_FILEPATH,
        GEOMETRY_FILEPATH
    )
    # Get joined timeseries
    order_by = ["primary_location_id"]

    df = tds.get_joined_timeseries(
        order_by=order_by,
        return_query=False,
        include_geometry=True,
    )
    # Modify the timeseries data to make the plots more interesting.
    mask = df.reference_time == '2022-01-01 00:00:00'
    df.loc[mask, "secondary_value"] = df["secondary_value"][mask] * 0.7

    output_file = Path("tests", "data", "temp", "test_plot.html")
    output_file.unlink(missing_ok=True)  # Remove the file if it exists.

    # Create a plot using the teehr accessor.
    df.teehr.plot_forecasts(
        primary_location_id="gage-A",
        variable_name="streamflow",
        measurement_unit="m^3/s",
        configuration="test_short",
        output_filepath=output_file,
    )

    # TODO: How to test visualizations?
    assert output_file.is_file()

    output_file.unlink(missing_ok=True)  # Remove the file if it exists


if __name__ == "__main__":
    test_metrics_query_accessors()
    test_timeseries_query_accessors()
