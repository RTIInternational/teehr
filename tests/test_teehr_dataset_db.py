from pathlib import Path
import time
import numpy as np

from teehr.database.teehr_dataset import TEEHRDatasetDB

# Test data
TEST_STUDY_DIR = Path("tests/data/test_study")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "test_short_obs.parquet")
SECONDARY_FILEPATH = Path(
    TEST_STUDY_DIR, "timeseries", "test_short_fcast.parquet"
)
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
ATTRIBUTES_FILEPATH = Path(TEST_STUDY_DIR, "geo", "test_attr2.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")
DATABASE_FILEPATH = Path(TEST_STUDY_DIR, "temp_test.db")


def test_unique_field_values():
    tds = TEEHRDatasetDB(DATABASE_FILEPATH)
    df = tds.get_unique_field_values("primary_location_id")
    assert sorted(df["unique_primary_location_id_values"].tolist()) == [
        "gage-A",
        "gage-B",
        "gage-C",
    ]
    pass


def test_metrics_query():
    tds = TEEHRDatasetDB(DATABASE_FILEPATH)

    # Insert geometry
    tds.insert_geometry(GEOMETRY_FILEPATH)

    # Get metrics
    order_by = ["lead_time", "primary_location_id"]
    group_by = ["lead_time", "primary_location_id"]
    filters = [
        {
            "column": "primary_location_id",
            "operator": "=",
            "value": "gage-A",
        },
        {
            "column": "reference_time",
            "operator": "=",
            "value": "2022-01-01 00:00:00",
        },
        {"column": "lead_time", "operator": "<=", "value": "10 hours"},
    ]

    df = tds.get_metrics(
        group_by=group_by,
        order_by=order_by,
        include_metrics="all",
        filters=filters,
        include_geometry=True,
    )
    assert df.index.size == 11


def test_insert_joined_timeseries():
    tds = TEEHRDatasetDB(DATABASE_FILEPATH)

    # Perform the join and insert into duckdb database
    tds.insert_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        drop_added_fields=True,
    )

    df = tds.query("SELECT * FROM joined_timeseries LIMIT 10;", format="df")
    np.testing.assert_approx_equal(
        df.absolute_difference.sum(), 12.34, significant=4
    )
    pass


def test_describe_inputs():
    tds = TEEHRDatasetDB(DATABASE_FILEPATH)
    # Perform the join and insert into duckdb database
    tds.insert_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        drop_added_fields=True,
    )
    df = tds.describe_inputs(PRIMARY_FILEPATH, SECONDARY_FILEPATH)

    pass


def test_calculate_field():
    tds = TEEHRDatasetDB(DATABASE_FILEPATH)

    # Perform the join and insert into duckdb database
    tds.insert_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        drop_added_fields=True,
    )
    # Add attributes
    tds.join_attributes(ATTRIBUTES_FILEPATH)

    # Calculate and add a field based on some user-defined function (UDF).
    def my_user_function(arg1: float, arg2: str) -> float:
        """Function arguments are fields in joined_timeseries, and
        should have the same data type.
        Note: In the data model, attribute values are always str type"""
        return float(arg1) / float(arg2)

    parameter_names = ["primary_value", "drainage_area_sq_km"]
    new_field_name = "primary_normalized_discharge"
    new_field_type = "FLOAT"
    tds.calculate_field(
        new_field_name=new_field_name,
        new_field_type=new_field_type,
        parameter_names=parameter_names,
        user_defined_function=my_user_function,
    )


if __name__ == "__main__":
    test_insert_joined_timeseries()
    test_unique_field_values()
    test_describe_inputs()
    test_calculate_field()
    test_unique_field_values()
    test_metrics_query()
