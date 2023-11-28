from pathlib import Path
import numpy as np
import pandas as pd

from teehr.database.teehr_dataset import TEEHRDatasetDB

# Test data
TEST_STUDY_DIR = Path("tests", "data", "test_study")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_obs.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_fcast.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")
ATTRIBUTES_FILEPATH = Path(TEST_STUDY_DIR, "geo", "test_attr.parquet")
DATABASE_FILEPATH = Path(TEST_STUDY_DIR, "temp_test.db")


def test_insert_joined_timeseries():

    if DATABASE_FILEPATH.is_file():
        DATABASE_FILEPATH.unlink()

    tds = TEEHRDatasetDB(DATABASE_FILEPATH)

    # Perform the join and insert into duckdb database
    tds.insert_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        drop_added_fields=True,
    )

    tds.insert_geometry(GEOMETRY_FILEPATH)

    df = tds.query("SELECT * FROM joined_timeseries", format="df")
    np.testing.assert_approx_equal(
        df.absolute_difference.sum(), 283.7, significant=4
    )
    pass


def test_unique_field_values():
    if DATABASE_FILEPATH.is_file():
        DATABASE_FILEPATH.unlink()

    tds = TEEHRDatasetDB(DATABASE_FILEPATH)

    # Perform the join and insert into duckdb database
    tds.insert_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        drop_added_fields=True,
    )
    df = tds.get_unique_field_values("primary_location_id")
    assert sorted(df["unique_primary_location_id_values"].tolist()) == [
        "gage-A",
        "gage-B",
        "gage-C",
    ]
    pass


def test_metrics_query():
    if DATABASE_FILEPATH.is_file():
        DATABASE_FILEPATH.unlink()

    tds = TEEHRDatasetDB(DATABASE_FILEPATH)

    # Perform the join and insert into duckdb database
    tds.insert_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        drop_added_fields=True,
    )

    # Insert geometry
    tds.insert_geometry(GEOMETRY_FILEPATH)

    # Get metrics
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

    group_by = ["primary_location_id"]
    order_by = ["primary_location_id"]
    include_metrics = "all"
    # filters = []

    df = tds.get_metrics(
        group_by=group_by,
        order_by=order_by,
        include_metrics=include_metrics,
        filters=filters,
        include_geometry=True,
    )
    # print(df)
    assert df.index.size == 1
    assert df.columns.size == 24


def test_describe_inputs():
    if DATABASE_FILEPATH.is_file():
        DATABASE_FILEPATH.unlink()

    tds = TEEHRDatasetDB(DATABASE_FILEPATH)

    # Perform the join and insert into duckdb database
    tds.insert_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        drop_added_fields=True,
    )
    df = tds.describe_inputs(PRIMARY_FILEPATH, SECONDARY_FILEPATH)

    base_df = pd.DataFrame(
        index=[
            "Number of unique location IDs",
            "Total number of rows",
            "Start Date",
            "End Date",
            "Number of duplicate rows",
            "Number of location IDs with duplicate value times",
            "Number of location IDs with missing time steps",
        ],
        data={
            "primary": [
                3,
                78,
                pd.to_datetime("2022-01-01 00:00:00"),
                pd.to_datetime("2022-01-02 01:00:00"),
                0,
                0,
                0,
            ],
            "secondary": [
                3,
                216,
                pd.to_datetime("2022-01-01 00:00:00"),
                pd.to_datetime("2022-01-02 01:00:00"),
                0,
                3,
                0,
            ],
        },
    )

    assert base_df.equals(df)


def test_calculate_field():
    if DATABASE_FILEPATH.is_file():
        DATABASE_FILEPATH.unlink()

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


def test_join_attributes():
    if DATABASE_FILEPATH.is_file():
        DATABASE_FILEPATH.unlink()

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

    df = tds.get_joined_timeseries_schema()

    cols = [
        "reference_time",
        "value_time",
        "secondary_location_id",
        "secondary_value",
        "configuration",
        "measurement_unit",
        "variable_name",
        "primary_value",
        "primary_location_id",
        "lead_time",
        "absolute_difference",
        "drainage_area_sq_km",
        "year_2_discharge_cfs",
    ]
    # Make sure attribute fields have been added
    assert sorted(df.column_name.tolist()) == sorted(cols)

    # Make sure attribute values are correct
    df = tds.query("SELECT * FROM joined_timeseries", format="df")
    np.testing.assert_approx_equal(
        df.year_2_discharge_cfs.astype(float).sum(), 72000.0, significant=6
    )

    np.testing.assert_approx_equal(
        df.drainage_area_sq_km.astype(float).sum(), 7200.0, significant=5
    )

    pass


def test_get_joined_timeseries_schema():
    if DATABASE_FILEPATH.is_file():
        DATABASE_FILEPATH.unlink()

    tds = TEEHRDatasetDB(DATABASE_FILEPATH)

    # Perform the join and insert into duckdb database
    tds.insert_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        drop_added_fields=True,
    )

    df = tds.get_joined_timeseries_schema()

    assert df.index.size == 11
    pass


def test_timeseries_query():
    if DATABASE_FILEPATH.is_file():
        DATABASE_FILEPATH.unlink()

    tds = TEEHRDatasetDB(DATABASE_FILEPATH)

    # Perform the join and insert into duckdb database
    tds.insert_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        drop_added_fields=True,
    )

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

    order_by = ["primary_location_id"]

    df = tds.get_timeseries(
        order_by=order_by,
        filters=filters
    )

    assert df.index.size == 11
    pass


def test_timeseries_char_query():
    if DATABASE_FILEPATH.is_file():
        DATABASE_FILEPATH.unlink()

    tds = TEEHRDatasetDB(DATABASE_FILEPATH)

    # Perform the join and insert into duckdb database
    tds.insert_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        drop_added_fields=True,
    )

    # filters = [
    #     {
    #         "column": "primary_location_id",
    #         "operator": "=",
    #         "value": "gage-A",
    #     },
    #     {
    #         "column": "reference_time",
    #         "operator": "=",
    #         "value": "2022-01-01 00:00:00",
    #     },
    #     {"column": "lead_time", "operator": "<=", "value": "10 hours"},
    # ]
    filters = []
    group_by = ["primary_location_id"]
    order_by = ["primary_location_id"]
    timeseries_name = "secondary"  # "primary, secondary"

    df = tds.get_timeseries_characteristics(
        order_by=order_by,
        group_by=group_by,
        timeseries_name=timeseries_name,
        filters=filters
    )

    assert df.index.size == 3
    pass


if __name__ == "__main__":

    test_insert_joined_timeseries()
    test_join_attributes()
    test_unique_field_values()
    test_describe_inputs()
    test_calculate_field()
    test_unique_field_values()
    test_metrics_query()
    test_get_joined_timeseries_schema()
    test_timeseries_query()
    test_timeseries_char_query()
