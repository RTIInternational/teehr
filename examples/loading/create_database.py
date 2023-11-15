"""
This script provides and example of how to create a TEEHR datyabase
and insert joined timeseries, append attributes, and add user
defined fields.
"""
from pathlib import Path
from teehr.database.teehr_dataset import TEEHRDatasetDB
import time
import datetime


TEST_STUDY_DIR = Path("/home/matt/temp/huc1802_retro")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "usgs.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "nwm2*.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "usgs_nwm2*_crosswalk.parquet") # noqa
ATTRIBUTES_FILEPATH = Path(TEST_STUDY_DIR, "geo", "usgs_attr_*.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR,  "geo", "usgs_geometry.parquet")
DATABASE_FILEPATH = Path(TEST_STUDY_DIR, "huc1802_retro.db")

# Test data
# TEST_STUDY_DIR = Path("tests/data/test_study")
# PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "test_short_obs.parquet") # noqa
# SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "test_short_fcast.parquet") # noqa
# CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
# ATTRIBUTES_FILEPATH = Path(TEST_STUDY_DIR, "geo", "test_attr2.parquet")
# GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR,  "geo", "gages.parquet")
# DATABASE_FILEPATH = Path(TEST_STUDY_DIR, "temp_test.db")


def describe_inputs():
    tds = TEEHRDatasetDB(DATABASE_FILEPATH)

    # Check the parquet files and report some stats to the user (WIP)
    df = tds.describe_inputs(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH
    )

    print(df)


def create_db_add_timeseries():

    tds = TEEHRDatasetDB(DATABASE_FILEPATH)

    # Perform the join and insert into duckdb database
    # NOTE: Right now this will re-join and overwrite
    print("Creating joined table")
    tds.insert_joined_timeseries(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH
        )
    tds.insert_geometry(geometry_filepath=GEOMETRY_FILEPATH)


def add_attributes():
    tds = TEEHRDatasetDB(DATABASE_FILEPATH)

    # Join (one or more?) table(s) of attributes to the timeseries table
    print("Adding attributes")
    tds.join_attributes(ATTRIBUTES_FILEPATH)


def add_fields():

    tds = TEEHRDatasetDB(DATABASE_FILEPATH)

    # Calculate and add a field based on some user-defined function (UDF).
    def test_user_function(arg1: float, arg2: str) -> float:
        """Function arguments are fields in joined_timeseries, and
        should have the same data type.
        Note: In the data model, attribute values are always str type"""
        return float(arg1) / float(arg2)

    parameter_names = ["primary_value", "upstream_area_km2"]
    new_field_name = "primary_normalized_discharge"
    new_field_type = "FLOAT"
    tds.calculate_field(new_field_name=new_field_name,
                        new_field_type=new_field_type,
                        parameter_names=parameter_names,
                        user_defined_function=test_user_function)

    # Calculate and add a field based on some user-defined function (UDF).
    def add_month_field(arg1: datetime.datetime) -> int:
        """Function arguments are fields in joined_timeseries, and
        should have the same data type.
        Note: In the data model, attribute values are always str type"""
        return arg1.month

    parameter_names = ["value_time"]
    new_field_name = "month"
    new_field_type = "INTEGER"
    tds.calculate_field(new_field_name=new_field_name,
                        new_field_type=new_field_type,
                        parameter_names=parameter_names,
                        user_defined_function=add_month_field)

    # Calculate and add a field based on some user-defined function (UDF).
    def exceed_2yr_recurrence(arg1: float, arg2: float) -> bool:
        """Function arguments are fields in joined_timeseries, and
        should have the same data type.
        Note: In the data model, attribute values are always str type"""
        return float(arg1) > float(arg2)

    parameter_names = ["primary_value", "retro_2yr_recurrence_flow_cms"]
    new_field_name = "exceed_2yr_recurrence"
    new_field_type = "BOOLEAN"
    tds.calculate_field(new_field_name=new_field_name,
                        new_field_type=new_field_type,
                        parameter_names=parameter_names,
                        user_defined_function=exceed_2yr_recurrence)
    pass


def run_metrics_query():

    tds = TEEHRDatasetDB(DATABASE_FILEPATH)
    # schema_df = tds.get_joined_timeseries_schema()
    # print(schema_df[["column_name", "column_type"]])

    # Get metrics
    group_by = ["primary_location_id", "configuration"]
    order_by = ["primary_location_id"]
    include_metrics = ["mean_error", "bias"]
    filters = [
        # {
        #     "column": "primary_location_id",
        #     "operator": "=",
        #     "value": "usgs-11337080"
        # },
        # {
        #     "column": "month",
        #     "operator": "=",
        #     "value": 1
        # },
        # {
        #     "column": "upstream_area_km2",
        #     "operator": ">",
        #     "value": 1000
        # },
        # {
        #     "column": "exceed_2yr_recurrence",
        #     "operator": "=",
        #     "value": True
        # }
    ]

    t1 = time.time()
    df1 = tds.get_metrics(
        group_by=group_by,
        order_by=order_by,
        filters=filters,
        include_metrics=include_metrics,
        include_geometry=True,
        # return_query=True
    )
    print(df1)
    print(f"Database query: {(time.time() - t1):.2f} secs")

    pass


def describe_database():
    tds = TEEHRDatasetDB(DATABASE_FILEPATH)
    df = tds.get_joined_timeseries_schema()
    print(df)


def run_raw_query():

    tds = TEEHRDatasetDB(DATABASE_FILEPATH)
    query = """
        WITH joined as (
            SELECT
                *
            FROM joined_timeseries
        )
        , metrics AS (
            SELECT
                joined.primary_location_id,joined.configuration
                , sum(primary_value - secondary_value)/count(*) as bias
                , sum(absolute_difference)/count(*) as mean_error
            FROM
                joined
            GROUP BY
                joined.primary_location_id,joined.configuration
        )
        SELECT
            metrics.*
            ,gf.geometry as geometry
        FROM metrics
        JOIN geometry gf
            on primary_location_id = gf.id
        ORDER BY
            metrics.primary_location_id
    ;
    ;"""
    # query = f"""
    #     COPY (
    #         SELECT * FROM joined_timeseries
    #     )
    #     TO '{str(Path(TEST_STUDY_DIR, "huc1802_retro.parquet"))}' (
    #         FORMAT 'parquet', COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000
    #     )
    # ;"""
    df = tds.query(query, format="df")
    print(df)


if __name__ == "__main__":
    # create_db_add_timeseries()
    # describe_inputs()
    # describe_database()
    # add_attributes()
    # describe_database()
    # add_fields()
    # describe_database()
    # run_metrics_query()
    # run_raw_query()
    pass
