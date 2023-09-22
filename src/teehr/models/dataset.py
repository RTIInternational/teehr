"""Defines the TEEHR dataset class and pre-processing methods"""
from typing import Union, List, Optional
from pathlib import Path
import pathlib

from pydantic import BaseModel
import duckdb

import teehr.queries.duckdb as tqu
from teehr.models.queries import JoinedFilterFieldEnum, JoinedFilter


TEST_STUDY_DIR = Path("tests", "data", "test_study")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_obs.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_fcast.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")
DATABASE_FILEPATH = Path(TEST_STUDY_DIR, "test_study.db")


class TeehrDataset(BaseModel):

    primary_filepath: Union[str, Path]
    secondary_filepath: Union[str, Path]
    crosswalk_filepath: Union[str, Path]
    database_filepath: Union[str, Path]
    order_by: List[JoinedFilterFieldEnum]
    filters: Optional[List[JoinedFilter]] = []

    def __init__(self, **kwargs):
        """Creates instance of a TeehrDataset class and
        initializes study area database"""
        super().__init__(**kwargs)
        # Ensure the database filepath is a str for duckdb
        if isinstance(self.database_filepath, pathlib.PosixPath):
            self.database_filepath = self.database_filepath.as_posix()

        # Create the persistent study database and table here
        create_table = """
            CREATE TABLE IF NOT EXISTS timeseries(
                reference_time DATETIME,
                value_time DATETIME,
                location_id VARCHAR,
                value FLOAT,
                configuration VARCHAR,
                measurement_unit VARCHAR,
                variable_name VARCHAR,
                primary_value FLOAT,
                primary_location_id VARCHAR,
                lead_time INTERVAL
                );"""

        with duckdb.connect(self.database_filepath) as con:
            con.sql(create_table)

    def join(self):
        """Join primary and secondary timeseries and insert
        into the database"""
        with duckdb.connect(self.database_filepath) as con:
            tqu.join_and_insert_timeseries(
                primary_filepath=self.primary_filepath,
                secondary_filepath=self.secondary_filepath,
                crosswalk_filepath=self.crosswalk_filepath,
                order_by=self.order_by,
                con=con,
                filters=self.filters,
                return_query=False,
            )

    def validate(self):
        """Check for duplicates, keep duplicate validtimes having
        most recent reference time, provide summary report of data, ex:
        - length of joined record
        - number of entries
        - duplicate values dropped
        - visualization?
        - some summary comparison stats (primary vs. secondary)"""
        calculate_stats = """

                );"""
        with duckdb.connect(self.database_filepath) as con:
            con.sql(calculate_stats)


    def add_field(self, field: str, some_function):
        """Provide a function-building utility?
        Potentially useful:
        - con.create_function(): creates duckdb func from python func"""

        print(f"Adding field: {field} by applying function: \
            {some_function} to existing table fields")

    def get_metrics(self):
        print("Performing MetricQuery here")


if __name__ == "__main__":

    data = {"primary_filepath": PRIMARY_FILEPATH,
            "secondary_filepath": SECONDARY_FILEPATH,
            "crosswalk_filepath": CROSSWALK_FILEPATH,
            "database_filepath": DATABASE_FILEPATH,
            "order_by": ["primary_location_id", "reference_time"]}

    # order_by=["primary_location_id", "reference_time"]

    tds = TeehrDataset.parse_obj(data)

    tds.join()

    tds.validate()

    pass
