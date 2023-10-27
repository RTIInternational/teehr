"""Defines the TEEHR dataset class and pre-processing methods"""
from typing import Union, List, Callable, Tuple, Dict
from pathlib import Path
import time

# import re
import duckdb
import pandas as pd
import geopandas as gpd

import teehr.queries.duckdb_database as tqu_db
# import teehr.queries.duckdb as tqu_original
from teehr.models.queries import MetricEnum


class TEEHRDataset():
    """Create instance of a TeehrDataset class and
    initialize study area database"""
    def __init__(self,
                 database_filepath: Union[str, Path],
                 primary_filepath: Union[str, Path] = None,
                 secondary_filepath: Union[str, Path] = None,
                 crosswalk_filepath: Union[str, Path] = None,
                 geometry_filepath: Union[str, Path] = None):

        self.database_filepath = str(database_filepath)
        self.primary_filepath = primary_filepath
        self.secondary_filepath = secondary_filepath
        self.crosswalk_filepath = crosswalk_filepath
        self.geometry_filepath = geometry_filepath

    def query(self, query: str, format: str = None):
        """Run query against the class's database.

        """
        with duckdb.connect(self.database_filepath) as con:
            con.install_extension("spatial")
            con.load_extension("spatial")
            con.sql("SET memory_limit='15GB';")

            if format == "df":
                return con.sql(query).df()
            elif format == "raw":
                return con.sql(query).show()

            con.sql(query)
            return None

    def _initialize_database_tables(self):
        """Create the persistent study database and empty table(s)"""
        create_timeseries_table = """
            CREATE TABLE IF NOT EXISTS joined_timeseries(
                reference_time DATETIME,
                value_time DATETIME,
                secondary_location_id VARCHAR,
                secondary_value FLOAT,
                configuration VARCHAR,
                measurement_unit VARCHAR,
                variable_name VARCHAR,
                primary_value FLOAT,
                primary_location_id VARCHAR,
                lead_time INTERVAL,
                absolute_difference FLOAT
                );"""

        self.query(create_timeseries_table)

        # Adding a unique index ~ doubles the size of the database on disk.
        # Doing so might start to get us close to PostgreSQL/TimescaleDB sizes?
        # add_index = """
        #     CREATE UNIQUE INDEX unique_ts_idx ON joined_timeseries (
        #         reference_time,
        #         value_time,
        #         configuration,
        #         variable_name,
        #         primary_location_id
        #         );"""
        # self.query(add_index)

        # Also initialize the geometry table (what if multiple geometry types?)
        create_geometry_table = """
            CREATE TABLE IF NOT EXISTS geometry(
                id VARCHAR,
                name VARCHAR,
                geometry BLOB
                );"""
        self.query(create_geometry_table)

    def create_joined_timeseries_table(self,
                                       order_by: List[str],
                                       filters: Union[List[dict], None] = None,
                                       return_query=False,
                                       include_geometry=False):
        """Join primary and secondary timeseries and insert
        into the database. Also loads the geometry file into a
        separate table if the filepath exists
        """

        self._initialize_database_tables()

        with duckdb.connect(self.database_filepath) as con:
            # con.sql("SET memory_limit='15GB';")  # No effect?
            tqu_db.join_and_save_timeseries(
                primary_filepath=self.primary_filepath,
                secondary_filepath=self.secondary_filepath,
                crosswalk_filepath=self.crosswalk_filepath,
                order_by=order_by,
                con=con,
                filters=filters,
                return_query=return_query,
                geometry_filepath=self.geometry_filepath,
                include_geometry=include_geometry
            )

        # Load the geometry data into a separate table if the file exists
        if self.geometry_filepath:
            query = f"""
                INSERT INTO
                    geometry
                SELECT
                    id, name, geometry
                FROM
                    read_parquet('{self.geometry_filepath}')
            ;"""
            self.query(query)

    def get_joined_timeseries_schema(self):
        """Get field names and field data types from joined_timeseries"""

        desc = """DESCRIBE SELECT * FROM joined_timeseries;"""
        df = self.query(desc, format="df")

        return df

    @staticmethod
    def _sanitize_field_name(field_name: str) -> str:
        # I think we will needs this
        # allowed_chars = r"[^a-zA-Z0-9_]"
        # search = re.compile(allowed_chars).search

        # if bool(search(field_name)):
        #     sub = re.compile(allowed_chars).sub
        #     return str(sub("_", field_name))

        return field_name

    @staticmethod
    def _get_unique_attributes(attributes_filepath: str) -> List:
        """Gets a list of unique attributes and attribute units from the
        provided attribute table(s)"""

        query = f"""
            SELECT
                DISTINCT attribute_name, attribute_unit
            FROM
                read_parquet('{attributes_filepath}')
        ;"""
        attr_list = duckdb.sql(query).df().to_dict(orient="records")
        return attr_list

    @staticmethod
    def _pivot_attribute_table(attributes_filepath: str,
                               attr: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation: # noqa
        """Pivots an attribute table selected as a name-unit pair.
        The schema of the returned table consists of a field whose name
        is a combination of attribute_name and attribute_unit, and whose
        values are attribute_value"""

        query = f"""
            WITH attribute AS (
                SELECT *
                FROM
                    read_parquet('{attributes_filepath}')
                WHERE
                    attribute_name = '{attr["attribute_name"]}'
                AND
                    attribute_unit = '{attr["attribute_unit"]}'
            )
            PIVOT
                attribute
            ON
                attribute_name, attribute_unit
            USING
                FIRST(attribute_value)
        ;"""
        attr_pivot = duckdb.sql(query)

        return attr_pivot

    def _add_field_name_to_joined_timeseries(self,
                                             field_name: str,
                                             field_dtype="VARCHAR"):
        """Adds a field name to joined_timeseries"""
        query = f"""
            ALTER TABLE
                joined_timeseries
            ADD IF NOT EXISTS
                {field_name} {field_dtype}
        ;"""
        with duckdb.connect(self.database_filepath) as con:
            con.sql(query)

    def _join_attribute_values(self, field_name: str):
        """Join values of the new attr field on location_id"""
        update_query = f"""
            UPDATE
                joined_timeseries
            SET
                {field_name} = (
            SELECT
                attr_pivot.{field_name}
            FROM
                attr_pivot
            WHERE
                joined_timeseries.primary_location_id = attr_pivot.location_id)
        ;"""
        print(f"Joining {field_name} values to joined_timeseries")
        with duckdb.connect(self.database_filepath) as con:
            con.sql(update_query)

    def join_attributes(self,
                        attributes_filepath: Union[str, Path]):
        """Joins attributes from the provided attribute table(s) to new
        fields in the joined_timeseries table"""
        attr_list = self._get_unique_attributes(str(attributes_filepath))

        for attr in attr_list:
            # Pivot the single attribute
            attr_pivot = self._pivot_attribute_table(str(attributes_filepath), attr) # noqa

            # Add the attr field name to joined_timeseries
            field_name = attr_pivot.columns
            field_name.remove("location_id")
            field_name = self._sanitize_field_name(field_name[0])

            self._add_field_name_to_joined_timeseries(field_name)

            # Join the attribute values to the new field, attr_pivot
            self._join_attribute_values(field_name)

    def describe_inputs(self) -> Tuple[Dict]:
        """Get descriptive stats on primary and secondary
        timeseries parquet files"""
        primary_dict = tqu_db.describe_timeseries( # noqa
            timeseries_filepath=self.primary_filepath)

        secondary_dict = tqu_db.describe_timeseries( # noqa
            timeseries_filepath=self.secondary_filepath)

        # TODO: Format dicts as a report to user (save to csv?)
        # Add more stats
        # Include visualization option here? (plot timeseries)
        return primary_dict, secondary_dict

    def calculate_field(self,
                        new_field_name: str,
                        new_field_type: str,
                        parameter_names: List[str],
                        user_defined_function: Callable):
        """Calculate a new field in joined_timeseries based on existing
        fields and a user-defined function

        Parameters
        ----------
        parameter_names: List[str]
            Arguments to your user function,
            exisiting joined_timeseries fields
        new_field_name: str
            Name of new field to be added to joined_timeseries
        new_field_type: str
            Data type of the new field
        user_defined_function: Callable
            Function to apply
        """
        schema_df = self.get_joined_timeseries_schema()
        schema_dict = dict(zip(schema_df.column_name, schema_df.column_type))

        # parameter_names must be valid joined_timeseries field names!
        parameter_types = [schema_dict[param] for param in parameter_names]

        self._add_field_name_to_joined_timeseries(field_name=new_field_name,
                                                  field_dtype=new_field_type)

        # Build the query using the parameter names
        args = ""
        for name in parameter_names:
            args += f"{name}, "
        args = args[:-2]

        query = f"""
            UPDATE
                joined_timeseries
            SET
                {new_field_name} = (
            SELECT
                user_defined_function({args})
            );
            """

        # Create and register the user-defined function (UDF)
        with duckdb.connect(self.database_filepath) as con:
            # Register the function
            con.create_function("user_defined_function",
                                user_defined_function,
                                parameter_types,
                                new_field_type)
            # Call the function and add the results to joined_timeseries
            con.sql(query)

    def get_metrics(self,
                    order_by: List[str],
                    group_by: List[str],
                    include_metrics: Union[List[MetricEnum], "all"],
                    filters: Union[List[dict], None] = None,
                    return_query: bool = False,
                    include_geometry: bool = True,
                    ) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
        """Calculate performance metrics using database queries"""
        df = tqu_db.get_metrics(self.database_filepath,
                                group_by=group_by,
                                order_by=order_by,
                                include_metrics=include_metrics,
                                filters=filters,
                                return_query=return_query,
                                include_geometry=include_geometry)
        return df


if __name__ == "__main__":

    # Test data
    TEST_STUDY_DIR = Path("tests/data/test_study")
    PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "test_short_obs.parquet")
    SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "test_short_fcast.parquet")
    CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
    ATTRIBUTES_FILEPATH = Path(TEST_STUDY_DIR, "geo", "test_attr2.parquet")
    GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR,  "geo", "gages.parquet")
    DATABASE_FILEPATH = Path(TEST_STUDY_DIR, "temp_test.db")

    data_vars = {"database_filepath": DATABASE_FILEPATH,
                 "primary_filepath": PRIMARY_FILEPATH,
                 "secondary_filepath": SECONDARY_FILEPATH,
                 "crosswalk_filepath": CROSSWALK_FILEPATH,
                 "geometry_filepath": GEOMETRY_FILEPATH}

    tds = TEEHRDataset(**data_vars)

    # Check the parquet files and report some stats to the user (WIP)
    primary_dict, secondary_dict = tds.describe_inputs()

    # Perform the join and insert into duckdb database
    # NOTE: Right now this will re-join and overwrite
    tds.create_joined_timeseries_table(order_by=["lead_time",
                                                 "primary_location_id"])

    # Join (one or more?) table(s) of attributes to the timeseries table
    tds.join_attributes(ATTRIBUTES_FILEPATH)

    # Calculate and add a field based on some user-defined function (UDF).
    def test_user_function(arg1: float, arg2: str) -> float:
        """Function arguments are fields in joined_timeseries, and
        should have the same data type.
        Note: In the data model, attribute values are always str type"""
        return float(arg1) / float(arg2)

    parameter_names = ["primary_value", "drainage_area_sq_km"]
    new_field_name = "primary_normalized_discharge"
    new_field_type = "FLOAT"
    tds.calculate_field(new_field_name=new_field_name,
                        new_field_type=new_field_type,
                        parameter_names=parameter_names,
                        user_defined_function=test_user_function)

    # Get metrics
    order_by = ["lead_time", "primary_location_id"]
    group_by = [new_field_name, "primary_location_id"]

    t1 = time.time()
    df1 = tds.get_metrics(group_by=group_by,
                          order_by=order_by,
                          include_metrics="all",
                          include_geometry=True)
    print(f"Database query: {(time.time() - t1):.2f} secs")

    pass
