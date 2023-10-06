"""Defines the TEEHR dataset class and pre-processing methods"""
from typing import Union, List, Callable
from pathlib import Path

import duckdb
# import pandas as pd

import teehr.queries.duckdb_database as tqu
import teehr.queries.duckdb as tqu_original
# from teehr.models.queries import JoinedFilterFieldEnum, JoinedFilter


TEST_STUDY_DIR = Path("/mnt/data/ciroh/2023_hilary")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "forcing_analysis_assim_extend", "*.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "forcing_medium_range", "*.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "huc10_huc10_crosswalk.conus.parquet")
# ATTRIBUTES_FILEPATH = Path(TEST_STUDY_DIR, "attrs/huc10_2_year_flow.parquet")
ATTRIBUTES_FILEPATH = Path(TEST_STUDY_DIR, "attrs/*.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR,  "huc10_geometry.conus.parquet")
DATABASE_FILEPATH = Path(TEST_STUDY_DIR, "hilary_post_event.db")


class TeehrDataset():
    """Create instance of a TeehrDataset class and
    initialize study area database"""
    def __init__(self,
                 primary_filepath: Union[str, Path],
                 secondary_filepath: Union[str, Path],
                 crosswalk_filepath: Union[str, Path],
                 database_filepath: Union[str, Path]):
        self.primary_filepath = str(primary_filepath)
        self.secondary_filepath = str(secondary_filepath)
        self.crosswalk_filepath = str(crosswalk_filepath)
        self.database_filepath = str(database_filepath)

        # Create the persistent study database and table here
        create_table = """
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
                );"""

        with duckdb.connect(self.database_filepath) as con:
            con.sql(create_table)
            pass

    def created_joined_timeseries_table(self,
                                        order_by: List[str],
                                        filters: Union[List[dict], None] = None):  # noqa
        """Join primary and secondary timeseries and insert
        into the database

        Need to ensure there's no duplicate data here (duplicate
        value times with different reference times)
        """
        with duckdb.connect(self.database_filepath) as con:
            tqu.join_and_save_timeseries(
                primary_filepath=self.primary_filepath,
                secondary_filepath=self.secondary_filepath,
                crosswalk_filepath=self.crosswalk_filepath,
                order_by=order_by,
                con=con,
                filters=filters,
                return_query=False,
            )

            pass

    def get_joined_timeseries_schema(self):
        """Get field names and field data types from joined_timeseries"""

        desc = """DESCRIBE SELECT * FROM joined_timeseries;"""
        with duckdb.connect(self.database_filepath) as con:
            schema_df = con.sql(desc).to_df()
        schema_dict = dict(zip(schema_df.column_name, schema_df.column_type))
        return schema_dict

    @staticmethod
    def _get_unique_attributes(attributes_filepath: str) -> List:
        """Gets a list of unique attributes and attribute units from the
        provided attribute table(s)"""

        query = f"""
            SELECT
                DISTINCT(attribute_name, attribute_unit)
            FROM
                read_parquet('{attributes_filepath}')
        ;"""
        attr_list = duckdb.sql(query).fetchall()
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
                    attribute_name = '{attr[0]["attribute_name"]}'
                AND
                    attribute_unit = '{attr[0]["attribute_unit"]}'
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

    # attr_pivot: duckdb.DuckDBPyRelation
    def _join_attribute_values(self, field_name: str):
        """Join values of the new attr field on location_id
        NOTE: Somehow attr_pivot does need need to be passed in here"""
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
            # NOTE: Somehow attr_pivot gets cached or saved in memory, so it
            # can be referenced in the update_query in _join_attribute_values
            attr_pivot = self._pivot_attribute_table(str(attributes_filepath), attr) # noqa

            # Add the attr field name to joined_timeseries
            field_name = attr_pivot.columns
            field_name.remove("location_id")
            field_name = field_name[0]

            self._add_field_name_to_joined_timeseries(field_name)

            # Join the attribute values to the new field  , attr_pivot
            self._join_attribute_values(field_name)

    def describe_inputs(self):
        """Get descriptive stats on primary and secondary
        timeseries parquet files"""
        primary_dict = tqu.describe_timeseries( # noqa
            timeseries_filepath=self.primary_filepath)

        secondary_dict = tqu.describe_timeseries( # noqa
            timeseries_filepath=self.secondary_filepath)

        # TODO: Format dicts as a report to user (save to csv?)
        # Add more stats
        # Include visualization option here? (plot timeseries)
        pass

    def calculate_field(self,
                        new_field_name: str,
                        new_field_type: str,
                        parameter_names: List[str],
                        user_defined_function: Callable):
        """
        name: Name of the func, 'user_defined_function'
        func: The user's python function. Executes row by row
        argument_type_list: List of column types used as input
            Must be joined_timeseries table fields
        return_type: Type of the object returned by the function
        type (Optional): Function type. Specify type='arrow' to use
            pyarrow arrays as variables to function
        null_handling (Optional): What to do when null values are
            encountered. By default, null is returned. Change this
            using null_handling='special'

        Notes
        -----
        - Have another function to get the fields and their types
        from the joined timeseries table
        - Joined table will contain all
        # """
        schema_dict = self.get_joined_timeseries_schema()

        # parameter_names must be valid joined_timeseries field names!
        parameter_types = [schema_dict[param] for param in parameter_names]

        self._add_field_name_to_joined_timeseries(field_name=new_field_name,
                                                  field_dtype="FLOAT")

        # Build the query using the parameter names
        args = ""
        for name in parameter_names:
            args += f"{name}, "
        args = args[:-2]

        query = f"""
            WITH udf AS (
                SELECT
                    primary_location_id,
                    user_defined_function({args})
                FROM joined_timeseries
            )
            UPDATE
                joined_timeseries
            SET
                {new_field_name} = (
            SELECT
                *
            EXCLUDE
                primary_location_id
            FROM
                udf
            WHERE
                primary_location_id = udf.primary_location_id
            );
            """
        # TODO: Do we need to join on location_id here? Can we assume
        # the output of UDF is in the same row order as joined_timeseries?

        # Create and register the user-defined function (UDF)
        with duckdb.connect(self.database_filepath) as con:
            # Register the function
            con.create_function("user_defined_function",
                                user_defined_function,
                                parameter_types,
                                new_field_type)
            # Call the function and add the results to joined_timeseries
            con.sql(query)

            # TODO: You could potentially register several functions
            # first, then use them all in a query to calculate a new field?
            pass


if __name__ == "__main__":

    data_vars = {"primary_filepath": PRIMARY_FILEPATH,
                 "secondary_filepath": SECONDARY_FILEPATH,
                 "crosswalk_filepath": CROSSWALK_FILEPATH,
                 "database_filepath": DATABASE_FILEPATH}

    tds = TeehrDataset(**data_vars)

    # query = "SELECT * FROM joined_timeseries LIMIT 10;"
    # with duckdb.connect(str(DATABASE_FILEPATH)) as con:
    #     res_df = con.sql(query).to_df()

    # Check the parquet files and report some stats to the user
    tds.describe_inputs()

    # Perform the join and insert into duckdb database or save as parquet
    tds.created_joined_timeseries_table(order_by=["lead_time",
                                                  "primary_location_id"])

    # Join (one or more?) table(s) of attributes to the timeseries table
    tds.join_attributes(ATTRIBUTES_FILEPATH)

    # Calculate and add a field based on some user-defined function (UDF).
    # Need to supply:
    # - parameter_names: Arguments to your user function
    # - new_field_name: Name of the new field to be added to joined_timeseries
    # - new_field_type: Data type of the new field
    # - names of input parameters (exisiting joined_timeseries fields)
    def test_user_function(arg1: str, arg2: str) -> float:
        """Function arguments are fields in joined_timeseries, and
        should have the same data type.
        Note: In the data model, attribute values are always str type"""
        return float(arg1) * float(arg2)

    parameter_names = ["two_year_flow_cfs", "mean_temperature_F"]
    new_field_name = "my_new_field"
    new_field_type = "FLOAT"
    tds.calculate_field(new_field_name=new_field_name,
                        new_field_type=new_field_type,
                        parameter_names=parameter_names,
                        user_defined_function=test_user_function)

    # TEMP: get_metrics() comparison
    # import time

    # # Get metrics
    # order_by = ["lead_time", "primary_location_id"]
    # group_by = ["lead_time", "primary_location_id"]

    # t1 = time.time()
    # df1 = tqu.get_metrics(DATABASE_FILEPATH,
    #                       group_by=group_by,
    #                       order_by=order_by,
    #                       include_metrics="all")
    # print(f"Database version: {(time.time() - t1):.2f} secs")

    # # df1.to_parquet(Path(TEST_STUDY_DIR, "all_metrics_database_version.parquet"))


    # t1 = time.time()
    # df2 = tqu_original.get_metrics(tds.primary_filepath,
    #                                tds.secondary_filepath,
    #                                tds.crosswalk_filepath,
    #                                group_by=group_by,
    #                                order_by=order_by,
    #                                include_metrics="all")
    # print(f"Original version: {(time.time() - t1):.2f} secs")

    # # df2.to_parquet(Path(TEST_STUDY_DIR, "all_metrics_original_version.parquet"))

    pass
