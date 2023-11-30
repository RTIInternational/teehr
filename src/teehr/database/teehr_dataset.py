"""Defines the TEEHR dataset class and pre-processing methods"""
from typing import Union, List, Callable, Tuple, Dict, Any
from pathlib import Path

import re
import duckdb
import pandas as pd
import geopandas as gpd

import teehr.queries.duckdb_database as tqu_db

import teehr.queries.utils as tqu
from teehr.models.queries_database import (
    JoinedFieldNameEnum,
    JoinedTimeseriesQuery,
    CalculateField,
    MetricQuery,
    TimeseriesQuery,
    TimeseriesCharQuery,
    JoinedTimeseriesFieldName,
)
from teehr.models.queries import MetricEnum


class TEEHRDatasetAPI:
    """Create instance of a TeehrDataset class and
    initialize study area database"""

    def __init__(
        self,
        database_filepath: Union[str, Path],
    ):
        self.database_filepath = str(database_filepath)
        self._initialize_database_tables()

    def query(
        self, query: str, format: str = None, create_function_args: Dict = None
    ):
        """Run query against the class's database."""
        if not create_function_args:
            with duckdb.connect(self.database_filepath) as con:
                if format == "df":
                    return con.sql(query).df()
                elif format == "raw":
                    return con.sql(query).show()
                elif format == "relation":
                    return con.sql(query)

                con.sql(query)
                return None
        else:
            user_defined_function = create_function_args["function"]
            function_name = create_function_args["function_name"]
            parameter_types = create_function_args["parameter_types"]
            new_field_type = create_function_args["new_field_type"]
            with duckdb.connect(self.database_filepath) as con:
                # Register the function
                con.create_function(
                    function_name,
                    user_defined_function,
                    parameter_types,
                    new_field_type,
                )
                # Call the function and add the results to joined_timeseries
                con.sql(query)

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

    def get_joined_timeseries_schema(self) -> pd.DataFrame:
        """Get field names and field data types from joined_timeseries,"""

        desc = """DESCRIBE SELECT * FROM joined_timeseries;"""
        joined_df = self.query(desc, format="df")

        return joined_df

    @staticmethod
    def _sanitize_field_name(field_name: str) -> str:
        # I think we will need this
        allowed_chars = r"[^a-zA-Z0-9_]"
        search = re.compile(allowed_chars).search

        if bool(search(field_name)):
            sub = re.compile(allowed_chars).sub
            return str(sub("_", field_name))

        return field_name

    @staticmethod
    def describe_inputs(
        primary_filepath: Union[str, Path],
        secondary_filepath: Union[str, Path],
    ) -> Tuple[Dict]:
        """Get descriptive stats on primary and secondary
        timeseries parquet files"""
        primary_dict = tqu_db.describe_timeseries(
            timeseries_filepath=primary_filepath
        )

        secondary_dict = tqu_db.describe_timeseries(
            timeseries_filepath=secondary_filepath
        )

        df = pd.DataFrame(
            {
                "primary": [primary_dict[key] for key in primary_dict.keys()],
                "secondary": [
                    secondary_dict[key] for key in secondary_dict.keys()
                ],
            },
            index=primary_dict.keys(),
        )

        return df

    def _check_if_geometry_is_inserted(self):
        """Make sure the geometry data has been inserted
        into the geometry table."""
        df = self.query("SELECT COUNT(geometry) FROM geometry;", format="df")
        if df["count(geometry)"].values == 0:
            raise ValueError(
                "The geometry table is empty! Please insert geometry first"
            )

    def _validate_query_model(self, query_model: Any) -> Any:
        """Validate the query based on existing table fields."""
        schema_df = self.get_joined_timeseries_schema()
        validated_model = query_model.model_validate(
            query_model.model_dump(),
            context={"existing_fields": schema_df.column_name.tolist()},
        )
        return validated_model

    def get_metrics(
        self,
        mq: MetricQuery,
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame, str]:
        """Calculate performance metrics using database queries"""

        mq = self._validate_query_model(mq)

        query = tqu_db.create_get_metrics_query(mq)

        if mq.return_query:
            return tqu.remove_empty_lines(query)
        elif mq.include_geometry:
            self._check_if_geometry_is_inserted()
            df = self.query(query, format="df")
            return tqu.df_to_gdf(df)
        else:
            df = self.query(query, format="df")
        return df

    def get_timeseries(
        self,
        tq: TimeseriesQuery
    ) -> Union[pd.DataFrame, str]:

        tq = self._validate_query_model(tq)

        query = tqu_db.create_get_timeseries_query(tq)

        if tq.return_query:
            return tqu.remove_empty_lines(query)
        else:
            df = self.query(query, format="df")
        return df

    def get_timeseries_characteristics(
        self, tcq: TimeseriesCharQuery
    ) -> Union[str, pd.DataFrame]:

        tcq = self._validate_query_model(tcq)

        query = tqu_db.create_get_timeseries_char_query(tcq)

        if tcq.return_query:
            return tqu.remove_empty_lines(query)
        else:
            df = self.query(query, format="df")
        return df

        pass

    def get_unique_field_values(self, fn: JoinedTimeseriesFieldName):
        """Get unique values for a given field"""
        fn = self._validate_query_model(fn)
        query = tqu_db.create_unique_field_values_query(fn)
        df = self.query(query, format="df")
        return df


class TEEHRDatasetDB(TEEHRDatasetAPI):
    """Extends TEEHRDatasetAPI class to provide additional
    functionality."""

    def _drop_joined_timeseries_field(self, field_name: str):
        """Drops a field by name from joined_timeseries table"""
        query = f"""
            ALTER TABLE joined_timeseries
            DROP COLUMN {field_name}
        ;"""
        self.query(query)

    def _validate_joined_timeseries_base_fields(self, drop_added_fields: bool):
        """(WIP) Make sure no extra fields have been
        added or base fields dropped."""
        schema_df = self.get_joined_timeseries_schema()
        if schema_df.index.size < 11:
            raise ValueError(
                "There are missing fields in the joined_timeseries schema"
            )
        for field_name in schema_df.column_name.tolist():
            if field_name not in JoinedFieldNameEnum.__members__:
                if drop_added_fields:
                    print(f"Dropping added field {field_name}")
                    self._drop_joined_timeseries_field(field_name)
                else:
                    raise ValueError(
                        f"An added field '{field_name}' exists,"
                        "please drop it before joining timeseries"
                    )

    def insert_geometry(self, geometry_filepath: Union[str, Path]):
        # Load the geometry data into a separate table
        if geometry_filepath:
            query = f"""
                INSERT INTO
                    geometry
                SELECT
                    pq.id, pq.name, pq.geometry
                FROM
                    read_parquet('{str(geometry_filepath)}') pq
                WHERE NOT EXISTS (
                    SELECT
                        id, name, geometry
                    FROM
                        geometry
                    WHERE
                        pq.id = id AND pq.name = name
                    AND pq.geometry = geometry
                )
            ;"""
            self.query(query)

    def insert_joined_timeseries(
        self,
        primary_filepath: Union[str, Path],
        secondary_filepath: Union[str, Path],
        crosswalk_filepath: Union[str, Path],
        order_by: List[str] = [
            "reference_time",
            "primary_location_id",
        ],
        drop_added_fields=False,
    ):
        """Join primary and secondary timeseries and insert
        into the database.
        """

        self._validate_joined_timeseries_base_fields(drop_added_fields)

        jtq = JoinedTimeseriesQuery.model_validate(
            {
                "primary_filepath": primary_filepath,
                "secondary_filepath": secondary_filepath,
                "crosswalk_filepath": crosswalk_filepath,
                "order_by": order_by,
            }
        )

        query = tqu_db.create_join_and_save_timeseries_query(jtq)
        self.query(query)

    def _get_unique_attributes(self, attributes_filepath: str) -> List:
        """Gets a list of unique attributes and attribute units from the
        provided attribute table(s)"""

        query = f"""
            SELECT
                DISTINCT attribute_name, attribute_unit
            FROM
                read_parquet('{attributes_filepath}')
        ;"""
        # attr_list = duckdb.sql(query).df().to_dict(orient="records")
        attr_list = self.query(query, format="df").to_dict(orient="records")
        return attr_list

    def _pivot_attribute_table(
        self, attributes_filepath: str, attr: duckdb.DuckDBPyRelation
    ) -> duckdb.DuckDBPyRelation:
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
        attr_pivot = self.query(query=query, format="relation")

        return attr_pivot

    def _add_field_name_to_joined_timeseries(
        self, field_name: str, field_dtype="VARCHAR"
    ):
        """Adds a field name to joined_timeseries if it does not already exist"""
        query = f"""
            ALTER TABLE
                joined_timeseries
            ADD IF NOT EXISTS
                {field_name} {field_dtype}
        ;"""
        self.query(query)

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
        self.query(update_query)

    def join_attributes(self, attributes_filepath: Union[str, Path]):
        """Joins attributes from the provided attribute table(s) to new
        fields in the joined_timeseries table"""
        attr_list = self._get_unique_attributes(str(attributes_filepath))

        for attr in attr_list:
            # Pivot the single attribute
            attr_pivot = self._pivot_attribute_table(
                str(attributes_filepath), attr
            )

            # Add the attr field name to joined_timeseries
            field_name = attr_pivot.columns
            field_name.remove("location_id")
            field_name = self._sanitize_field_name(field_name[0])

            self._add_field_name_to_joined_timeseries(field_name)

            # Join the attribute values to the new field, attr_pivot
            self._join_attribute_values(field_name)

    def calculate_field(
        self,
        new_field_name: str,
        new_field_type: str,
        parameter_names: List[str],
        user_defined_function: Callable,
        replace: bool = True,
    ):
        """Calculate a new field in joined_timeseries based on existing
        fields and a user-defined function

        Parameters
        ----------
        parameter_names: List[str]
            Arguments to your user function,
            must be exisiting joined_timeseries fields
        new_field_name: str
            Name of new field to be added to joined_timeseries
        new_field_type: str
            Data type of the new field
        user_defined_function: Callable
            Function to apply
        replace: bool
            If replace is True and new_field_name already exists, it is
            dropped before being recalculated and re-added

        """
        schema_df = self.get_joined_timeseries_schema()

        sanitized_field_name = self._sanitize_field_name(new_field_name)

        cf = CalculateField.model_validate(
            {
                "new_field_name": sanitized_field_name,
                "new_field_type": new_field_type,
                "parameter_names": parameter_names,
            },
            context={"existing_fields": schema_df.column_name.tolist()},
        )

        # Get data types of function parameters
        schema_dict = dict(zip(schema_df.column_name, schema_df.column_type))
        parameter_types = [schema_dict[param] for param in parameter_names]

        if replace and (
            sanitized_field_name in schema_df.column_name.tolist()
        ):
            self._drop_joined_timeseries_field(new_field_name)

        self._add_field_name_to_joined_timeseries(
            field_name=sanitized_field_name, field_dtype=new_field_type
        )

        # Build the query using the parameter names
        args = ""
        for name in cf.parameter_names:
            args += f"{name}, "
        args = args[:-2]

        query = f"""
            UPDATE
                joined_timeseries
            SET
                {cf.new_field_name} = (
            SELECT
                user_defined_function({args})
            );
            """

        create_function_args = {
            "function": user_defined_function,
            "function_name": "user_defined_function",
            "parameter_types": parameter_types,
            "new_field_type": cf.new_field_type,
        }

        self.query(query=query, create_function_args=create_function_args)

    def _validate_query_model(self, query_model: Any, data: Dict) -> Any:
        """Validate the query based on existing table fields."""
        schema_df = self.get_joined_timeseries_schema()
        validated_model = query_model.model_validate(
            data,
            context={"existing_fields": schema_df.column_name.tolist()},
        )
        return validated_model

    def get_metrics(
        self,
        group_by: List[str],
        order_by: List[str],
        include_metrics: Union[List[MetricEnum], "all"],
        filters: Union[List[dict], None] = None,
        include_geometry: bool = False,
        return_query: bool = False,
    ) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
        """Calculate performance metrics using database queries"""

        data = {
            "group_by": group_by,
            "order_by": order_by,
            "include_metrics": include_metrics,
            "filters": filters,
            "include_geometry": include_geometry,
            "return_query": return_query,
        }
        mq = self._validate_query_model(MetricQuery, data)

        query = tqu_db.create_get_metrics_query(mq)

        if mq.return_query:
            return tqu.remove_empty_lines(query)
        elif mq.include_geometry:
            self._check_if_geometry_is_inserted()
            df = self.query(query, format="df")
            return tqu.df_to_gdf(df)
        else:
            df = self.query(query, format="df")
        return df

    def get_timeseries(
        self,
        order_by: List[str],
        filters: Union[List[dict], None] = None,
        return_query: bool = False,
    ) -> Union[pd.DataFrame, str]:

        data = {
            "order_by": order_by,
            "filters": filters,
            "return_query": return_query,
        }
        tq = self._validate_query_model(TimeseriesQuery, data)

        query = tqu_db.create_get_timeseries_query(tq)

        if tq.return_query:
            return tqu.remove_empty_lines(query)
        else:
            df = self.query(query, format="df")
        return df

    def get_timeseries_characteristics(
        self,
        group_by: List[str],
        order_by: List[str],
        timeseries_name: str,
        filters: Union[List[dict], None] = None,
        return_query: bool = False,
    ) -> Union[str, pd.DataFrame]:

        data = {
            "order_by": order_by,
            "group_by": group_by,
            "timeseries_name": timeseries_name,
            "filters": filters,
            "return_query": return_query,
        }
        tcq = self._validate_query_model(TimeseriesCharQuery, data)

        query = tqu_db.create_get_timeseries_char_query(tcq)

        if tcq.return_query:
            return tqu.remove_empty_lines(query)
        else:
            df = self.query(query, format="df")
        return df

    def get_unique_field_values(self, field_name: str):
        """Get unique values for a given field"""
        data = {"field_name": field_name}
        fn = self._validate_query_model(JoinedTimeseriesFieldName, data)
        query = tqu_db.create_unique_field_values_query(fn)
        df = self.query(query, format="df")
        return df
