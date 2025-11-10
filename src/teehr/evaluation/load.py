"""Load class for TEEHR evaluations."""
# from typing import List
from pathlib import Path
import logging

import pyspark.sql.functions as F
import pyspark.sql as ps
import pandas as pd
# from pyarrow import schema as arrow_schema
import geopandas as gpd

from teehr.models.pydantic_table_models import Attribute
from teehr import const
from teehr.models.table_enums import TableWriteEnum
from teehr.utils.utils import remove_dir_if_exists
from teehr.loading.utils import add_or_replace_sdf_column_prefix

logger = logging.getLogger(__name__)


class Load:
    """Class to handle loading data into the warehouse."""

    def __init__(self, ev=None):
        """Initialize the Loader with an Evaluation instance.

        Parameters
        ----------
        ev : Evaluation
            An instance of the Evaluation class containing Spark session
            and catalog details. The default is None, which allows access to
            the classes static methods only.
        """
        if ev is not None:
            self._ev = ev
            self._read = ev.read
            self._extract = ev.extract
            self._validate = ev.validate
            self._write = ev.write

    def dataframe(
        self,
        df: pd.DataFrame | ps.DataFrame,
        table_name: str,
        namespace_name: str = None,
        catalog_name: str = None,
        field_mapping: dict = None,
        constant_field_values: dict = None,
        primary_location_id_prefix: str = None,
        primary_location_id_field: str = "location_id",
        secondary_location_id_prefix: str = None,
        secondary_location_id_field: str = None,
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True
    ):
        """Load a timeseries from an in-memory dataframe.

        Parameters
        ----------
        df : pd.DataFrame | ps.DataFrame
            The input dataframe to load into the warehouse.
        table_name : str
            The name of the table to load the data into.
        namespace_name : str, optional
            The namespace name to load the data into. The default is None,
            which uses the active namespace of the Evaluation.
        catalog_name : str, optional
            The catalog name to load the data into. The default is None,
            which uses the active catalog of the Evaluation.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}.
        primary_location_id_prefix : str, optional
            The prefix to add to primary location IDs.
            Used to ensure unique location IDs across configurations.
        primary_location_id_field : str, optional
            The name of the primary location ID field in the dataframe.
            The default is "location_id".
        secondary_location_id_prefix : str, optional
            The prefix to add to secondary location IDs.
            Used to ensure unique location IDs across configurations.
        secondary_location_id_field : str, optional
            The name of the secondary location ID field in the dataframe.
        write_mode : TableWriteEnum, optional (default: "append")
            The write mode for the table.
            Options are "append", "upsert", and "create_or_replace".
            If "append", the table will be appended without checking
            existing data.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "create_or_replace", a new table will be created or an existing
            table will be replaced.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the DataFrame during validation.
        """
        tbl = self._ev.table(table_name=table_name)

        schema_func = tbl.schema_func
        uniqueness_fields = tbl.uniqueness_fields
        foreign_keys = tbl.foreign_keys
        fields = tbl.fields()

        if (isinstance(df, ps.DataFrame) and df.isEmpty()) or (
            isinstance(df, pd.DataFrame) and df.empty
        ):
            logger.debug(
                "The input dataframe is empty. "
                "No data will be loaded into the table."
            )
            return
        field_mapping = self._extract._merge_field_mapping(
            table_fields=fields,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values
        )
        if field_mapping is not None:
            df = df.rename(columns=field_mapping)

        # Convert the input DataFrame to Spark DataFrame
        if isinstance(df, gpd.GeoDataFrame):
            # This is a bit of a workaround due to spark failing when converting
            # a pd.DataFrame with all null columns. We can pass in a schema, but
            # first we validate with pandera to ensure all columns are present.
            df = schema_func("pandas").validate(df)
            df = df.to_wkb()
            df = self._ev.spark.createDataFrame(
                df, schema=schema_func().to_structtype()
            )
        elif isinstance(df, pd.DataFrame):
            # This is a bit of a workaround due to spark failing when converting
            # a pd.DataFrame with all null columns. We can pass in a schema, but
            # first we validate with pandera to ensure all columns are present.
            df = schema_func("pandas").validate(df)
            df = self._ev.spark.createDataFrame(
                df, schema=schema_func().to_structtype()
            )
        elif not isinstance(df, ps.DataFrame):
            raise TypeError(
                "Input dataframe must be one of Pandas, GeoPandas, or PySpark."
            )

        if constant_field_values:
            for field, value in constant_field_values.items():
                df = df.withColumn(field, F.lit(value))

        if primary_location_id_prefix:
            df = add_or_replace_sdf_column_prefix(
                sdf=df,
                column_name=primary_location_id_field,
                prefix=primary_location_id_prefix,
            )
        if secondary_location_id_prefix:
            df = add_or_replace_sdf_column_prefix(
                sdf=df,
                column_name=secondary_location_id_field,
                prefix=secondary_location_id_prefix,
            )
        if foreign_keys is not None:
            validated_df = self._validate.schema(
                sdf=df,
                table_schema=schema_func(),
                drop_duplicates=drop_duplicates,
                foreign_keys=foreign_keys,
                uniqueness_fields=uniqueness_fields
            )
        else:
            validated_df = self._validate.data(
                df=df,
                table_schema=schema_func(),
            )
        self._write.to_warehouse(
            source_data=validated_df,
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            write_mode=write_mode,
            uniqueness_fields=uniqueness_fields
        )

    def file(
        self,
        in_path: Path | str,
        table_name: str,
        namespace_name: str = None,
        catalog_name: str = None,
        extraction_function: callable = None,
        pattern: str = None,
        field_mapping: dict = None,
        primary_location_id_prefix: str = None,
        primary_location_id_field: str = "location_id",
        secondary_location_id_prefix: str = None,
        secondary_location_id_field: str = None,
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True,
        update_attrs_table: bool = True,
        parallel: bool = False,
        max_workers: int = 1,
        **kwargs
    ):
        """Load location attributes helper.

        Parameters
        ----------
        in_path : Path | str
            The input file or directory path.
        table_name : str
            The name of the table to load the data into.
        namespace_name : str, optional
            The namespace name to load the data into. The default is None,
            which uses the active namespace of the Evaluation.
        catalog_name : str, optional
            The catalog name to load the data into. The default is None,
            which uses the active catalog of the Evaluation.
        extraction_function : callable, optional
            The function to extract data from the input files into TEEHR's
            data model. If None, the table's default extraction function
            is used.
        pattern : str, optional
            The glob pattern to match files in the input directory.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        primary_location_id_prefix : str, optional
            The prefix to add to primary location IDs.
            Used to ensure unique location IDs across configurations.
        primary_location_id_field : str, optional
            The name of the primary location ID field in the dataframe.
            The default is "location_id".
        secondary_location_id_prefix : str, optional
            The prefix to add to secondary location IDs.
            Used to ensure unique location IDs across configurations.
        secondary_location_id_field : str, optional
            The name of the secondary location ID field in the dataframe.
        write_mode : TableWriteEnum, optional (default: "append")
            The write mode for the table.
            Options are "append", "upsert", and "create_or_replace".
            If "append", the table will be appended without checking
            existing data.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "create_or_replace", a new table will be created or an existing
            table will be replaced.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the DataFrame during validation.
        update_attrs_table : bool, optional (default: True)
            Whether to update the location attributes table with any new
            attribute names found in the input data. Only applicable when
            table_name is "location_attributes".
        parallel : bool, optional
            Whether to process timeseries files in parallel. Default is False.
        max_workers : int, optional
            The maximum number of worker processes to use if parallel is True.
            Default is 1. If set to -1, uses the number of CPUs available.
        **kwargs
            Additional keyword arguments passed to the extraction function.
        """
        # Clear the cache directory if it exists.
        table_cache_dir = Path(
            self._ev.cache_dir,
            const.LOADING_CACHE_DIR,
            table_name
        )
        remove_dir_if_exists(table_cache_dir)
        in_path = Path(in_path)

        tbl = self._ev.table(table_name=table_name)
        if extraction_function is None:
            extraction_function = tbl.extraction_func
            if extraction_function is None:
                raise ValueError(
                    "No extraction function provided and "
                    "none found in table properties."
                )
        schema_func = tbl.schema_func
        uniqueness_fields = tbl.uniqueness_fields
        foreign_keys = tbl.foreign_keys
        fields = tbl.fields()

        # Begin the ETL process.
        self._extract.to_cache(
            in_datapath=in_path,
            field_mapping=field_mapping,
            pattern=pattern,
            cache_dir=table_cache_dir,
            table_fields=fields,
            table_schema_func=schema_func(type="pandas"),
            write_schema_func=schema_func(type="arrow"),
            extraction_func=extraction_function,
            parallel=parallel,
            max_workers=max_workers,
            **kwargs
        )
        # Read the converted files to Spark DataFrame
        sdf = self._read.from_cache(
            path=table_cache_dir,
            table_schema_func=schema_func()
        ).to_sdf()
        # Add or replace primary_location_id prefix if provided
        if primary_location_id_prefix:
            sdf = add_or_replace_sdf_column_prefix(
                sdf=sdf,
                column_name=primary_location_id_field,
                prefix=primary_location_id_prefix,
            )
        if secondary_location_id_prefix:
            sdf = add_or_replace_sdf_column_prefix(
                sdf=sdf,
                column_name=secondary_location_id_field,
                prefix=secondary_location_id_prefix,
            )
        # Only valid when table_name = 'location_attributes'
        # What happens if not? -- the select fails
        if update_attrs_table and table_name == "location_attributes":
            attr_names = [
                row.attribute_name for row in
                sdf.select("attribute_name").distinct().collect()
            ]
            attr_list = []
            for attr_name in attr_names:
                attr_list.append(
                    Attribute(
                        name=attr_name,
                        type="continuous",
                        description=f"{attr_name} default description"
                    )
                )
            self._ev.attributes.add(attr_list)

        if foreign_keys is not None:
            validated_df = self._validate.schema(
                sdf=sdf,
                table_schema=schema_func(),
                drop_duplicates=drop_duplicates,
                foreign_keys=foreign_keys,
                uniqueness_fields=uniqueness_fields
            )
        else:
            validated_df = self._validate.data(
                df=sdf,
                table_schema=schema_func(),
            )
        self._write.to_warehouse(
            source_data=validated_df,
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            write_mode=write_mode,
            uniqueness_fields=uniqueness_fields
        )

    def from_cache(
        self,
        in_path: Path | str,
        table_name: str,
        namespace_name: str = None,
        catalog_name: str = None,
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True,
        update_attrs_table: bool = True
    ):
        """Load data from the cache.

        Parameters
        ----------
        in_path : Path | str
            The input cache directory path.
        table_name : str
            The name of the table to load the data into.
        namespace_name : str, optional
            The namespace name to load the data into. The default is None,
            which uses the active namespace of the Evaluation.
        catalog_name : str, optional
            The catalog name to load the data into. The default is None,
            which uses the active catalog of the Evaluation.
        write_mode : TableWriteEnum, optional (default: "append")
            The write mode for the table.
            Options are "append", "upsert", and "create_or_replace".
            If "append", the table will be appended without checking
            existing data.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "create_or_replace", a new table will be created or an existing
            table will be replaced.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the DataFrame during validation.
        update_attrs_table : bool, optional (default: True)
            Whether to update the location attributes table with any new
            attribute names found in the input data. Only applicable when
            table_name is "location_attributes".
        """
        # Get the cache directory path.
        in_path = Path(in_path)

        tbl = self._ev.table(table_name=table_name)
        schema_func = tbl.schema_func
        uniqueness_fields = tbl.uniqueness_fields
        foreign_keys = tbl.foreign_keys

        sdf = self._read.from_cache(
            path=in_path,
            table_schema_func=schema_func()
        ).to_sdf()

        if update_attrs_table and table_name == "location_attributes":
            attr_names = [
                row.attribute_name for row in
                sdf.select("attribute_name").distinct().collect()
            ]
            attr_list = []
            for attr_name in attr_names:
                attr_list.append(
                    Attribute(
                        name=attr_name,
                        type="continuous",
                        description=f"{attr_name} default description"
                    )
                )
            self._ev.attributes.add(attr_list)

        if foreign_keys is not None:
            validated_df = self._validate.schema(
                sdf=sdf,
                table_schema=schema_func(),
                drop_duplicates=drop_duplicates,
                foreign_keys=foreign_keys,
                uniqueness_fields=uniqueness_fields
            )
        else:
            validated_df = self._validate.data(
                df=sdf,
                table_schema=schema_func(),
            )
        self._write.to_warehouse(
            source_data=validated_df,
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            write_mode=write_mode,
            uniqueness_fields=uniqueness_fields
        )
