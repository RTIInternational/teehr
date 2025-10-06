"""Load class for TEEHR evaluations."""
# from typing import List
from pathlib import Path
import logging

import pyspark.sql.functions as F
import pyspark.sql as ps
import pandas as pd
# from pyarrow import schema as arrow_schema
# import geopandas as gpd

from teehr.models.pydantic_table_models import Attribute
from teehr import const
from teehr.models.table_enums import TableWriteEnum
from teehr.models.table_properties import TBLPROPERTIES
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
        field_mapping: dict,
        constant_field_values: dict,
        location_id_prefix: str,
        write_mode: TableWriteEnum,
        persist_dataframe: bool,
        drop_duplicates: bool
    ):
        """Load a timeseries from an in-memory dataframe."""
        if (isinstance(df, ps.DataFrame) and df.isEmpty()) or (
            isinstance(df, pd.DataFrame) and df.empty
        ):
            logger.debug(
                "The input dataframe is empty. "
                "No data will be loaded into the table."
            )
            return
        # self.schema_func(type="pandas").columns.keys()
        self._extract._merge_field_mapping(
            table_fields=self.field_enum(),
            field_mapping=field_mapping,
            constant_field_values=constant_field_values
        )
        # Convert the input DataFrame to Spark DataFrame
        if isinstance(df, pd.DataFrame):
            df = self.spark.createDataFrame(df)
        elif not isinstance(df, ps.DataFrame):
            raise TypeError(
                "Input dataframe must be a Pandas DataFrame or a PySpark DataFrame."
            )
        # Apply field mapping and constant field values
        if field_mapping:
            df = df.withColumnsRenamed(field_mapping)

        if constant_field_values:
            for field, value in constant_field_values.items():
                df = df.withColumn(field, F.lit(value))

        if persist_dataframe:
            df = df.persist()

        if location_id_prefix:
            df = add_or_replace_sdf_column_prefix(
                sdf=df,
                column_name="location_id",
                prefix=location_id_prefix,
            )
        validated_df = self._validate.schema(
            sdf=df,
            table_schema=self.schema_func(),
            drop_duplicates=drop_duplicates,
            foreign_keys=self.foreign_keys,
            uniqueness_fields=self.uniqueness_fields,
            add_missing_columns=True
        )
        self._write.to_warehouse(
            source_data=validated_df,
            table_name=self.table_name,
            write_mode=write_mode,
            uniqueness_fields=self.uniqueness_fields
        )

        df.unpersist()

    def file(
        self,
        in_path: Path | str,
        table_name: str,
        namespace_name: str = None,
        catalog_name: str = None,
        extraction_function: callable = None,
        pattern: str = None,
        field_mapping: dict = None,
        location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True,
        update_attrs_table: bool = True,
        parallel: bool = False,
        max_workers: int = 1,
        location_id_field: str = "location_id",
        persist_dataframe: bool = False,
        **kwargs
    ):
        """Load location attributes helper."""
        # TODO: remove persist_dataframe?

        # Clear the cache directory if it exists.
        table_cache_dir = Path(
            self._ev.cache_dir,
            const.LOADING_CACHE_DIR,
            table_name
        )
        remove_dir_if_exists(table_cache_dir)
        in_path = Path(in_path)

        table_props = TBLPROPERTIES.get(table_name)
        # TODO: Should we allow loading to a new table?
        if not table_props:
            raise ValueError(f"Table properties for {table_name} not found.")

        if extraction_function is None:
            extraction_function = table_props.get("extraction_func")
            if extraction_function is None:
                raise ValueError(
                    "No extraction function provided and "
                    "none found in table properties."
                )

        schema_func = table_props.get("schema_func")
        uniqueness_fields = table_props.get("uniqueness_fields")
        foreign_keys = table_props.get("foreign_keys")
        # Aren't the table fields fixed and defined in the schema?
        fields = list(schema_func().columns.keys())

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
        # Add or replace location_id prefix if provided
        if location_id_prefix:
            sdf = add_or_replace_sdf_column_prefix(
                sdf=sdf,
                column_name=location_id_field,
                prefix=location_id_prefix,
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


    # def domain_value(
    #     self,
    #     table_name: str,
    #     domain_model:
    # )
    # Include "add()" here for domain values? Not really loading though?