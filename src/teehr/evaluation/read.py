"""Read class for TEEHR evaluations."""
from typing import Union
import logging
from pathlib import Path

import pyspark.sql.types as T
import pyspark.sql.functions as F
import pyspark.sql as ps
from pandera.pyspark import DataFrameSchema as SparkDataFrameSchema
from pandera import DataFrameSchema as PandasDataFrameSchema

from teehr.utils.utils import path_to_spark

logger = logging.getLogger(__name__)

DATATYPE_READ_TRANSFORMS = {"forecast_lead_time": T.DayTimeIntervalType(0, 3)}


# NOTE: Should this inherit the Table class?
class Read:
    """Class to handle reading evaluation results from storage."""

    def __init__(self, ev=None):
        """Initialize the Reader with an Evaluation instance.

        Parameters
        ----------
        ev : Evaluation
            An instance of the Evaluation class containing Spark session
            and catalog details. The default is None, which allows access to
            the classes static methods only.
        """
        if ev is not None:
            self._ev = ev

    @staticmethod
    def _apply_datatype_transform(sdf: ps.DataFrame) -> ps.DataFrame:
        """Apply datatype transformations to the Spark DataFrame.

        Parameters
        ----------
        sdf : ps.DataFrame
            The Spark DataFrame to transform.

        Returns
        -------
        ps.DataFrame
            The transformed Spark DataFrame.
        """
        tbl_columns = sdf.columns
        if any(item in DATATYPE_READ_TRANSFORMS for item in tbl_columns):
            for col, datatype in DATATYPE_READ_TRANSFORMS.items():
                if col in tbl_columns:
                    sdf = sdf.withColumn(col, F.col(col).cast(datatype))
        return sdf

    def from_cache(
        self,
        path: Union[str, Path],
        table_schema: SparkDataFrameSchema | PandasDataFrameSchema,
        pattern: str = "**/*.parquet",
        file_format: str = "parquet",
        show_missing_table_warning: bool = False,
        **options
    ) -> ps.DataFrame:
        """Read data from table directory as a spark dataframe.

        Parameters
        ----------
        path : Union[str, Path]
            The path to the cache directory containing the files.
        table_schema : SparkDataFrameSchema | PandasDataFrameSchema
            The schema of the table.
        pattern : str, optional
            The pattern to match files. The default is "**/*.parquet".
        file_format : str, optional
            The file format to read. The default is "parquet".
        show_missing_table_warning : bool, optional
            If True, show the warning an empty table was returned.
            The default is True.
        **options
            Additional options to pass to the spark read method.

        Returns
        -------
        df : ps.DataFrame
            The spark dataframe.
        """
        logger.info(f"Reading files from {path}.")
        if len(options) == 0:
            options = {
                "header": "true",
                "ignoreMissingFiles": "true"
            }

        path = path_to_spark(path, pattern)
        # First, read the file with the schema and check if it's empty.
        # If it's not empty and it's the joined timeseries table,
        # read it again without the schema to ensure all fields are included.
        # Otherwise, continue.
        # TODO: What if it's Pandas schema?
        schema = None
        if isinstance(table_schema, SparkDataFrameSchema):
            schema = table_schema.to_structtype()

        reader = self._ev.spark.read.format(file_format).options(**options)
        if schema is not None:
            df = reader.load(path, schema=schema)
        else:
            df = reader.load(path)
        if df.isEmpty():
            if show_missing_table_warning:
                logger.warning(
                    f"An empty dataframe was returned from '{path}'."
                )
        return df

    def from_warehouse(
        self,
        table_name: str,
        catalog_name: str = None,
        namespace_name: str = None,
    ) -> ps.DataFrame:
        """Read data from table as a spark dataframe.

        Parameters
        ----------
        table_name : str
            The name of the table to read.
        catalog_name : str, optional
            The catalog name. If None, uses the active catalog. The default is None.
        namespace_name : str, optional
            The namespace name. If None, uses the active namespace. The default is None.
        filters : Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]
        ], optional
            The filters to apply to the table. The default is None.
        validate_filter_field_types : bool, optional
            Whether to validate the filter field types. The default is True.

        Returns
        -------
        df : ps.DataFrame
            The spark dataframe.

        Example
        -------
        >>> ts_sdf = ev.read.from_warehouse(
        >>>     table_name="primary_timeseries",
        >>>     filters=[
        >>>         "value_time > '2022-01-01'",
        >>>         "value_time < '2022-01-02'",
        >>>         "location_id = 'gage-C'"
        >>>     ]
        >>> ).to_sdf()
        """
        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name
        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name
        logger.info(
            f"Reading files from {catalog_name}.{namespace_name}.{table_name}."
        )
        sdf = (self._ev.spark.read.format("iceberg").load(
                    f"{catalog_name}.{namespace_name}.{table_name}"
            )
        )
        sdf = self._apply_datatype_transform(sdf)

        return sdf
