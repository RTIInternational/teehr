"""Read class for TEEHR evaluations."""
from typing import Union
import logging
from pathlib import Path

# from pyspark.sql import DataFrame
# import pandas as pd
# from pyarrow import schema as arrow_schema
# import geopandas as gpd
import pyspark.sql as ps
from pandera.pyspark import DataFrameSchema as SparkDataFrameSchema
from pandera import DataFrameSchema as PandasDataFrameSchema

from teehr.utils.utils import path_to_spark
# from teehr.evaluation.utils import get_table_instance

logger = logging.getLogger(__name__)


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
        if ev is not None:  # needed?
            self._ev = ev

    def from_cache(
        self,
        path: Union[str, Path],
        table_schema_func: SparkDataFrameSchema | PandasDataFrameSchema,
        pattern: str,
        file_format: str,
        show_missing_table_warning: bool = False,
        **options
    ) -> ps.DataFrame:
        """Read data from table directory as a spark dataframe.

        Parameters
        ----------
        path : Union[str, Path, S3Path]
            The path to the directory containing the files.
        pattern : str, optional
            The pattern to match files.
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
        if isinstance(table_schema_func, SparkDataFrameSchema):
            schema = table_schema_func.to_structtype()
        df = self._ev.spark.read.format(file_format).options(**options).load(path, schema=schema)
        if df.isEmpty():
            if show_missing_table_warning:
                logger.warning(
                    f"An empty dataframe was returned from '{path}'."
                )

        return df

    def from_warehouse(
        self,
        table: str,
        catalog_name: str = None,
        namespace: str = None,
    ) -> ps.DataFrame:
        """Read data from table as a spark dataframe.

        Returns
        -------
        df : ps.DataFrame
            The spark dataframe.
        """
        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name
        if namespace is None:
            namespace = self._ev.active_catalog.namespace_name
        logger.info(
            f"Reading files from {catalog_name}.{namespace}.{table}."
        )
        sdf = (self._ev.spark.read.format("iceberg").load(
                f"{catalog_name}.{namespace}.{table}"
            )
        )
        return sdf
