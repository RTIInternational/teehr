"""Read class for TEEHR evaluations."""
from typing import Union, List
import logging
from pathlib import Path

import pyspark.sql.types as T
import pyspark.sql.functions as F
import pyspark.sql as ps
from pandera.pyspark import DataFrameSchema as SparkDataFrameSchema
from pandera import DataFrameSchema as PandasDataFrameSchema

from teehr.models.filters import FilterBaseModel
from teehr.utils.utils import path_to_spark
from teehr.querying.filter_format import (
    format_filter,
    validate_filter
)
from teehr.evaluation.utils import get_table_instance
from teehr.models.table_properties import TBLPROPERTIES


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
        if ev is not None:  # needed?
            self._ev = ev
        self.sdf: ps.DataFrame = None

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
        table_schema_func: SparkDataFrameSchema | PandasDataFrameSchema,
        pattern: str = "**/*.parquet",
        file_format: str = "parquet",
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
        self.sdf = df
        return self

    def from_warehouse(
        self,
        table_name: str,
        catalog_name: str = None,
        namespace_name: str = None,
        filters: Union[
            str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]
        ] = None,
        validate_filter_field_types: bool = True
    ) -> None:
        """Read data from table as a spark dataframe.

        Returns
        -------
        df : ps.DataFrame
            The spark dataframe.
        """
        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name
        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name
        logger.info(
            f"Reading files from {catalog_name}.{namespace_name}.{table_name}."
        )
        # This is the guts of validate_and_apply_filters re-configured a bit.
        # Should it be moved to it's own function?
        # TODO: Replace with ev.validate.filters?
        if filters is None:
            # No filter applied, just read the whole table
            sdf = (self._ev.spark.read.format("iceberg").load(
                    f"{catalog_name}.{namespace_name}.{table_name}"
                )
            )
            sdf = self._apply_datatype_transform(sdf)
            self.sdf = sdf
            return self

        if isinstance(filters, str):
            logger.debug(
                f"Filter {filters} is already string.  Applying as is."
            )
            sdf = (self._ev.spark.read.format("iceberg").load(
                    f"{catalog_name}.{namespace_name}.{table_name}"
                ).filter(filters)
            )
            sdf = self._apply_datatype_transform(sdf)
            self.sdf = sdf
            return self

        if not isinstance(filters, List):
            logger.debug("Filter is not a list.  Making a list.")
            filters = [filters]

        filter_model = TBLPROPERTIES[table_name].get("filter_model")
        dataframe_schema = TBLPROPERTIES[table_name].get("schema_func")
        # fields_enum = list(dataframe_schema().columns.keys())
        # TODO: Should the field_enum be added to the TBLPROPERTIES?
        fields_enum = get_table_instance(ev=self._ev, table_name=table_name).field_enum()
        sdf = (
            self._ev.spark.read.format("iceberg").load(
                    f"{catalog_name}.{namespace_name}.{table_name}"
                )
        )
        for filter in filters:
            logger.debug(f"Validating and applying {filter}")

            if not isinstance(filter, str):
                filter = filter_model.model_validate(
                    filter,
                    context={"fields_enum": fields_enum}
                )
                logger.debug(f"Filter: {filter.model_dump_json()}")
                if validate_filter_field_types is True:
                    filter = validate_filter(filter, dataframe_schema())
                filter = format_filter(filter)

            sdf = sdf.filter(filter)
            sdf = self._apply_datatype_transform(sdf)
            self.sdf = sdf
            return self

    def to_pandas(self):
        """Return Pandas DataFrame."""
        if self.sdf is None:
            raise ValueError(
                "No data has been read, please read data first using the "
                "from_warehouse() or from_cache() methods."
            )
        df = self.sdf.toPandas()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        # Join location geometry if applicable (if location IDs are present)?
        raise NotImplementedError("to_geopandas method must be implemented.")

    def to_sdf(self):
        """Return PySpark DataFrame.

        The PySpark DataFrame can be further processed using PySpark. Note,
        PySpark DataFrames are lazy and will not be executed until an action
        is called.  For example, calling `show()`, `collect()` or toPandas().
        This can be useful for further processing or analysis, for example,

        >>> ts_sdf = ev.primary_timeseries.query(
        >>>     filters=[
        >>>         "value_time > '2022-01-01'",
        >>>         "value_time < '2022-01-02'",
        >>>         "location_id = 'gage-C'"
        >>>     ]
        >>> ).to_sdf()
        >>> ts_df = (
        >>>     ts_sdf.select("value_time", "location_id", "value")
        >>>    .orderBy("value").toPandas()
        >>> )
        >>> ts_df.head()
        """
        if self.sdf is None:
            raise ValueError(
                "No data has been read, please read data first using the "
                "from_warehouse() or from_cache() methods."
            )
        return self.sdf
