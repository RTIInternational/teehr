"""Domain table class."""
from teehr.evaluation.tables.base_table import BaseTable
from teehr.models.pydantic_table_models import TableBaseModel
from teehr.querying.utils import order_df
from teehr.models.filters import TableFilter
from teehr.models.str_enum import StrEnum
from teehr.models.table_enums import TableWriteEnum
import pandas as pd
from typing import List, Union
import logging

import pyspark.sql as ps

logger = logging.getLogger(__name__)


class DomainTable(BaseTable):
    """Domain table class."""

    def __init__(self, ev):
        """Initialize the Table class.

        Parameters
        ----------
        ev : EvaluationBaseModel
            The parent Evaluation instance providing access to Spark session,
            catalogs, and related table operations.
        """
        super().__init__(ev)

    def _add(
        self,
        obj: Union[TableBaseModel, List[TableBaseModel]],
        write_mode: TableWriteEnum = TableWriteEnum.append
    ):
        # logger.info(f"Adding attribute to {self.dir}")
        self._check_load_table()

        org_df = self.to_sdf()

        if issubclass(type(obj), TableBaseModel):
            obj = [obj]

        # validate the data to be added
        new_df = self._ev.spark.createDataFrame(
            pd.DataFrame([o.model_dump() for o in obj])
            )
        logger.info(
            f"Validating {len(obj)} objects before adding to {self.table_name} table"
            )
        # if self.foreign_keys is not None:
        new_df_validated = self._ev.validate.data(
            df=new_df.cache(),
            table_schema=self.schema_func(),
        )

        # warn user if rows in added data already exist in the original table
        df_matched = new_df_validated.join(
            org_df, on=self.uniqueness_fields, how="left_semi"
        )
        if df_matched.count() == 0:
            # add the validated new data to the existing data
            logger.info(f"Adding {len(obj)} objects to {self.table_name} table")
            combined_df = org_df.unionByName(new_df_validated).repartition(1)

            # validate the combined data
            logger.info(
                f"Validating {self.table_name} table after adding {len(obj)} objects"
                )

            validated_df = self._ev.validate.data(
                df=combined_df.cache(),
                table_schema=self.schema_func(),
            )
            self._ev.write.to_warehouse(
                source_data=validated_df,
                table_name=self.table_name,
                write_mode=write_mode,
                uniqueness_fields=self.uniqueness_fields
            )
        else:
            # warn the user that some rows in the added data already exist
            matched_count = df_matched.count()
            logger.warning(
                f"{matched_count} rows in the added data already exist in the "
                f"{self.table_name} table. Skipping these duplicates. "
                )
            # Include a warning detailing which values are duplicates
            matched_values = df_matched.select(
                self.uniqueness_fields
                ).distinct()
            matched_values_list = matched_values.collect()
            matched_values_str = "; ".join(
                [
                    ", ".join(
                        f"{col}={row[col]}" for col in self.uniqueness_fields
                    )
                    for row in matched_values_list
                ]
            )
            logger.warning(
                f"Duplicate values in {self.uniqueness_fields}: "
                f"{matched_values_str}"
            )
            # add data that is not already in the original table
            new_df_not_matched = new_df_validated.join(
                org_df, on=self.uniqueness_fields, how="left_anti"
            )
            logger.info(
                f"Adding {new_df_not_matched.count()} new objects to "
                f"{self.table_name} table"
            )
            combined_df = org_df.unionByName(new_df_not_matched).repartition(1)
            # validate the combined data
            logger.info(
                f"Validating {self.table_name} table after adding "
                f"{new_df_not_matched.count()} new objects"
            )
            validated_df = self._ev.validate.data(
                df=combined_df.cache(),
                table_schema=self.schema_func(),
            )

            self._ev.write.to_warehouse(
                source_data=validated_df,
                table_name=self.table_name,
                write_mode=write_mode,
                uniqueness_fields=self.uniqueness_fields
            )

    def query(
        self,
        filters: Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]
        ] = None,
        order_by: Union[str, StrEnum, List[Union[str, StrEnum]]] = None,
    ):
        """Run a query against the table with filters and order_by.

        In general a user will either use the query methods or the filter and
        order_by methods.  The query method is a convenience method that will
        apply filters and order_by in a single call.

        Parameters
        ----------
        filters : Union[
                str, dict, TableFilter,
                List[Union[str, dict, TableFilter]]
            ]
            The filters to apply to the query.  The filters can be an SQL string,
            dictionary, TableFilter or a list of any of these. The filters
            will be applied in the order they are provided.
        order_by : Union[str, List[str], StrEnum, List[StrEnum]]
            The fields to order the query by.  The fields can be a string,
            StrEnum or a list of any of these.  The fields will be ordered in
            the order they are provided.

        Returns
        -------
        self : BaseTable or subclass of BaseTable

        Examples
        --------
        Filters as dictionaries:

        >>> ts_df = ev.table(table_name="primary_timeseries").query(
        >>>     filters=[
        >>>         {
        >>>             "column": "value_time",
        >>>             "operator": ">",
        >>>             "value": "2022-01-01",
        >>>         },
        >>>         {
        >>>             "column": "value_time",
        >>>             "operator": "<",
        >>>             "value": "2022-01-02",
        >>>         },
        >>>         {
        >>>             "column": "location_id",
        >>>             "operator": "=",
        >>>             "value": "gage-C",
        >>>         },
        >>>     ],
        >>>     order_by=["location_id", "value_time"]
        >>> ).to_pandas()

        Filters as SQL strings:

        >>> ts_df = ev.table(table_name="primary_timeseries").query(
        >>>     filters=[
        >>>         "value_time > '2022-01-01'",
        >>>         "value_time < '2022-01-02'",
        >>>         "location_id = 'gage-C'"
        >>>     ],
        >>>     order_by=["location_id", "value_time"]
        >>> ).to_pandas()

        Filters as FilterBaseModels:

        >>> from teehr.models.filters import TimeseriesFilter
        >>> from teehr.models.filters import FilterOperators

        >>> fields = ev.table(table_name="primary_timeseries").field_enum()
        >>> ts_df = ev.table(table_name="primary_timeseries").query(
        >>>     filters=[
        >>>         TimeseriesFilter(
        >>>             column=fields.value_time,
        >>>             operator=FilterOperators.gt,
        >>>             value="2022-01-01",
        >>>         ),
        >>>         TimeseriesFilter(
        >>>             column=fields.value_time,
        >>>             operator=FilterOperators.lt,
        >>>             value="2022-01-02",
        >>>         ),
        >>>         TimeseriesFilter(
        >>>             column=fields.location_id,
        >>>             operator=FilterOperators.eq,
        >>>             value="gage-C",
        >>>         ),
        >>> ]).to_pandas()
        """
        logger.info("Performing the query.")
        self._check_load_table()
        if filters is not None:
            self.sdf = self._read.from_warehouse(
                catalog_name=self.catalog_name,
                namespace_name=self.table_namespace_name,
                table_name=self.table_name,
                filters=filters,
                validate_filter_field_types=self.validate_filter_field_types,
            ).to_sdf()

        if order_by is not None:
            logger.debug(f"Ordering the metrics by: {order_by}.")
            self.sdf = order_df(self.sdf, order_by)
        return self

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        raise NotImplementedError(
            "The to_geopandas() method is not implemented for Domain Tables"
            " because they do not contain location information."
        )

    def load_dataframe(
        self,
        df: Union[pd.DataFrame, ps.DataFrame],
        namespace_name: str = None,
        catalog_name: str = None,
        field_mapping: dict = None,
        constant_field_values: dict = None,
        write_mode: TableWriteEnum = TableWriteEnum.append,
        drop_duplicates: bool = True,
    ):
        """Load data from an in-memory dataframe.

        Parameters
        ----------
        df : Union[pd.DataFrame, ps.DataFrame]
            DataFrame or GeoDataFrame to load into the table.
        namespace_name : str, optional
            The namespace name to write to. If None, uses the
            active catalog's namespace.
        catalog_name : str, optional
            The catalog name to write to. If None, uses the
            active catalog's catalog name.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}.
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
        """ # noqa
        self._load.dataframe(
            df=df,
            table_name=self.table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates,
        )
        self._load_table()
