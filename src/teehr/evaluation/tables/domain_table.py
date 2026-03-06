"""Domain table class."""
from teehr.evaluation.tables.base_table import BaseTable
from teehr.models.pydantic_table_models import TableBaseModel
from teehr.models.filters import TableFilter
from teehr.models.str_enum import StrEnum
import pandas as pd
from typing import List, Union
import logging


logger = logging.getLogger(__name__)


class DomainTable(BaseTable):
    """Domain table class.

    Domain tables store reference data (units, variables, configurations,
    attributes) that other tables reference via foreign keys.
    """

    # Common defaults for all domain tables
    strict_validation = True
    validate_filter_field_types = True
    foreign_keys = None
    extraction_func = None

    def __init__(
        self,
        ev,
        table_name: str = None,
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None
    ):
        """Initialize the Table class.

        Parameters
        ----------
        ev : EvaluationBaseModel
            The parent Evaluation instance providing access to Spark session,
            catalogs, and related table operations.
        table_name : str, optional
            The name of the table to operate on.
        namespace_name : Union[str, None], optional
            The namespace containing the table. If None, uses the
            active catalog's namespace.
        catalog_name : Union[str, None], optional
            The catalog containing the table. If None, uses the
            active catalog name.
        """
        super().__init__(ev, table_name, namespace_name, catalog_name)

    def _add(
        self,
        obj: Union[TableBaseModel, List[TableBaseModel]],
        write_mode: str = "append"
    ):
        # logger.info(f"Adding attribute to {self.dir}")
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
        new_df_validated = self._ev.validate.schema(
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

            validated_df = self._ev.validate.schema(
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
            validated_df = self._ev.validate.schema(
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
        """Run a query with filters and ordering.

        See :meth:`DataFrameBase.query` for full documentation.

        Note
        ----
        The ``group_by`` and ``include_metrics`` parameters are not
        available for domain tables.
        """
        super().query(filters=filters, order_by=order_by)

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        raise NotImplementedError(
            "The to_geopandas() method is not implemented for Domain Tables"
            " because they do not contain location information."
        )

    def add_geometry(self):
        """Add geometry to the DataFrame."""
        raise NotImplementedError(
            "The add_geometry() method is not implemented for Domain Tables"
            " because they do not contain location information."
        )
