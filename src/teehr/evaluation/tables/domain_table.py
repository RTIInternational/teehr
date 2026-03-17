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
        write_mode: str = "upsert"
    ):
        """Add a record or list of records to the table.

        This method is intended to be called by the public add() method of child classes,
        which should handle any table-specific logic before calling this method to perform
        the actual addition of records to the warehouse.

        Parameters
        ----------
        obj : Union[TableBaseModel, List[TableBaseModel]]
            The record(s) to add to the table. Can be a single Pydantic model
            instance or a list of instances.
        write_mode : str, optional
            The write mode to use when writing to the warehouse. Defaults to "upsert".
            Other options may include "append", "overwrite", etc., depending on the underlying
            warehouse implementation.
        """
        if issubclass(type(obj), TableBaseModel):
            obj = [obj]

        # validate the data to be added
        sdf = self._ev.spark.createDataFrame(
            pd.DataFrame([o.model_dump() for o in obj])
        )

        validated_df = self._ev.validate.schema(
            df=sdf.cache(),
            table_schema=self.schema_func(),
        )
        self._ev.write.to_warehouse(
            source_data=validated_df,
            table_name=self.table_name,
            catalog_name=self.catalog_name,
            namespace_name=self.namespace_name,
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
