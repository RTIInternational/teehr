"""Domain table class."""
from teehr.evaluation.tables.base_table import BaseTable
from teehr.models.pydantic_table_models import TableBaseModel
from teehr.loading.utils import single_file_to_dataframe
import pandas as pd
from typing import List, Union
from pathlib import Path
import logging
from teehr.loading.utils import (
    validate_input_is_csv,
    validate_input_is_parquet
)


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
    extraction_func = staticmethod(single_file_to_dataframe)

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

        validated_df = self.ev._validate.dataframe(
            df=sdf,
            table_schema=self.schema_func(),
            strict=self.strict_validation,
            add_missing_columns=True,
            drop_duplicates=False,
        )

        self._ev._write.to_warehouse(
            source_data=validated_df,
            table_name=self.table_name,
            catalog_name=self.catalog_name,
            namespace_name=self.namespace_name,
            write_mode=write_mode,
            uniqueness_fields=self.uniqueness_fields
        )

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

    def load_parquet(
        self,
        in_path: Union[Path, str],
        namespace_name: str = None,
        catalog_name: str = None,
        extraction_function: callable = None,
        pattern: str = "**/*.parquet",
        field_mapping: dict = None,
        write_mode: str = "append",
        drop_duplicates: bool = True,
        **kwargs
    ):
        """Import location attributes from parquet file format.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            Parquet file format.
        namespace_name : str, optional
            The namespace name to write to, by default None, which means the
            namespace_name of the active catalog is used.
        catalog_name : str, optional
            The catalog name to write to, by default None, which means the
            catalog_name of the active catalog is used.
        extraction_function : callable, optional
            A custom function to extract and transform the data from the input
            files to the TEEHR data model. If None (default), uses the table's
            default extraction function.
        pattern : str, optional
            The glob pattern to use when searching for files in a directory.
            Default is '**/*.parquet' to search for all parquet files recursively.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        write_mode : str, optional (default: "append")
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
        **kwargs
            Additional keyword arguments are passed to pd.read_csv()
            or pd.read_parquet().

        Notes
        -----
        The TEEHR Location Crosswalk table schema includes fields:

        - primary_location_id
        - secondary_location_id
        """
        validate_input_is_parquet(in_path)
        extraction_function = extraction_function or self.extraction_func
        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name
        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name

        self._load.file(
            in_path=in_path,
            pattern=pattern,
            table_name=self.table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            extraction_function=extraction_function,
            field_mapping=field_mapping,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates,
            **kwargs
        )
        self._load_sdf()

    def load_csv(
        self,
        in_path: Union[Path, str],
        namespace_name: str = None,
        catalog_name: str = None,
        extraction_function: callable = None,
        pattern: str = "**/*.csv",
        field_mapping: dict = None,
        write_mode: str = "append",
        drop_duplicates: bool = True,
        **kwargs
    ):
        """Import location attributes from CSV file format.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            CSV file format.
        namespace_name : str, optional
            The namespace name to write to, by default None, which means the
            namespace_name of the active catalog is used.
        catalog_name : str, optional
            The catalog name to write to, by default None, which means the
            catalog_name of the active catalog is used.
        extraction_function : callable, optional
            A custom function to extract and transform the data from the input
            files to the TEEHR data model. If None (default), uses the table's
            default extraction function.
        pattern : str, optional
            The glob pattern to use when searching for files in a directory.
            Default is '**/*.csv' to search for all CSV files recursively.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        write_mode : str, optional (default: "append")
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
        **kwargs
            Additional keyword arguments are passed to pd.read_csv()
            or pd.read_parquet().

        Notes
        -----
        The TEEHR Location Crosswalk table schema includes fields:

        - primary_location_id
        - secondary_location_id
        """ # noqa
        validate_input_is_csv(in_path)
        extraction_function = extraction_function or self.extraction_func
        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name
        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name

        self._load.file(
            in_path=in_path,
            pattern=pattern,
            table_name=self.table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            extraction_function=extraction_function,
            field_mapping=field_mapping,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates,
            **kwargs
        )
        self._load_sdf()
