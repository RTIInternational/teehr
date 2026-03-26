"""Base class for all tables."""
from typing import List, Dict, Union, Callable
import logging

import pyspark.sql as ps
import pandas as pd

from teehr.evaluation.dataframe_base import TeehrDataFrameBase
from teehr.models.evaluation_base import EvaluationBaseModel
from teehr.models.filters import TableFilter
from teehr.models.table_enums import TableNamesEnum
from pyspark.sql.functions import split, col


logger = logging.getLogger(__name__)


class BaseTable(TeehrDataFrameBase):
    """Base class inherited by all table classes.

    Tables represent persisted iceberg data that is read from storage.

    Subclasses should define class-level attributes for table metadata:
        - table_name: str - The name of the table
        - uniqueness_fields: List[str] - Fields that uniquely identify a row
        - foreign_keys: List[Dict] - Foreign key constraints
        - schema_func: Callable - Function returning the table schema
        - strict_validation: bool - Whether to use strict validation
        - validate_filter_field_types: bool - Whether to validate filter types
        - extraction_func: Callable - Function to extract/transform input data
    """

    # Class-level defaults (None for generic/unknown tables)
    table_name: str = None
    uniqueness_fields: List[str] = None
    foreign_keys: List[Dict[str, str]] = None
    schema_func: Callable = None
    strict_validation: bool = None
    validate_filter_field_types: bool = None
    extraction_func: Callable = None
    primary_location_id_field: str = None
    secondary_location_id_field: str = None

    def __init__(
        self,
        ev: EvaluationBaseModel,
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
            The name of the table to operate on. If provided, the table
            will be initialized for this specific table.
        namespace_name : Union[str, None], optional
            The namespace containing the table. If None, uses the
            active catalog's namespace.
        catalog_name : Union[str, None], optional
            The catalog containing the table. If None, uses the
            active catalog name.
        """
        super().__init__(ev)
        self._read = ev._read
        self._load = ev._load

        # Instance-level attributes for namespace/catalog
        self.namespace_name = None
        self.catalog_name = None

        # Initialize for specific table if table_name provided
        # Use class-level table_name if not provided
        effective_table_name = table_name or self.__class__.table_name
        if effective_table_name is not None:
            self._initialize_table(
                effective_table_name,
                namespace_name,
                catalog_name
            )

    def _initialize_table(
        self,
        table_name: str,
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None
    ):
        """Initialize the table.

        Initialize for a specific table name, namespace, and catalog.

        Parameters
        ----------
        table_name : str
            The name of the table to operate on.
        namespace_name : Union[str, None], optional
            The namespace containing the table. If None, uses the
            active catalog's namespace.
        catalog_name : Union[str, None], optional
            The catalog containing the table. If None, uses the
            active catalog name.
        """
        logger.info(
            f"Initializing Table for table: {table_name}"
            f".{namespace_name or ''}"
            f"{'.' if namespace_name else ''}{catalog_name or ''}"
        )
        # Override class-level table_name if provided
        self.table_name = table_name
        self._sdf = None

        if namespace_name is None:
            self.namespace_name = self._ev.active_catalog.namespace_name
        else:
            self.namespace_name = namespace_name

        if catalog_name is None:
            self.catalog_name = self._ev.active_catalog.catalog_name
        else:
            self.catalog_name = catalog_name

        # Load the table (lazy Spark reference - no data read until action)
        # Table may not exist yet (e.g., before data is loaded), so catch error
        try:
            self._load_sdf()
        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                logger.debug(
                    f"Table '{table_name}' does not exist yet. "
                    "sdf will be set after data is loaded."
                )
                self._sdf = None
            else:
                raise

    def _load_sdf(self):
        """Load the table from the warehouse to self._sdf."""
        logger.info(
            f"Loading files from {self.catalog_name}."
            f"{self.namespace_name}."
            f"{self.table_name}."
        )
        self._sdf = self._read.from_warehouse(
            catalog_name=self.catalog_name,
            namespace_name=self.namespace_name,
            table_name=self.table_name
        )

    def _apply_filters(
        self,
        filters: Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]
        ],
        validate: bool = None
    ):
        """Apply filters to the DataFrame.

        Overrides base class to use table-specific validation setting.

        Parameters
        ----------
        filters : Union[str, dict, TableFilter, List[...]]
            The filters to apply.
        validate : bool, optional
            Whether to validate filter field types. If None, uses
            self.validate_filter_field_types.
        """
        if validate is None:
            validate = self.validate_filter_field_types or False
        super()._apply_filters(filters, validate=validate)

    def validate(self, drop_duplicates: bool = True):
        """Validate the dataset table against the schema.

        Parameters
        ----------
        drop_duplicates : bool, optional
            Whether to drop duplicates based on the uniqueness fields.
            Default is True.

        Examples
        --------
        Validate a table:

        >>> ev.table(
        >>>     table_name="primary_timeseries"
        >>> ).validate(drop_duplicates=True)
        """
        self._ev._validate.dataframe(
            df=self.to_sdf(),
            table_schema=self.schema_func(),
            drop_duplicates=drop_duplicates,
            foreign_keys=self.foreign_keys,
            uniqueness_fields=self.uniqueness_fields,
        )

    @property
    def is_core_table(self) -> bool:
        """Return True if this table is a core (built-in) TEEHR table.

        Core tables (e.g., primary_timeseries, locations, units) are part of
        the standard TEEHR schema and cannot be dropped. User-created tables
        (e.g., materialized views or saved query results) are not core tables
        and can be dropped.

        Returns
        -------
        bool
            True if the table is a core TEEHR table, False otherwise.
        """
        return self.table_name in [e.value for e in TableNamesEnum]

    def drop(self):
        """Drop this table from the catalog.

        Only non-core tables (user-created tables, materialized views, saved
        query results) can be dropped. Attempting to drop a core table
        (e.g., primary_timeseries, locations, units) will raise a ValueError.

        Raises
        ------
        ValueError
            If the table is a core TEEHR table.

        Examples
        --------
        Write and then drop a user-created table:

        >>> ev.joined_timeseries_view().write("my_results")
        >>> ev.table("my_results").drop()
        """
        if self.is_core_table:
            raise ValueError(
                f"Cannot drop core table '{self.table_name}'. "
                "Only user-created tables (e.g., materialized views or "
                "saved query results) can be dropped."
            )
        logger.info(
            f"Dropping table: {self.catalog_name}."
            f"{self.namespace_name}.{self.table_name}"
        )
        self._ev.sql(
            f"DROP TABLE IF EXISTS "
            f"{self.catalog_name}.{self.namespace_name}.{self.table_name}"
        )

    def delete(
        self,
        filters: Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]
        ] = None,
        dry_run: bool = False,
    ) -> Union[int, ps.DataFrame]:
        """Delete rows from this table based on filter conditions.

        Delegates to
        :meth:`Write.delete_from() <teehr.evaluation.write.Write.delete_from>`.

        Parameters
        ----------
        filters : Union[str, dict, TableFilter, List[...]], optional
            Filter conditions specifying which rows to delete.
            Supports SQL strings, dictionaries, or
            :class:`~teehr.models.filters.TableFilter` objects.
            If ``None``, all rows in the table will be deleted.
        dry_run : bool, optional
            If ``True``, returns a Spark DataFrame of rows that would be
            deleted without performing the actual deletion. Default is
            ``False``.

        Returns
        -------
        int or ps.DataFrame
            If ``dry_run=False``, returns the number of rows deleted (int).
            If ``dry_run=True``, returns a Spark DataFrame of rows that
            would be deleted.

        Examples
        --------
        Preview rows that would be deleted (dry run):

        >>> sdf = ev.table("primary_timeseries").delete(
        >>>     filters=["location_id = 'usgs-01234567'"],
        >>>     dry_run=True,
        >>> )
        >>> print(f"Rows to delete: {sdf.count()}")

        Delete rows and get the count:

        >>> count = ev.table("primary_timeseries").delete(
        >>>     filters=["location_id = 'usgs-01234567'"],
        >>> )
        >>> print(f"Deleted {count} rows.")

        Delete all rows from this table:

        >>> count = ev.primary_timeseries.delete()
        """
        return self._ev._write.delete_from(
            table_name=self.table_name,
            filters=filters,
            catalog_name=self.catalog_name,
            namespace_name=self.namespace_name,
            dry_run=dry_run,
        )

    def distinct_values(
        self,
        column: str,
        location_prefixes: bool = False
    ) -> List[str]:
        """Return distinct values for a column.

        Parameters
        ----------
        column : str
            The column to get distinct values for.
        location_prefixes : bool
            Whether to return location prefixes. If True, only the unique
            prefixes of the locations will be returned.
            Default: False

        Returns
        -------
        List[str]
            The distinct values for the column.

        Examples
        --------
        Get distinct location IDs from the primary timeseries table:

        >>> ev.table(table_name="primary_timeseries").distinct_values(
        >>>     column='location_id',
        >>>     location_prefixes=False
        >>> )

        Get distinct location prefixes from the joined timeseries table:

        >>> ev.table(table_name="joined_timeseries").distinct_values(
        >>>     column='primary_location_id',
        >>>     location_prefixes=True
        >>> )
        """
        sdf = self.to_sdf()
        if column not in sdf.columns:
            raise ValueError(
                f"Invalid column: '{column}' for table: '{self.table_name}'"
            )
        if location_prefixes:
            # Split in Spark, then distinct, then collect
            prefixes_df = sdf.select(
                split(col(column), '-').getItem(0).alias('prefix')
            ).distinct()
            return [row.prefix for row in prefixes_df.collect()]

        else:
            unique_values_df = sdf.select(column).distinct()
            return [row[column] for row in unique_values_df.collect()]

    def load_dataframe(
        self,
        df: Union[pd.DataFrame, ps.DataFrame],
        namespace_name: str = None,
        catalog_name: str = None,
        field_mapping: dict = None,
        constant_field_values: dict = None,
        write_mode: str = "append",
        drop_duplicates: bool = True,
    ):
        """Import data from an in-memory dataframe.

        Parameters
        ----------
        df : Union[pd.DataFrame, ps.DataFrame]
            DataFrame to load into the table.
        namespace_name : str, optional
            The namespace name to write to, by default None, which means the
            namespace_name of the active catalog is used.
        catalog_name : str, optional
            The catalog name to write to, by default None, which means the
            catalog_name of the active catalog is used.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}.
        write_mode : str, optional (default: "append")
            The write mode for the table. Options:

            - ``"insert"``: Insert all rows directly without duplicate checking.
            - ``"append"``: Insert new rows; skip rows matching uniqueness fields.
            - ``"upsert"``: Insert new rows; update existing rows matching
              uniqueness fields.
            - ``"overwrite"``: Replace all data, preserving table history.
            - ``"create_or_replace"``: Drop and recreate table. Loses history.

        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the DataFrame during validation.
        """ # noqa
        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name
        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name

        self._load.dataframe(
            df=df,
            table_name=self.table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates
        )
        self._load_sdf()
