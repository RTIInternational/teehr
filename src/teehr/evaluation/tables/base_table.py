"""Base class for all tables."""
from typing import List, Dict, Union
import logging

from teehr.evaluation.dataframe_base import DataFrameBase
from teehr.querying.utils import join_geometry
from teehr.models.evaluation_base import EvaluationBase
from teehr.models.filters import TableFilter
from teehr.models.table_properties import TBLPROPERTIES
import pyspark.sql as ps


logger = logging.getLogger(__name__)


class BaseTable(DataFrameBase):
    """Base class inherited by all table classes.

    Tables represent persisted iceberg data that is read from storage.
    """

    def __init__(
        self,
        ev: EvaluationBase,
        table_name: str = None,
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None
    ):
        """Initialize the Table class.

        Parameters
        ----------
        ev : EvaluationBase
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
        self._read = ev.read
        self.uniqueness_fields: List[str] = None
        self.foreign_keys: List[Dict[str, str]] = None
        self.schema_func = None
        self.filter_model: TableFilter = None
        self.strict_validation = None
        self.validate_filter_field_types = None
        self.field_enum_model = None
        self.extraction_func = None
        self.table_name = None
        self.table_namespace_name = None
        self.catalog_name = None

        # Initialize for specific table if table_name provided
        if table_name is not None:
            self._initialize_table(table_name, namespace_name, catalog_name)

    @property
    def sdf(self) -> ps.DataFrame:
        """Get the Spark DataFrame."""
        return self._sdf

    @sdf.setter
    def sdf(self, value: ps.DataFrame):
        """Set the Spark DataFrame."""
        self._sdf = value

    def _initialize_table(
        self,
        table_name: str,
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None
    ):
        """Initialize the table properties for a specific table.

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
        self.table_name = table_name
        self._sdf = None

        if namespace_name is None:
            self.table_namespace_name = self._ev.active_catalog.namespace_name
        else:
            self.table_namespace_name = namespace_name
        if catalog_name is None:
            self.catalog_name = self._ev.active_catalog.catalog_name
        else:
            self.catalog_name = catalog_name

        tbl_props = TBLPROPERTIES.get(table_name)
        if tbl_props is None:
            logger.info(
                f"No table properties found for table: '{table_name}'."
                " Proceeding without table properties."
            )
        else:
            self.uniqueness_fields = tbl_props.get("uniqueness_fields")
            self.foreign_keys = tbl_props.get("foreign_keys")
            self.schema_func = tbl_props.get("schema_func")
            self.filter_model = tbl_props.get("filter_model")
            self.strict_validation = tbl_props.get("strict_validation")
            self.validate_filter_field_types = tbl_props.get(
                "validate_filter_field_types"
            )
            self.field_enum_model = tbl_props.get("field_enum_model")
            self.extraction_func = tbl_props.get("extraction_func")

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
        """Load the table from the warehouse to self.sdf."""
        logger.info(
            f"Loading files from {self.catalog_name}."
            f"{self.table_namespace_name}."
            f"{self.table_name}."
        )
        self._sdf = self._read.from_warehouse(
            catalog_name=self.catalog_name,
            namespace_name=self.table_namespace_name,
            table_name=self.table_name
        ).to_sdf()

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

    def _get_schema(self, type: str = "pyspark"):
        """Get the table schema.

        Parameters
        ----------
        type : str, optional
            The type of schema to return. Valid values are "pyspark" and
            "pandas". Default is "pyspark".
        """
        if type == "pandas":
            return self.schema_func(type="pandas")
        return self.schema_func()

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
        self._ev.validate.schema(
            sdf=self.sdf,
            table_schema=self.schema_func(),
            drop_duplicates=drop_duplicates,
            foreign_keys=self.foreign_keys,
            uniqueness_fields=self.uniqueness_fields,
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
            prefixes of the locations will be returned. Only compatible with
            primary_timeseries, secondary_timeseries, joined_timeseries,
            locations, location_attributes, and location_crosswalk tables and
            their respective location columns.
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
        if column not in self.sdf.columns:
            raise ValueError(
                f"Invalid column: '{column}' for table: '{self.table_name}'"
            )
        if location_prefixes:
            # ensure valid table
            valid_tables = ['primary_timeseries',
                            'secondary_timeseries',
                            'joined_timeseries',
                            'locations',
                            'location_attributes',
                            'location_crosswalks']
            if self.table_name not in valid_tables:
                raise ValueError(
                    f"""
                    Invalid table: '{self.table_name}' with argument
                    location_prefixes==True. Valid tables are: {valid_tables}
                    """
                    )
            # ensure valid columns for selected table
            valid_columns = {'primary_timeseries': ['location_id'],
                             'secondary_timeseries': ['location_id'],
                             'joined_timeseries': ['primary_location_id',
                                                   'secondary_location_id'],
                             'locations': ['id'],
                             'location_attributes': ['location_id'],
                             'location_crosswalks': ['primary_location_id',
                                                     'secondary_location_id']
                             }
            if column not in valid_columns[self.table_name]:
                raise ValueError(
                    f"""
                    Invalid column: '{column}' for table: '{self.table_name}' with
                    argument location_prefixes==True. Valid columns are:
                    {valid_columns[self.table_name]}
                    """
                )
            # get unique location prefixes
            unique_locations = self.sdf.select(column).distinct().rdd.flatMap(
                lambda x: x
                ).collect()
            prefixes = [location.split('-')[0] for location
                        in unique_locations
                        ]
            return list(set(prefixes))

        else:
            return self.sdf.select(column).distinct().rdd.flatMap(
                lambda x: x
                ).collect()

    def to_pandas(self):
        """Return Pandas DataFrame."""
        df = super().to_pandas()
        # df.attrs['table_type'] = self.table_name
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        gdf = join_geometry(self.sdf, self._ev.locations.to_sdf())
        # gdf.attrs['table_type'] = self.__class__.__name__
        # gdf.attrs['fields'] = self.to_sdf().columns
        return gdf
