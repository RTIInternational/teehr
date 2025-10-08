"""Base class to represent generic tables."""
from typing import List, Dict, Union
import logging
import geopandas as gpd

from teehr.models.str_enum import StrEnum
from teehr.querying.utils import order_df, join_geometry, df_to_gdf
from teehr.models.evaluation_base import EvaluationBase
from teehr.models.filters import FilterBaseModel
from teehr.models.table_properties import TBLPROPERTIES


logger = logging.getLogger(__name__)


class Table:
    """Base class to represent generic tables."""

    def __init__(self, ev: EvaluationBase):
        """Initialize the Table class."""
        self._ev = ev
        self._read = ev.read
        # self._write = ev.write
        # self._validate = ev.validate
        # self._extract = ev.extract

    def __call__(
        self,
        table_name: str,
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ) -> "Table":
        """Initialize the Table class."""
        self.table_name = table_name
        self.sdf = None

        if namespace_name is None:
            self.table_namespace_name = self._ev.active_catalog.namespace_name
        else:
            self.table_namespace_name = namespace_name
        if catalog_name is None:
            self.catalog_name = self._ev.active_catalog.catalog_name
        else:
            self.catalog_name = catalog_name

        if table_name in TBLPROPERTIES:
            # Just set self.table_properties here?
            table_props = TBLPROPERTIES[self.table_name]
            self.uniqueness_fields: List[str] = table_props["uniqueness_fields"]
            self.foreign_keys: List[Dict[str, str]] = table_props["foreign_keys"]
            self.schema_func = table_props["schema_func"]
            self.filter_model: FilterBaseModel = table_props["filter_model"]
            self.strict_validation = table_props["strict_validation"]
            self.validate_filter_field_types = table_props["validate_filter_field_types"]
        else:
            self.uniqueness_fields: List[str] = None
            self.foreign_keys: List[Dict[str, str]] = None
            self.schema_func = None
            self.filter_model: FilterBaseModel = None
            self.strict_validation = None
            self.validate_filter_field_types = None

        return self

    def _load_table(self):
        """Load the table from the directory to self.sdf.

        Parameters
        ----------
        **kwargs
            Additional options to pass to the spark read method.
        """
        logger.info(
            f"Loading files from {self.catalog_name}."
            f"{self.table_namespace_name}."
            f"{self.table_name}."
        )
        self.sdf = self._read.from_warehouse(
            catalog_name=self.catalog_name,
            namespace_name=self.table_namespace_name,
            table_name=self.table_name
        ).to_sdf()

    def _check_load_table(self):
        """Check if the table is loaded.

        If the table is not loaded, try to load it.  If the table is still
        not loaded, raise an error.
        """
        if self.sdf is None:
            self._load_table()
        # if self.sdf is None:
        #     self._raise_missing_table_error(table_name=self.table_name)

    def _get_schema(self, type: str = "pyspark"):
        """Get the primary timeseries schema.

        Parameters
        ----------
        type : str, optional
            The type of schema to return.  Valid values are "pyspark" and
            "pandas".  The default is "pyspark".
        """
        if type == "pandas":
            return self.schema_func(type="pandas")

        return self.schema_func()

    def validate(self, drop_duplicates: bool = True):
        """Validate the dataset table against the schema."""
        self._check_load_table()
        self._ev.validate.schema(
            sdf=self.sdf,
            table_schema=self.schema_func(),
            drop_duplicates=drop_duplicates,
            foreign_keys=self.foreign_keys,
            uniqueness_fields=self.uniqueness_fields,
        )

    def query(
        self,
        filters: Union[
            str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]
        ] = None,
        order_by: Union[str, StrEnum, List[Union[str, StrEnum]]] = None
    ):
        """Run a query against the table with filters and order_by.

        In general a user will either use the query methods or the filter and
        order_by methods.  The query method is a convenience method that will
        apply filters and order_by in a single call.

        Parameters
        ----------
        filters : Union[
                str, dict, FilterBaseModel,
                List[Union[str, dict, FilterBaseModel]]
            ]
            The filters to apply to the query.  The filters can be an SQL string,
            dictionary, FilterBaseModel or a list of any of these. The filters
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

        >>> ts_df = ev.primary_timeseries.query(
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

        >>> ts_df = ev.primary_timeseries.query(
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
        >>>
        >>> fields = ev.primary_timeseries.field_enum()
        >>> ts_df = ev.primary_timeseries.query(
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
            self.sdf = order_df(self.sdf, order_by)
        return self

    def filter(
        self,
        filters: Union[
            str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]
        ]
    ):
        """Apply a filter.

        Parameters
        ----------
        filters : Union[
                str, dict, FilterBaseModel,
                List[Union[str, dict, FilterBaseModel]]
            ]
            The filters to apply to the query.  The filters can be an SQL string,
            dictionary, FilterBaseModel or a list of any of these.

        Returns
        -------
        self : BaseTable or subclass of BaseTable

        Examples
        --------
        Note: The filter method is universal for all table types. When
        repurposing this example, ensure filter arguments (e.g., column names,
        values) are valid for the specific table type.

        Filters as dictionary:

        >>> ts_df = ev.primary_timeseries.filter(
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
        >>>     ]
        >>> ).to_pandas()

        Filters as string:

        >>> ts_df = ev.primary_timeseries.filter(
        >>>     filters=[
        >>>         "value_time > '2022-01-01'",
        >>>         "value_time < '2022-01-02'",
        >>>         "location_id = 'gage-C'"
        >>>     ]
        >>> ).to_pandas()

        Filters as FilterBaseModel:

        >>> from teehr.models.filters import TimeseriesFilter
        >>> from teehr.models.filters import FilterOperators
        >>>
        >>> fields = ev.primary_timeseries.field_enum()
        >>> ts_df = ev.primary_timeseries.filter(
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
        logger.info(f"Setting filter {filter}.")
        self._check_load_table()
        self.sdf = self._read.from_warehouse(
            catalog_name=self.catalog_name,
            namespace_name=self.table_namespace_name,
            table_name=self.table_name,
            filters=filters,
            validate_filter_field_types=self.validate_filter_field_types,
        ).to_sdf()
        return self

    def order_by(
        self,
        fields: Union[str, StrEnum, List[Union[str, StrEnum]]]
    ):
        """Apply an order_by.

        Parameters
        ----------
        fields : Union[str, StrEnum, List[Union[str, StrEnum]]]
            The fields to order the query by.  The fields can be a string,
            StrEnum or a list of any of these.  The fields will be ordered in
            the order they are provided.

        Returns
        -------
        self : BaseTable or subclass of BaseTable

        Examples
        --------
        Order by string:

        >>> ts_df = ev.primary_timeseries.order_by("value_time").to_df()

        Order by StrEnum:

        >>> from teehr.querying.field_enums import TimeseriesFields
        >>> ts_df = ev.primary_timeseries.order_by(
        >>>     TimeseriesFields.value_time
        >>> ).to_pandas()
        """
        logger.info(f"Setting order_by {fields}.")
        self._check_load_table()
        self.sdf = order_df(self.sdf, fields)
        return self

    def fields(self) -> List[str]:
        """Return table columns as a list."""
        self._check_load_table()
        return self.sdf.columns

    def distinct_values(self,
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
        """
        self._check_load_table()
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

    # def field_enum(self) -> StrEnum:
    #     """Get the fields enum."""
    #     raise NotImplementedError("field_enum method must be implemented.")

    def to_pandas(self):
        """Return Pandas DataFrame."""
        self._check_load_table()
        df = self.sdf.toPandas()
        df.attrs['table_type'] = self.table_name
        df.attrs['fields'] = self.fields()
        return df

    def _join_geometry_using_crosswalk(self):
        """Join geometry."""
        logger.debug("Joining locations geometry via the crosswalk.")
        joined_df = self._ev.sql("""
            SELECT
                sf.*,
                lf.geometry as geometry
            FROM secondary_timeseries sf
            JOIN location_crosswalks cf
                on cf.secondary_location_id = sf.location_id
            JOIN locations lf
                on cf.primary_location_id = lf.id
        """,
        create_temp_views=["secondary_timeseries", "location_crosswalks", "locations"])
        return df_to_gdf(joined_df.toPandas())

    def to_geopandas(self) -> gpd.GeoDataFrame:
        """Convert the DataFrame to a GeoPandas DataFrame."""
        self._check_load_table()
        if "location_id" not in self.sdf.columns:
            err_msg = "The location_id field was not found in the table."
            logger.error(err_msg)
            raise ValueError(err_msg)
        locations_sdf = self._read.from_warehouse(
            catalog_name=self.catalog_name,
            namespace_name=self.table_namespace_name,
            table_name="locations"
        ).to_sdf()
        if self.table_name == "secondary_timeseries":
            return self._join_geometry_using_crosswalk()
        return join_geometry(
            self.sdf,
            locations_sdf,
            "location_id"
        )

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
        self._check_load_table()
        return self.sdf