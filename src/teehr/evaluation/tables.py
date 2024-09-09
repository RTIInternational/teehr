"""Access methods to units table."""
from teehr.models.str_enum import StrEnum
from teehr.querying.filter_format import validate_and_apply_filters
import pyspark.sql as ps
from typing import List, Union
from teehr.querying.utils import df_to_gdf, order_df, join_geometry
from teehr.models.tables import (
    TableBaseModel,
    Unit,
    Variable,
    Attribute,
    Configuration,
    Location,
    LocationAttribute,
    LocationCrosswalk,
    Timeseries
)
from teehr.models.filters import (
    FilterBaseModel,
    UnitFilter,
    VariableFilter,
    AttributeFilter,
    ConfigurationFilter,
    LocationFilter,
    LocationAttributeFilter,
    LocationCrosswalkFilter,
    TimeseriesFilter,
    JoinedTimeseriesFilter
)
from teehr.models.table_enums import (
    UnitFields,
    VariableFields,
    ConfigurationFields,
    AttributeFields,
    LocationFields,
    LocationAttributeFields,
    LocationCrosswalkFields,
    TimeseriesFields,
    JoinedTimeseriesFields
)

import logging

logger = logging.getLogger(__name__)


class BaseTable():
    """Base query class."""

    def __init__(self, eval):
        """Initialize class."""
        self.dir = None
        self.eval = eval
        self.spark = eval.spark
        self.table_model: TableBaseModel = None
        self.filter_model: FilterBaseModel = None
        self.validate_filter_field_types = True
        self.df: ps.DataFrame = None

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
            The filters to apply to the query.  The filters can be a string,
            dictionary, FilterBaseModel or a list of any of these.  The filters

        order_by : Union[str, List[str], StrEnum, List[StrEnum]]
            The fields to order the query by.  The fields can be a string,
            StrEnum or a list of any of these.  The fields will be ordered in
            the order they are provided.

        Returns
        -------
        self : BaseTable or subclass of BaseTable

        Examples
        --------
        Filters as dictionary
        >>> ts_df = eval.primary_timeseries.query(
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

        Filters as string
        >>> ts_df = eval.primary_timeseries.query(
        >>>     filters=[
        >>>         "value_time > '2022-01-01'",
        >>>         "value_time < '2022-01-02'",
        >>>         "location_id = 'gage-C'"
        >>>     ],
        >>>     order_by=["location_id", "value_time"]
        >>> ).to_pandas()

        Filters as FilterBaseModel
        >>> from teehr.models.filters import TimeseriesFilter
        >>> from teehr.models.filters import FilterOperators
        >>>
        >>> fields = eval.primary_timeseries.field_enum()
        >>> ts_df = eval.primary_timeseries.query(
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
        if filters is not None:
            self.df = validate_and_apply_filters(
                sdf=self.df,
                filters=filters,
                filter_model=self.filter_model,
                fields_enum=self.field_enum(),
                table_model=self.table_model,
                validate=self.validate_filter_field_types
            )
        if order_by is not None:
            self.df = order_df(self.df, order_by)
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
            The filters to apply to the query.  The filters can be a string,
            dictionary, FilterBaseModel or a list of any of these.

        Returns
        -------
        self : BaseTable or subclass of BaseTable

        Examples
        --------
        Filters as dictionary
        >>> ts_df = eval.primary_timeseries.filter(
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

        Filters as string
        >>> ts_df = eval.primary_timeseries.filter(
        >>>     filters=[
        >>>         "value_time > '2022-01-01'",
        >>>         "value_time < '2022-01-02'",
        >>>         "location_id = 'gage-C'"
        >>>     ]
        >>> ).to_pandas()

        Filters as FilterBaseModel
        >>> from teehr.models.filters import TimeseriesFilter
        >>> from teehr.models.filters import FilterOperators
        >>>
        >>> fields = eval.primary_timeseries.field_enum()
        >>> ts_df = eval.primary_timeseries.filter(
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
        self.df = validate_and_apply_filters(
            sdf=self.df,
            filters=filters,
            filter_model=self.filter_model,
            fields_enum=self.field_enum(),
            table_model=self.table_model
        )
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
        Order by string
        >>> ts_df = eval.primary_timeseries.order_by("value_time").to_df()

        Order by StrEnum
        >>> from teehr.querying.field_enums import TimeseriesFields
        >>> ts_df = eval.primary_timeseries.order_by(
        >>>     TimeseriesFields.value_time
        >>> ).to_pandas()

        """
        self.df = order_df(self.df, fields)
        return self

    def fields(self):
        """Return table columns as a list."""
        return self.df.columns

    def field_enum(self) -> StrEnum:
        """Get the fields enum."""
        raise NotImplementedError("field_enum method must be implemented.")

    def to_pandas(self):
        """Return Pandas DataFrame."""
        return self.df.toPandas()

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        raise NotImplementedError("to_geopandas method must be implemented.")

    def to_sdf(self):
        """Return PySpark DataFrame.

        The PySpark DataFrame can be further processed using PySpark. Note,
        PySpark DataFrames are lazy and will not be executed until an action
        is called.  For example, calling `show()`, `collect()` or toPandas().
        This can be useful for further processing or analysis, for example,

        >>> ts_sdf = eval.primary_timeseries.query(
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
        return self.df


class UnitTable(BaseTable):
    """Access methods to units table."""

    def __init__(self, eval):
        """Initialize class."""
        super().__init__(eval)
        self.dir = eval.units_dir
        self.table_model = Unit
        self.filter_model = UnitFilter

        self.df = (
            self.spark.read.format("csv")
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
            .option("header", True)
            .load(str(self.dir))
        )

    def field_enum(self) -> UnitFields:
        """Get the unit fields enum."""
        fields_list = Unit.get_field_names()
        return UnitFields(
            "UnitFields",
            {field: field for field in fields_list}
        )


class VariableTable(BaseTable):
    """Access methods to variables table."""

    def __init__(self, eval):
        """Initialize class."""
        super().__init__(eval)
        self.dir = eval.variables_dir
        self.table_model = Variable
        self.filter_model = VariableFilter

        self.df = (
            self.spark.read.format("csv")
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
            .option("header", True)
            .load(str(self.dir))
        )

    def field_enum(self) -> VariableFields:
        """Get the variable fields enum."""
        fields_list = Variable.get_field_names()
        return VariableFields(
            "VariableFields",
            {field: field for field in fields_list}
        )


class AttributeTable(BaseTable):
    """Access methods to attributes table."""

    def __init__(self, eval):
        """Initialize class."""
        super().__init__(eval)
        self.dir = eval.attributes_dir
        self.table_model = Attribute
        self.filter_model = AttributeFilter

        self.df = (
            self.spark.read.format("csv")
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
            .option("header", True)
            .load(str(self.dir))
        )

    def field_enum(self) -> AttributeFields:
        """Get the attribute fields enum."""
        fields_list = Attribute.get_field_names()
        return AttributeFields(
            "AttributeFields",
            {field: field for field in fields_list}
        )


class ConfigurationTable(BaseTable):
    """Access methods to configurations table."""

    def __init__(self, eval):
        """Initialize class."""
        super().__init__(eval)
        self.dir = eval.configurations_dir
        self.table_model = Configuration
        self.filter_model = ConfigurationFilter

        self.df = (
            self.spark.read.format("csv")
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
            .option("header", True)
            .load(str(self.dir))
        )

    def field_enum(self) -> ConfigurationFields:
        """Get the configuration fields enum."""
        fields_list = Configuration.get_field_names()
        return ConfigurationFields(
            "ConfigurationFields",
            {field: field for field in fields_list}
        )


class LocationTable(BaseTable):
    """Access methods to locations table."""

    def __init__(self, eval):
        """Initialize class."""
        super().__init__(eval)
        self.dir = eval.locations_dir
        self.table_model = Location
        self.filter_model = LocationFilter

        self.df = (
            self.spark.read.format("parquet")
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
            .option("header", True)
            .load(str(self.dir))
        )

    def field_enum(self) -> LocationFields:
        """Get the location fields enum."""
        fields_list = Location.get_field_names()
        return LocationFields(
            "LocationFields",
            {field: field for field in fields_list}
        )

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        return df_to_gdf(self.to_pandas())


class LocationAttributeTable(BaseTable):
    """Access methods to location attributes table."""

    def __init__(self, eval):
        """Initialize class."""
        super().__init__(eval)
        self.dir = eval.location_attributes_dir
        self.table_model = LocationAttribute
        self.filter_model = LocationAttributeFilter

        self.df = (
            self.spark.read.format("parquet")
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
            .option("header", True)
            .load(str(self.dir))
        )

    def field_enum(self) -> LocationAttributeFields:
        """Get the location attribute fields enum."""
        fields_list = LocationAttribute.get_field_names()
        return LocationAttributeFields(
            "LocationAttributeFields",
            {field: field for field in fields_list}
        )

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        return join_geometry(self.df, self.eval.locations.to_sdf())


class LocationCrosswalkTable(BaseTable):
    """Access methods to location crosswalks table."""

    def __init__(self, eval):
        """Initialize class."""
        super().__init__(eval)
        self.dir = eval.location_crosswalks_dir
        self.table_model = LocationCrosswalk
        self.filter_model = LocationCrosswalkFilter

        self.df = (
            self.spark.read.format("parquet")
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
            .option("header", True)
            .load(str(self.dir))
        )

    def field_enum(self) -> LocationCrosswalkFields:
        """Get the location crosswalk fields enum."""
        fields_list = LocationCrosswalk.get_field_names()
        return LocationCrosswalkFields(
            "LocationCrosswalkFields",
            {field: field for field in fields_list}
        )

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        return join_geometry(
            self.df, self.eval.locations.to_sdf(),
            "primary_location_id"
        )


class PrimaryTimeseriesTable(BaseTable):
    """Access methods to timeseries table."""

    def __init__(self, eval):
        """Initialize class."""
        super().__init__(eval)
        self.dir = eval.primary_timeseries_dir
        self.table_model = Timeseries
        self.filter_model = TimeseriesFilter

        self.df = (
            self.spark.read.format("parquet")
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
            .option("header", True)
            .load(str(self.dir))
        )

    def field_enum(self) -> TimeseriesFields:
        """Get the timeseries fields enum."""
        fields_list = Timeseries.get_field_names()
        return TimeseriesFields(
            "TimeseriesFields",
            {field: field for field in fields_list}
        )

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        return join_geometry(self.df, self.eval.locations.to_sdf())


class SecondaryTimeseriesTable(BaseTable):
    """Access methods to timeseries table."""

    def __init__(self, eval):
        """Initialize class."""
        super().__init__(eval)
        self.dir = eval.secondary_timeseries_dir
        self.table_model = Timeseries
        self.filter_model = TimeseriesFilter

        self.df = (
            self.spark.read.format("parquet")
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
            .option("header", True)
            .load(str(self.dir))
        )

    def field_enum(self) -> TimeseriesFields:
        """Get the timeseries fields enum."""
        fields_list = Timeseries.get_field_names()
        return TimeseriesFields(
            "TimeseriesFields",
            {field: field for field in fields_list}
        )

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        return join_geometry(self.df, self.eval.locations.to_sdf())


class JoinedTimeseriesTable(BaseTable):
    """Access methods to joined timeseries table."""

    def __init__(self, eval):
        """Initialize class."""
        super().__init__(eval)
        self.dir = eval.joined_timeseries_dir
        self.filter_model = JoinedTimeseriesFilter
        self.table_model = JoinedTimeseriesTable
        self.validate_filter_field_types = False

        self.df = (
            self.spark.read.format("parquet")
            # .option("recursiveFileLookup", "true")
            # .option("mergeSchema", "true")
            .option("header", True)
            .load(str(self.dir))
        )

    def field_enum(self) -> JoinedTimeseriesFields:
        """Get the joined timeseries fields enum."""
        fields_list = self.df.columns
        return JoinedTimeseriesFields(
            "JoinedTimeseriesFields",
            {field: field for field in fields_list}
        )

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        return join_geometry(
            self.df, self.eval.locations.to_sdf(),
            "primary_location_id"
        )
