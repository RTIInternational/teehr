"""Access methods to units table."""
from pathlib import Path

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
from teehr.loading.timeseries import (
    convert_timeseries,
    validate_and_insert_timeseries
)
from teehr.loading.add_domains import (
    add_configuration,
    add_unit,
    add_variable,
    add_attribute
)
from teehr.loading.locations import (
    convert_locations,
    validate_and_insert_locations
)
from teehr.loading.location_crosswalks import (
    convert_location_crosswalks,
    validate_and_insert_location_crosswalks
)
from teehr.loading.location_attributes import (
    convert_location_attributes,
    validate_and_insert_location_attributes
)
from teehr.loading.joined_timeseries import (
    create_joined_timeseries_dataset
)
from teehr.loading.utils import (
    validate_input_is_parquet,
    validate_input_is_csv,
    validate_input_is_netcdf
)
import teehr.const as const

import logging

logger = logging.getLogger(__name__)


class BaseTable():
    """Base query class."""

    def __init__(self, ev):
        """Initialize class."""
        self.dir = None
        self.ev = ev
        self.spark = ev.spark
        self.table_model: TableBaseModel = None
        self.filter_model: FilterBaseModel = None
        self.validate_filter_field_types = True
        self.df: ps.DataFrame = None
        self.cache_dir = ev.cache_dir
        self.dataset_dir = ev.dataset_dir
        self.scripts_dir = ev.scripts_dir
        self.locations_cache_dir = Path(
            self.cache_dir,
            const.LOADING_CACHE_DIR,
            const.LOCATIONS_DIR
        )
        self.crosswalk_cache_dir = Path(
            self.cache_dir,
            const.LOADING_CACHE_DIR,
            const.LOCATION_CROSSWALKS_DIR
        )
        self.attributes_cache_dir = Path(
            self.cache_dir,
            const.LOADING_CACHE_DIR,
            const.LOCATION_ATTRIBUTES_DIR
        )
        self.secondary_cache_dir = Path(
            self.cache_dir,
            const.LOADING_CACHE_DIR,
            const.SECONDARY_TIMESERIES_DIR
        )
        self.primary_cache_dir = Path(
            self.cache_dir,
            const.LOADING_CACHE_DIR,
            const.PRIMARY_TIMESERIES_DIR
        )

    @staticmethod
    def _raise_missing_table_error():
        """Raise an error if the table does not exist."""
        err_msg = (
            "The requested table does not exist in the dataset."
            " Please load it first."
        )
        logger.error(err_msg)
        raise ValueError(err_msg)

    def _dir_is_emtpy(self, pattern: str = "**/*.parquet") -> bool:
        """Check if the directory contains files of specified pattern."""
        return len(list(self.dir.glob(pattern))) == 0

    def _read_spark_df(self, format: str = "parquet"):
        """Read data from directory as a spark dataframe."""
        self.df = (
            self.spark.read.format(format)
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
            .option("header", True)
            .load(str(self.dir))
        )

    def _load(self):
        """Load data into the dataset."""
        raise NotImplementedError("_load method must be implemented.")

    def add(self):
        """Add domain variables."""
        raise NotImplementedError("add method must be implemented.")

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

        Filters as dictionary:

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

        Filters as string:

        >>> ts_df = ev.primary_timeseries.query(
        >>>     filters=[
        >>>         "value_time > '2022-01-01'",
        >>>         "value_time < '2022-01-02'",
        >>>         "location_id = 'gage-C'"
        >>>     ],
        >>>     order_by=["location_id", "value_time"]
        >>> ).to_pandas()

        Filters as FilterBaseModel:

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
        if self.df is None:
            self._raise_missing_table_error()
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
        if self.df is None:
            self._raise_missing_table_error()
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

        Order by string:

        >>> ts_df = ev.primary_timeseries.order_by("value_time").to_df()

        Order by StrEnum:

        >>> from teehr.querying.field_enums import TimeseriesFields
        >>> ts_df = ev.primary_timeseries.order_by(
        >>>     TimeseriesFields.value_time
        >>> ).to_pandas()

        """
        logger.info(f"Setting order_by {fields}.")
        if self.df is None:
            self._raise_missing_table_error()
        self.df = order_df(self.df, fields)
        return self

    def fields(self) -> List[str]:
        """Return table columns as a list."""
        return self.df.columns

    def distinct_values(self, column: str) -> List[str]:
        """Return distinct values for a column."""
        return self.df.select(column).distinct().rdd.flatMap(lambda x: x).collect()

    def field_enum(self) -> StrEnum:
        """Get the fields enum."""
        raise NotImplementedError("field_enum method must be implemented.")

    def to_pandas(self):
        """Return Pandas DataFrame."""
        if self.df is None:
            self._raise_missing_table_error()
        df = self.df.toPandas()
        df.attrs['table_type'] = None
        df.attrs['fields'] = None
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
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
        if self.df is None:
            self._raise_missing_table_error()
        return self.df


class UnitTable(BaseTable):
    """Access methods to units table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.dir = ev.units_dir
        self.table_model = Unit
        self.filter_model = UnitFilter
        if not self._dir_is_emtpy(pattern="**/*.csv"):
            self._read_spark_df(format="csv")

    def field_enum(self) -> UnitFields:
        """Get the unit fields enum."""
        fields_list = Unit.get_field_names()
        return UnitFields(
            "UnitFields",
            {field: field for field in fields_list}
        )

    def add(
        self,
        unit: Union[Unit, List[Unit]]
    ):
        """Add a unit to the evaluation.

        Parameters
        ----------
        unit : Union[Unit, List[Unit]]
            The unit domain to add.

        Example
        -------
        >>> from teehr.models.domain_tables import Unit
        >>> unit = Unit(
        >>>     name="m^3/s",
        >>>     long_name="Cubic meters per second"
        >>> )
        >>> ev.load.add_unit(unit)
        """
        add_unit(self.dataset_dir, unit)


class VariableTable(BaseTable):
    """Access methods to variables table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.dir = ev.variables_dir
        self.table_model = Variable
        self.filter_model = VariableFilter
        if not self._dir_is_emtpy(pattern="**/*.csv"):
            self._read_spark_df(format="csv")

    def field_enum(self) -> VariableFields:
        """Get the variable fields enum."""
        fields_list = Variable.get_field_names()
        return VariableFields(
            "VariableFields",
            {field: field for field in fields_list}
        )

    def add(
        self,
        variable: Union[Variable, List[Variable]]
    ):
        """Add a unit to the evaluation.

        Parameters
        ----------
        variable : Union[Variable, List[Variable]]
            The variable domain to add.

        Example
        -------
        >>> from teehr.models.domain_tables import Variable
        >>> variable = Variable(
        >>>     name="streamflow_hourly_inst",
        >>>     long_name="Instantaneous streamflow"
        >>> )
        >>> ev.load.add_variable(variable)
        """
        add_variable(self.dataset_dir, variable)


class AttributeTable(BaseTable):
    """Access methods to attributes table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.dir = ev.attributes_dir
        self.table_model = Attribute
        self.filter_model = AttributeFilter
        if not self._dir_is_emtpy(pattern="**/*.csv"):
            self._read_spark_df(format="csv")

    def field_enum(self) -> AttributeFields:
        """Get the attribute fields enum."""
        fields_list = Attribute.get_field_names()
        return AttributeFields(
            "AttributeFields",
            {field: field for field in fields_list}
        )

    def add(
        self,
        attribute: Union[Attribute, List[Attribute]]
    ):
        """Add an attribute to the evaluation.

        Parameters
        ----------
        attribute : Union[Attribute, List[Attribute]]
            The attribute domain to add.

        Example
        -------
        >>> from teehr.models.domain_tables import Attribute
        >>> attribute = Attribute(
        >>>     name="drainage_area",
        >>>     type="continuous",
        >>>     description="Drainage area in square kilometers"
        >>> )
        >>> ev.load.add_attribute(attribute)
        """
        add_attribute(self.dataset_dir, attribute)


class ConfigurationTable(BaseTable):
    """Access methods to configurations table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.dir = ev.configurations_dir
        self.table_model = Configuration
        self.filter_model = ConfigurationFilter
        if not self._dir_is_emtpy(pattern="**/*.csv"):
            self._read_spark_df(format="csv")

    def field_enum(self) -> ConfigurationFields:
        """Get the configuration fields enum."""
        fields_list = Configuration.get_field_names()
        return ConfigurationFields(
            "ConfigurationFields",
            {field: field for field in fields_list}
        )

    def add(
        self,
        configuration: Union[Configuration, List[Configuration]]
    ):
        """Add a configuration domain to the evaluation.

        Parameters
        ----------
        configuration : Union[Configuration, List[Configuration]]
            The configuration domain to add.

        Example
        -------
        >>> from teehr.models.domain_tables import Configuration
        >>> configuration = Configuration(
        >>>     name="usgs_observations",
        >>>     type="primary",
        >>>     description="USGS observations",
        >>> )
        >>> ev.load.add_configuration(configuration)

        """
        add_configuration(self.dataset_dir, configuration)


class LocationTable(BaseTable):
    """Access methods to locations table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.dir = ev.locations_dir
        self.table_model = Location
        self.filter_model = LocationFilter
        if not self._dir_is_emtpy():
            self._read_spark_df()

    def field_enum(self) -> LocationFields:
        """Get the location fields enum."""
        fields_list = Location.get_field_names()
        return LocationFields(
            "LocationFields",
            {field: field for field in fields_list}
        )

    def to_pandas(self):
        """Return Pandas DataFrame for Location Table."""
        df = self.df.toPandas()
        df.attrs['table_type'] = 'locations'
        df.attrs['fields'] = self.fields()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        if self.df is None:
            err_msg = (
                "No location data exists in the dataset."
                " Please load it first using load_spatial()."
            )
            logger.error(err_msg)
        gdf = df_to_gdf(self.to_pandas())
        gdf.attrs['table_type'] = 'locations'
        gdf.attrs['fields'] = self.fields()
        return gdf

    def load_spatial(
        self,
        in_path: Union[Path, str],
        field_mapping: dict = None,
        pattern: str = "**/*.parquet",
        **kwargs
    ):
        """Import geometry data.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            Any file format that can be read by GeoPandas.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        pattern : str, optional (default: "**/*.parquet")
            The pattern to match files.
            Only used when in_path is a directory.
        **kwargs
            Additional keyword arguments are passed to GeoPandas read_file().

        File is first read by GeoPandas, field names renamed and
        then validated and inserted into the dataset.

        Notes
        -----

        The TEEHR Location Crosswalk table schema includes fields:

        - id
        - name
        - geometry
        """
        convert_locations(
            in_path,
            self.locations_cache_dir,
            field_mapping=field_mapping,
            pattern=pattern,
            **kwargs
        )
        validate_and_insert_locations(
            self.ev,
            self.locations_cache_dir,
            pattern=pattern
        )
        self._read_spark_df()


class LocationAttributeTable(BaseTable):
    """Access methods to location attributes table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.dir = ev.location_attributes_dir
        self.table_model = LocationAttribute
        self.filter_model = LocationAttributeFilter
        if not self._dir_is_emtpy():
            self._read_spark_df()

    def _load(
        self,
        in_path: Union[Path, str],
        pattern: str = None,
        field_mapping: dict = None,
        **kwargs
    ):
        """Load location attributes helper."""
        convert_location_attributes(
            in_path,
            self.attributes_cache_dir,
            pattern=pattern,
            field_mapping=field_mapping,
            **kwargs
        )
        validate_and_insert_location_attributes(
            ev=self.ev,
            in_path=self.attributes_cache_dir,
            pattern="**/*.parquet"
        )
        self._read_spark_df()

    def field_enum(self) -> LocationAttributeFields:
        """Get the location attribute fields enum."""
        fields_list = LocationAttribute.get_field_names()
        return LocationAttributeFields(
            "LocationAttributeFields",
            {field: field for field in fields_list}
        )

    def to_pandas(self):
        """Return Pandas DataFrame for Location Attributes."""
        df = self.df.toPandas()
        df.attrs['table_type'] = 'location_attributes'
        df.attrs['fields'] = self.fields()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        if self.df is None:
            err_msg = (
                "No location attributes data exists in the dataset."
                " Please load it first using load_parquet()"
                " or load_csv()."
            )
            logger.error(err_msg)
            raise ValueError(err_msg)
        return join_geometry(self.df, self.ev.locations.to_sdf())

    def load_parquet(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.parquet",
        field_mapping: dict = None,
        **kwargs
    ):
        """Import location_attributes from parquet file format.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            Parquet file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        **kwargs
            Additional keyword arguments are passed to pd.read_parquet().

        Notes
        -----

        The TEEHR Location Attribute table schema includes fields:

        - location_id
        - attribute_name
        - value
        """
        validate_input_is_parquet(in_path)
        self._load(
            in_path=in_path,
            pattern=pattern,
            field_mapping=field_mapping,
            **kwargs
        )
        self._read_spark_df()

    def load_csv(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.csv",
        field_mapping: dict = None,
        **kwargs
    ):
        """Import location_attributes from CSV file format.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            CSV file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        **kwargs
            Additional keyword arguments are passed to pd.read_parquet().

        Notes
        -----

        The TEEHR Location Attribute table schema includes fields:

        - location_id
        - attribute_name
        - value
        """
        validate_input_is_csv(in_path)
        self._load(
            in_path=in_path,
            pattern=pattern,
            field_mapping=field_mapping,
            **kwargs
        )
        self._read_spark_df()


class LocationCrosswalkTable(BaseTable):
    """Access methods to location crosswalks table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.dir = ev.location_crosswalks_dir
        self.table_model = LocationCrosswalk
        self.filter_model = LocationCrosswalkFilter
        if not self._dir_is_emtpy():
            self._read_spark_df()

    def _load(
        self,
        in_path: Union[Path, str],
        field_mapping: dict = None,
        pattern: str = None,
        **kwargs
    ):
        """Load location crosswalks helper."""
        convert_location_crosswalks(
            in_path,
            self.crosswalk_cache_dir,
            field_mapping=field_mapping,
            pattern=pattern,
            **kwargs
        )
        validate_and_insert_location_crosswalks(
            ev=self.ev,
            in_path=self.crosswalk_cache_dir,
            pattern="**/*.parquet"
        )
        self._read_spark_df()

    def field_enum(self) -> LocationCrosswalkFields:
        """Get the location crosswalk fields enum."""
        fields_list = LocationCrosswalk.get_field_names()
        return LocationCrosswalkFields(
            "LocationCrosswalkFields",
            {field: field for field in fields_list}
        )

    def to_pandas(self):
        """Return Pandas DataFrame for Location Crosswalk."""
        df = self.df.toPandas()
        df.attrs['table_type'] = 'location_crosswalks'
        df.attrs['fields'] = self.fields()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        if self.df is None:
            err_msg = (
                "No location crosswalk data exists in the dataset."
                " Please load it first using load_parquet()."
            )
            logger.error(err_msg)
            raise ValueError(err_msg)
        return join_geometry(
            self.df, self.ev.locations.to_sdf(),
            "primary_location_id"
        )

    def load_parquet(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.parquet",
        field_mapping: dict = None,
        **kwargs
    ):
        """Import location crosswalks from parquet file format.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            Parquet file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
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
        self._load(
            in_path=in_path,
            field_mapping=field_mapping,
            pattern=pattern,
            **kwargs
        )
        self._read_spark_df()

    def load_csv(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.csv",
        field_mapping: dict = None,
        **kwargs
    ):
        """Import location crosswalks from CSV file format.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            CSV file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        **kwargs
            Additional keyword arguments are passed to pd.read_csv()
            or pd.read_parquet().

        Notes
        -----

        The TEEHR Location Crosswalk table schema includes fields:

        - primary_location_id
        - secondary_location_id
        """
        validate_input_is_csv(in_path)
        self._load(
            in_path=in_path,
            field_mapping=field_mapping,
            pattern=pattern,
            **kwargs
        )
        self._read_spark_df()


class PrimaryTimeseriesTable(BaseTable):
    """Access methods to timeseries table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.dir = ev.primary_timeseries_dir
        self.table_model = Timeseries
        self.filter_model = TimeseriesFilter
        if not self._dir_is_emtpy():
            self._read_spark_df()

    def _read_spark_df(self, format: str = "parquet"):
        """Read data from directory as a spark dataframe."""
        self.df = (
            self.spark.read.format(format)
            .option("header", True)
            .load(str(self.dir))
        )

    def _load(
        self,
        in_path: Union[Path, str],
        cache_path: Path,
        pattern="**/*.parquet",
        timeseries_type: str = None,
        field_mapping: dict = None,
        constant_field_values: dict = None,
        **kwargs
    ):
        """Import timeseries helper."""
        convert_timeseries(
            in_path=in_path,
            out_path=cache_path,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            pattern=pattern,
            **kwargs
        )

        if pattern.endswith(".csv"):
            pattern = pattern.replace(".csv", ".parquet")
        elif pattern.endswith(".nc"):
            pattern = pattern.replace(".nc", ".parquet")

        validate_and_insert_timeseries(
            ev=self.ev,
            in_path=cache_path,
            timeseries_type=timeseries_type,
            pattern="**/*.parquet"
        )
        self._read_spark_df()

    def field_enum(self) -> TimeseriesFields:
        """Get the timeseries fields enum."""
        fields_list = Timeseries.get_field_names()
        return TimeseriesFields(
            "TimeseriesFields",
            {field: field for field in fields_list}
        )

    def to_pandas(self):
        """Return Pandas DataFrame for Primary Timeseries."""
        df = self.df.toPandas()
        df.attrs['table_type'] = 'timeseries'
        df.attrs['fields'] = self.fields()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        if self.df is None:
            err_msg = (
                "No primary timeseries data exists in the dataset."
                " Please load it first using either load_parquet(),"
                " load_csv(), or load_netcdf()."
            )
            logger.error(err_msg)
            raise ValueError(err_msg)
        return join_geometry(self.df, self.ev.locations.to_sdf())

    def load_parquet(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.parquet",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        **kwargs
    ):
        """Import primary timeseries parquet data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            parquet file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}
        **kwargs
            Additional keyword arguments are passed to pd.read_parquet().

        Includes validation and importing data to database.

        Notes
        -----

        The TEEHR Timeseries table schema includes fields:

        - reference_time
        - value_time
        - configuration_name
        - unit_name
        - variable_name
        - value
        - location_id
        """
        logger.info(f"Loading primary timeseries parquet data: {in_path}")
        self.primary_cache_dir.mkdir(parents=True, exist_ok=True)

        validate_input_is_parquet(in_path)
        self._load(
            in_path=in_path,
            cache_path=self.primary_cache_dir,
            pattern=pattern,
            timeseries_type="primary",
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            **kwargs
        )
        self._read_spark_df()

    def load_csv(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.csv",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        **kwargs
    ):
        """Import primary timeseries csv data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            csv file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}
        **kwargs
            Additional keyword arguments are passed to pd.read_csv().

        Includes validation and importing data to database.

        Notes
        -----

        The TEEHR Timeseries table schema includes fields:

        - reference_time
        - value_time
        - configuration_name
        - unit_name
        - variable_name
        - value
        - location_id
        """
        logger.info(f"Loading primary timeseries csv data: {in_path}")
        self.primary_cache_dir.mkdir(parents=True, exist_ok=True)

        validate_input_is_csv(in_path)
        self._load(
            in_path=in_path,
            cache_path=self.primary_cache_dir,
            pattern=pattern,
            timeseries_type="primary",
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            **kwargs
        )
        self._read_spark_df()

    def load_netcdf(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.nc",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        **kwargs
    ):
        """Import primary timeseries netcdf data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            netcdf file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}
        **kwargs
            Additional keyword arguments are passed to xr.open_dataset().

        Includes validation and importing data to database.

        Notes
        -----

        The TEEHR Timeseries table schema includes fields:

        - reference_time
        - value_time
        - configuration_name
        - unit_name
        - variable_name
        - value
        - location_id
        """
        logger.info(f"Loading primary timeseries netcdf data: {in_path}")
        self.primary_cache_dir.mkdir(parents=True, exist_ok=True)

        validate_input_is_netcdf(in_path)
        self._load(
            in_path=in_path,
            cache_path=self.primary_cache_dir,
            pattern=pattern,
            timeseries_type="primary",
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            **kwargs
        )
        self._read_spark_df()


class SecondaryTimeseriesTable(BaseTable):
    """Access methods to timeseries table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.dir = ev.secondary_timeseries_dir
        self.table_model = Timeseries
        self.filter_model = TimeseriesFilter
        if not self._dir_is_emtpy():
            self._read_spark_df()

    def _read_spark_df(self, format: str = "parquet"):
        """Read data from directory as a spark dataframe."""
        self.df = (
            self.spark.read.format(format)
            .option("header", True)
            .load(str(self.dir))
        )

    def _load(
        self,
        in_path: Union[Path, str],
        cache_path: Path,
        pattern="**/*.parquet",
        timeseries_type: str = None,
        field_mapping: dict = None,
        constant_field_values: dict = None,
        **kwargs
    ):
        """Import timeseries helper."""
        convert_timeseries(
            in_path=in_path,
            out_path=cache_path,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            pattern=pattern,
            **kwargs
        )

        if pattern.endswith(".csv"):
            pattern = pattern.replace(".csv", ".parquet")
        elif pattern.endswith(".nc"):
            pattern = pattern.replace(".nc", ".parquet")

        validate_and_insert_timeseries(
            ev=self.ev,
            in_path=cache_path,
            timeseries_type=timeseries_type,
            pattern="**/*.parquet"
        )
        self._read_spark_df()

    def field_enum(self) -> TimeseriesFields:
        """Get the timeseries fields enum."""
        fields_list = Timeseries.get_field_names()
        return TimeseriesFields(
            "TimeseriesFields",
            {field: field for field in fields_list}
        )

    def to_pandas(self):
        """Return Pandas DataFrame for Secondary Timeseries."""
        df = self.df.toPandas()
        df.attrs['table_type'] = 'timeseries'
        df.attrs['fields'] = self.fields()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        if self.df is None:
            err_msg = (
                "No secondary timeseries data exists in the dataset."
                " Please load it first using either load_parquet(),"
                " load_csv(), or load_netcdf()."
            )
            logger.error(err_msg)
            raise ValueError(err_msg)
        # TODO: Need to access the crosswalk table here?
        return join_geometry(self.df, self.ev.locations.to_sdf())

    def load_parquet(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.parquet",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        **kwargs
    ):
        """Import secondary timeseries parquet data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            parquet file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}
        **kwargs
            Additional keyword arguments are passed to pd.read_parquet().

        Includes validation and importing data to database.

        Notes
        -----

        The TEEHR Timeseries table schema includes fields:

        - reference_time
        - value_time
        - configuration_name
        - unit_name
        - variable_name
        - value
        - location_id
        """
        logger.info(f"Loading secondary timeseries parquet data: {in_path}")
        self.secondary_cache_dir.mkdir(parents=True, exist_ok=True)

        validate_input_is_parquet(in_path)
        self._load(
            in_path=in_path,
            cache_path=self.secondary_cache_dir,
            pattern=pattern,
            timeseries_type="secondary",
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            **kwargs
        )
        self._read_spark_df()

    def load_csv(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.csv",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        **kwargs
    ):
        """Import secondary timeseries csv data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            csv file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}
        **kwargs
            Additional keyword arguments are passed to pd.read_csv().

        Includes validation and importing data to database.

        Notes
        -----

        The TEEHR Timeseries table schema includes fields:

        - reference_time
        - value_time
        - configuration_name
        - unit_name
        - variable_name
        - value
        - location_id
        """
        logger.info(f"Loading secondary timeseries csv data: {in_path}")
        self.secondary_cache_dir.mkdir(parents=True, exist_ok=True)

        validate_input_is_csv(in_path)
        self._load(
            in_path=in_path,
            cache_path=self.secondary_cache_dir,
            pattern=pattern,
            timeseries_type="secondary",
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            **kwargs
        )
        self._read_spark_df()

    def load_netcdf(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.nc",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        **kwargs
    ):
        """Import secondary timeseries netcdf data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            netcdf file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}
        **kwargs
            Additional keyword arguments are passed to xr.open_dataset().

        Includes validation and importing data to database.

        Notes
        -----

        The TEEHR Timeseries table schema includes fields:

        - reference_time
        - value_time
        - configuration_name
        - unit_name
        - variable_name
        - value
        - location_id
        """
        logger.info(f"Loading secondary timeseries netcdf data: {in_path}")
        self.secondary_cache_dir.mkdir(parents=True, exist_ok=True)

        validate_input_is_netcdf(in_path)
        self._load(
            in_path=in_path,
            cache_path=self.secondary_cache_dir,
            pattern=pattern,
            timeseries_type="secondary",
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            **kwargs
        )
        self._read_spark_df()


class JoinedTimeseriesTable(BaseTable):
    """Access methods to joined timeseries table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.dir = ev.joined_timeseries_dir
        self.filter_model = JoinedTimeseriesFilter
        self.table_model = JoinedTimeseriesTable
        self.validate_filter_field_types = False
        if not self._dir_is_emtpy():
            self._read_spark_df()

    def _read_spark_df(self, format: str = "parquet"):
        """Read data from directory as a spark dataframe."""
        self.df = (
            self.spark.read.format(format)
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

    def to_pandas(self):
        """Return Pandas DataFrame for Joined Timeseries."""
        df = self.df.toPandas()
        df.attrs['table_type'] = 'joined_timeseries'
        df.attrs['fields'] = self.fields()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        if self.df is None:
            err_msg = (
                "No joined timeseries data exists in the dataset."
                " Please create it first using create()."
            )
            logger.error(err_msg)
            raise ValueError(err_msg)
        return join_geometry(
            self.df, self.ev.locations.to_sdf(),
            "primary_location_id"
        )

    def create(self, execute_udf: bool = False):
        """Create joined timeseries table.

        Parameters
        ----------
        execute_udf : bool, optional
            Execute UDFs, by default False
        """
        create_joined_timeseries_dataset(
            self.spark,
            self.dataset_dir,
            self.scripts_dir,
            execute_udf,
        )
