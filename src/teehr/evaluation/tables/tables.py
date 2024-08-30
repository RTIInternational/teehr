"""Access methods to units table."""
from teehr.querying.utils import df_to_gdf
from teehr.evaluation.tables.base_table import BaseTable
from teehr.models.dataset.table_models import (
    Unit,
    Variable,
    Attribute,
    Configuration,
    Location,
    LocationAttribute,
    LocationCrosswalk,
    Timeseries
)

from teehr.models.dataset.filters import (
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
from teehr.models.dataset.table_enums import (
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


class UnitTable(BaseTable):
    """Access methods to units table."""

    def __init__(self, eval):
        """Initialize class."""
        self.dir = eval.units_dir
        self.spark = eval.spark
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
        self.dir = eval.variables_dir
        self.spark = eval.spark
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
        self.dir = eval.attributes_dir
        self.spark = eval.spark
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
        self.dir = eval.configurations_dir
        self.spark = eval.spark
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
        self.dir = eval.locations_dir
        self.spark = eval.spark
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

    def to_df(self):
        """Return the DataFrame as a GeoDataFrame."""
        return df_to_gdf(super().to_df())


class LocationAttributeTable(BaseTable):
    """Access methods to location attributes table."""

    def __init__(self, eval):
        """Initialize class."""
        self.dir = eval.location_attributes_dir
        self.spark = eval.spark
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


class LocationCrosswalkTable(BaseTable):
    """Access methods to location crosswalks table."""

    def __init__(self, eval):
        """Initialize class."""
        self.dir = eval.location_crosswalks_dir
        self.spark = eval.spark
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


class PrimaryTimeseriesTable(BaseTable):
    """Access methods to timeseries table."""

    def __init__(self, eval):
        """Initialize class."""
        self.dir = eval.primary_timeseries_dir
        self.spark = eval.spark
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


class SecondaryTimeseriesTable(BaseTable):
    """Access methods to timeseries table."""

    def __init__(self, eval):
        """Initialize class."""
        self.dir = eval.secondary_timeseries_dir
        self.spark = eval.spark
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


class JoinedTimeseriesTable(BaseTable):
    """Access methods to joined timeseries table."""

    def __init__(self, eval):
        """Initialize class."""
        self.dir = eval.joined_timeseries_dir
        self.spark = eval.spark
        self.filter_model = JoinedTimeseriesFilter

        self.df = (
            self.spark.read.format("parquet")
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
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
