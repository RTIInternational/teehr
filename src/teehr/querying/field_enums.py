"""This module contains functions to get the fields of the tables as enums."""
from teehr.evaluation.utils import get_spark_schema
from pyspark.sql import SparkSession
from pathlib import Path
from typing import Union
from teehr.models.dataset.table_models import (
    Configuration,
    Unit,
    Variable,
    Attribute,
    Location,
    LocationAttribute,
    LocationCrosswalk,
    Timeseries
)
from teehr.models.dataset.table_enums import (
    ConfigurationFields,
    UnitFields,
    VariableFields,
    AttributeFields,
    LocationFields,
    LocationAttributeFields,
    LocationCrosswalkFields,
    TimeseriesFields,
    JoinedTimeseriesFields
)
import logging

logger = logging.getLogger(__name__)


def get_location_fields() -> LocationFields:
    """Get the location fields."""
    logger.debug("Getting location fields.")
    fields_list = Location.get_field_names()
    return LocationFields(
        "LocationFieldEnum",
        {field: field for field in fields_list}
    )


def get_configuration_fields() -> ConfigurationFields:
    """Get the configuration fields."""
    logger.debug("Getting configuration fields.")
    fields_list = Configuration.get_field_names()
    return ConfigurationFields(
        "ConfigurationFieldEnum",
        {field: field for field in fields_list}
    )


def get_unit_fields() -> UnitFields:
    """Get the unit fields."""
    logger.debug("Getting unit fields.")
    fields_list = Unit.get_field_names()
    return UnitFields(
        "UnitFieldEnum",
        {field: field for field in fields_list}
    )


def get_variable_fields() -> VariableFields:
    """Get the variable fields."""
    logger.debug("Getting variable fields.")
    fields_list = Variable.get_field_names()
    return VariableFields(
        "VariableFieldEnum",
        {field: field for field in fields_list}
    )


def get_attribute_fields() -> AttributeFields:
    """Get the attribute fields."""
    logger.debug("Getting attribute fields.")
    field_list = Attribute.get_field_names()
    return AttributeFields(
        "AttributeFieldEnum",
        {field: field for field in field_list}
    )


def get_location_attribute_fields() -> LocationAttributeFields:
    """Get the location attribute fields."""
    logger.debug("Getting location attribute fields.")
    field_list = LocationAttribute.get_field_names()
    return LocationAttributeFields(
        "LocationAttributeFieldEnum",
        {field: field for field in field_list}
    )


def get_location_crosswalk_fields() -> LocationCrosswalkFields:
    """Get the location crosswalk fields."""
    logger.debug("Getting location crosswalk fields.")
    field_list = LocationCrosswalk.get_field_names()
    return LocationCrosswalkFields(
        "LocationCrosswalkFieldEnum",
        {field: field for field in field_list}
    )


def get_timeseries_fields() -> TimeseriesFields:
    """Get the timeseries fields."""
    logger.debug("Getting timeseries fields.")
    field_list = Timeseries.get_field_names()
    return TimeseriesFields(
        "TimeseriesFieldEnum",
        {field: field for field in field_list}
    )


def get_joined_timeseries_fields(
    spark: SparkSession,
    joined_timeseries_dir: Union[Path, str]
) -> JoinedTimeseriesFields:
    """Get the joined timeseries fields.

    This function reads the schema of the joined timeseries directory and
    returns the fields as an enum.
    """
    if len(list(Path(joined_timeseries_dir).glob("**/*.parquet"))) == 0:
        logger.error(f"No parquet files in {joined_timeseries_dir}.")
        raise FileNotFoundError
    else:
        logger.info(f"Reading fields from {joined_timeseries_dir}.")
        schema = get_spark_schema(spark, joined_timeseries_dir)
        fields_list = [field.name for field in schema.fields]
        return JoinedTimeseriesFields(
            "JoinedTimeseriesFieldEnum",
            {field: field for field in fields_list}
        )
