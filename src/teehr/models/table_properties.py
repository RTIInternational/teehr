"""Pydantic models for table properties."""
# from pathlib import Path
# from typing import Dict, Any, List

# from pydantic import BaseModel as PydanticBaseModel, ConfigDict, Field, model_validator

import teehr.models.pandera_dataframe_schemas as schemas
import teehr.models.filters as table_filters


TBLPROPERTIES = {
    "units": {
        "uniqueness_fields": ["name"],
        "foreign_keys": None,
        "filter_model": table_filters.UnitFilter,
        "schema_func": schemas.unit_schema,
        "file_format": "csv",
        "strict_validation": True,
        "validate_filter_field_types": True
    },
    "variables": {
        "uniqueness_fields": ["name"],
        "foreign_keys": None,
        "filter_model": table_filters.VariableFilter,
        "schema_func": schemas.variable_schema,
        "file_format": "csv",
        "strict_validation": True,
        "validate_filter_field_types": True
    },
    "configurations": {
        "uniqueness_fields": ["name"],
        "foreign_keys": None,
        "filter_model": table_filters.ConfigurationFilter,
        "schema_func": schemas.configuration_schema,
        "file_format": "csv",
        "strict_validation": True,
        "validate_filter_field_types": True
    },
    "attributes": {
        "uniqueness_fields": ["name"],
        "foreign_keys": None,
        "filter_model": table_filters.AttributeFilter,
        "schema_func": schemas.attribute_schema,
        "file_format": "csv",
        "strict_validation": True,
        "validate_filter_field_types": True
    },
    "locations": {
        "uniqueness_fields": ["id"],
        "foreign_keys": None,
        "filter_model": table_filters.LocationFilter,
        "schema_func": schemas.locations_schema,
        "file_format": "parquet",
        "strict_validation": True,
        "validate_filter_field_types": True
    },
    "location_attributes": {
        "uniqueness_fields": ["location_id", "attribute_name"],
        "foreign_keys": [
            {
                "column": "location_id",
                "domain_table": "locations",
                "domain_column": "id",
            },
            {
                "column": "attribute_name",
                "domain_table": "attributes",
                "domain_column": "name",
            }
        ],
        "filter_model": table_filters.LocationAttributeFilter,
        "schema_func": schemas.location_attributes_schema,
        "file_format": "parquet",
        "strict_validation": True,
        "validate_filter_field_types": True
    },
    "location_croswalks": {
        "uniqueness_fields": ["secondary_location_id"],
        "foreign_keys": [
            {
                "column": "primary_location_id",
                "domain_table": "locations",
                "domain_column": "id",
            }
        ],
        "filter_model": table_filters.LocationCrosswalkFilter,
        "schema_func": schemas.location_crosswalks_schema,
        "file_format": "parquet",
        "strict_validation": True,
        "validate_filter_field_types": True
    },
    "primary_timeseries": {
        "uniqueness_fields": [
            "location_id",
            "value_time",
            "reference_time",
            "variable_name",
            "unit_name",
            "configuration_name"
        ],
        "foreign_keys": [
            {
                "column": "variable_name",
                "domain_table": "variables",
                "domain_column": "name",
            },
            {
                "column": "unit_name",
                "domain_table": "units",
                "domain_column": "name",
            },
            {
                "column": "configuration_name",
                "domain_table": "configurations",
                "domain_column": "name",
            },
            {
                "column": "location_id",
                "domain_table": "locations",
                "domain_column": "id",
            }
        ],
        "filter_model": table_filters.TimeseriesFilter,
        "schema_func": schemas.primary_timeseries_schema,
        "file_format": "parquet",
        "strict_validation": True,
        "validate_filter_field_types": True
    },
    "secondary_timeseries": {
        "uniqueness_fields": [
            "location_id",
            "value_time",
            "reference_time",
            "variable_name",
            "unit_name",
            "configuration_name"
        ],
        "foreign_keys": [
            {
                "column": "variable_name",
                "domain_table": "variables",
                "domain_column": "name",
            },
            {
                "column": "unit_name",
                "domain_table": "units",
                "domain_column": "name",
            },
            {
                "column": "configuration_name",
                "domain_table": "configurations",
                "domain_column": "name",
            },
            {
                "column": "location_id",
                "domain_table": "location_crosswalks",
                "domain_column": "secondary_location_id",
            }
        ],
        "filter_model": table_filters.TimeseriesFilter,
        "schema_func": schemas.secondary_timeseries_schema,
        "file_format": "parquet",
        "strict_validation": True,
        "validate_filter_field_types": True
    },
    "joined_timeseries": {
        "uniqueness_fields": [
            "primary_location_id",
            "secondary_location_id",
            "value_time",
            "reference_time",
            "variable_name",
            "unit_name",
            "configuration_name",
        ],
        "foreign_keys": None,
        "filter_model": table_filters.JoinedTimeseriesFilter,
        "schema_func": schemas.joined_timeseries_schema,
        "file_format": "parquet",
        "strict_validation": False,
        "validate_filter_field_types": False
    }
}
