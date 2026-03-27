"""This module tests the dataframe validation."""
from datetime import datetime

import pandas as pd
import pytest

from teehr.models.pandera_dataframe_schemas import (
    configuration_schema,
    primary_timeseries_schema,
)


@pytest.mark.session_scope_test_warehouse
def test_validate_dataframe_valid_data(session_scope_test_warehouse):
    """Test validation passes with valid data."""
    ev = session_scope_test_warehouse

    # Create valid configuration data
    pdf = pd.DataFrame({
        "name": ["test_config"],
        "type": ["primary"],
        "description": ["A test configuration"],
        "created_at": [datetime(2022, 1, 1)],
        "updated_at": [datetime(2022, 1, 1)]
    })

    schema = configuration_schema(type="pandas")

    result = ev._validate.dataframe(
        df=pdf,
        table_schema=schema,
        strict=True,
        add_missing_columns=False,
        drop_duplicates=True,
        uniqueness_fields=["name"]
    )

    assert len(result) == 1
    assert result["name"].iloc[0] == "test_config"


@pytest.mark.session_scope_test_warehouse
def test_validate_dataframe_add_missing_columns(session_scope_test_warehouse):
    """Test that missing nullable columns are added when add_missing_columns=True."""
    ev = session_scope_test_warehouse
    import pandera.pandas as pa

    # Create a custom schema with a nullable column for testing
    test_schema = pa.DataFrameSchema(
        columns={
            "id": pa.Column(pa.String, nullable=False),
            "optional_field": pa.Column(pa.String, nullable=True),
        },
        strict="filter"
    )

    # Create data missing the optional column
    pdf = pd.DataFrame({
        "id": ["test_id"],
    })

    result = ev._validate.dataframe(
        df=pdf,
        table_schema=test_schema,
        strict=True,
        add_missing_columns=True,
        drop_duplicates=True,
        uniqueness_fields=["id"]
    )

    assert "optional_field" in result.columns
    # optional_field should be None since it was added as missing
    assert pd.isna(result["optional_field"].iloc[0])


@pytest.mark.session_scope_test_warehouse
def test_validate_dataframe_add_missing_raises_for_required(session_scope_test_warehouse):
    """Test that adding missing columns raises error for non-nullable columns."""
    ev = session_scope_test_warehouse
    import pandera.pandas as pa

    # Create a custom schema with required (non-nullable) columns
    test_schema = pa.DataFrameSchema(
        columns={
            "id": pa.Column(pa.String, nullable=False),
            "required_field": pa.Column(pa.String, nullable=False),
        },
        strict="filter"
    )

    # Create data missing the required column
    pdf = pd.DataFrame({
        "id": ["test_id"],
    })

    with pytest.raises(ValueError, match="Required.*non-nullable.*required_field"):
        ev._validate.dataframe(
            df=pdf,
            table_schema=test_schema,
            strict=True,
            add_missing_columns=True,
            drop_duplicates=True,
            uniqueness_fields=["id"]
        )


@pytest.mark.session_scope_test_warehouse
def test_validate_dataframe_strict_removes_extra_columns(session_scope_test_warehouse):
    """Test that extra columns are removed when strict=True."""
    ev = session_scope_test_warehouse

    # Create data with extra columns
    pdf = pd.DataFrame({
        "name": ["test_config"],
        "type": ["primary"],
        "description": ["A test configuration"],
        "extra_column": ["should be removed"],
        "created_at": [datetime(2022, 1, 1)],
        "updated_at": [datetime(2022, 1, 1)]
    })

    schema = configuration_schema(type="pandas")

    result = ev._validate.dataframe(
        df=pdf,
        table_schema=schema,
        strict=True,
        add_missing_columns=False,
        drop_duplicates=True,
        uniqueness_fields=["name"]
    )

    assert "extra_column" not in result.columns
    assert len(result.columns) == 5


@pytest.mark.session_scope_test_warehouse
def test_validate_dataframe_drop_duplicates(session_scope_test_warehouse):
    """Test that duplicates are dropped when drop_duplicates=True."""
    ev = session_scope_test_warehouse

    # Create data with duplicates
    pdf = pd.DataFrame({
        "name": ["test_config", "test_config", "other_config"],
        "type": ["primary", "primary", "secondary"],
        "description": ["First", "Duplicate", "Other"],
        "created_at": [datetime(2022, 1, 1), datetime(2022, 1, 1), datetime(2022, 1, 2)],
        "updated_at": [datetime(2022, 1, 1), datetime(2022, 1, 1), datetime(2022, 1, 2)]
    })

    schema = configuration_schema(type="pandas")

    result = ev._validate.dataframe(
        df=pdf,
        table_schema=schema,
        strict=True,
        add_missing_columns=False,
        drop_duplicates=True,
        uniqueness_fields=["name"]
    )

    assert len(result) == 2


@pytest.mark.session_scope_test_warehouse
def test_validate_dataframe_invalid_type_value(session_scope_test_warehouse):
    """Test validation fails with invalid type value."""
    ev = session_scope_test_warehouse

    # Create data with invalid type value
    pdf = pd.DataFrame({
        "name": ["test_config"],
        "type": ["invalid_type"],  # Not in ["primary", "secondary"]
        "description": ["A test configuration"],
        "created_at": [datetime(2022, 1, 1)],
        "updated_at": [datetime(2022, 1, 1)]
    })

    schema = configuration_schema(type="pandas")

    with pytest.raises(Exception):  # pandera.errors.SchemaError
        ev._validate.dataframe(
            df=pdf,
            table_schema=schema,
            strict=True,
            add_missing_columns=False,
            drop_duplicates=True,
            uniqueness_fields=["name"]
        )


@pytest.mark.session_scope_test_warehouse
def test_validate_dataframe_missing_required_column(session_scope_test_warehouse):
    """Test validation fails when required column is missing and add_missing=False."""
    ev = session_scope_test_warehouse

    # Create data missing required column
    pdf = pd.DataFrame({
        "name": ["test_config"],
        "type": ["primary"],
        # Missing "description"
        "created_at": [datetime(2022, 1, 1)],
        "updated_at": [datetime(2022, 1, 1)]
    })

    schema = configuration_schema(type="pandas")

    with pytest.raises(ValueError, match="missing from the DataFrame"):
        ev._validate.dataframe(
            df=pdf,
            table_schema=schema,
            strict=True,
            add_missing_columns=False,
            drop_duplicates=True,
            uniqueness_fields=["name"]
        )


@pytest.mark.session_scope_test_warehouse
def test_validate_dataframe_invalid_name_format(session_scope_test_warehouse):
    """Test validation fails with invalid name format (special characters)."""
    ev = session_scope_test_warehouse

    # Create data with invalid name (contains special characters)
    pdf = pd.DataFrame({
        "name": ["test-config!"],  # Invalid: contains hyphen and exclamation
        "type": ["primary"],
        "description": ["A test configuration"],
        "created_at": [datetime(2022, 1, 1)],
        "updated_at": [datetime(2022, 1, 1)]
    })

    schema = configuration_schema(type="pandas")

    with pytest.raises(Exception):  # pandera.errors.SchemaError
        ev._validate.dataframe(
            df=pdf,
            table_schema=schema,
            strict=True,
            add_missing_columns=False,
            drop_duplicates=True,
            uniqueness_fields=["name"]
        )


@pytest.mark.session_scope_test_warehouse
def test_validate_dataframe_pyspark(session_scope_test_warehouse):
    """Test validation with PySpark DataFrame."""
    ev = session_scope_test_warehouse

    # Create valid configuration data as pandas first
    pdf = pd.DataFrame({
        "name": ["spark_test_config"],
        "type": ["primary"],
        "description": ["A PySpark test configuration"],
        "created_at": [datetime(2022, 1, 1)],
        "updated_at": [datetime(2022, 1, 1)]

    })

    # Convert to Spark DataFrame
    sdf = ev.spark.createDataFrame(pdf)

    schema = configuration_schema(type="pyspark")

    result = ev._validate.dataframe(
        df=sdf,
        table_schema=schema,
        strict=True,
        add_missing_columns=False,
        drop_duplicates=True,
        uniqueness_fields=["name"]
    )

    assert result.count() == 1
    assert result.collect()[0]["name"] == "spark_test_config"


@pytest.mark.session_scope_test_warehouse
def test_validate_dataframe_foreign_key_enforcement(session_scope_test_warehouse):
    """Test foreign key constraint enforcement."""
    ev = session_scope_test_warehouse

    # First, let's get the existing configurations from the test warehouse
    existing_configs = ev.configurations.to_pandas()

    if len(existing_configs) > 0:
        valid_config_name = existing_configs["name"].iloc[0]
        catalog = ev.active_catalog.catalog_name
        namespace = ev.active_catalog.namespace_name

        # Create timeseries data with valid foreign key
        pdf = pd.DataFrame({
            "reference_time": [datetime(2022, 1, 1)],
            "value_time": [datetime(2022, 1, 1)],
            "configuration_name": [valid_config_name],
            "unit_name": ["m^3/s"],
            "variable_name": ["streamflow_hourly_inst"],
            "value": [100.0],
            "location_id": ["usgs-01010000"]
        })

        sdf = ev.spark.createDataFrame(pdf)
        schema = primary_timeseries_schema(type="pyspark")

        # Define foreign keys to check using dynamic catalog/namespace
        foreign_keys = [
            {
                "column": "configuration_name",
                "domain_table": f"{catalog}.{namespace}.configurations",
                "domain_column": "name"
            }
        ]

        # This should pass since config exists
        result = ev._validate.dataframe(
            df=sdf,
            table_schema=schema,
            strict=True,
            add_missing_columns=True,
            drop_duplicates=True,
            uniqueness_fields=["location_id", "value_time", "configuration_name", "variable_name"],
            foreign_keys=foreign_keys
        )

        assert result.count() == 1


@pytest.mark.session_scope_test_warehouse
def test_validate_dataframe_foreign_key_violation(session_scope_test_warehouse):
    """Test foreign key constraint violation raises error."""
    ev = session_scope_test_warehouse

    catalog = ev.active_catalog.catalog_name
    namespace = ev.active_catalog.namespace_name

    # Create timeseries data with invalid foreign key
    pdf = pd.DataFrame({
        "reference_time": [datetime(2022, 1, 1)],
        "value_time": [datetime(2022, 1, 1)],
        "configuration_name": ["nonexistent_config"],  # This doesn't exist
        "unit_name": ["m^3/s"],
        "variable_name": ["streamflow_hourly_inst"],
        "value": [100.0],
        "location_id": ["usgs-01010000"]
    })

    sdf = ev.spark.createDataFrame(pdf)
    schema = primary_timeseries_schema(type="pyspark")

    # Define foreign keys to check using dynamic catalog/namespace
    foreign_keys = [
        {
            "column": "configuration_name",
            "domain_table": f"{catalog}.{namespace}.configurations",
            "domain_column": "name"
        }
    ]

    with pytest.raises(ValueError, match="Foreign key constraint violation"):
        ev._validate.dataframe(
            df=sdf,
            table_schema=schema,
            strict=True,
            add_missing_columns=True,
            drop_duplicates=True,
            uniqueness_fields=["location_id", "value_time", "configuration_name", "variable_name"],
            foreign_keys=foreign_keys
        )
