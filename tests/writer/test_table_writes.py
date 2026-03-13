"""Test the Writer class."""

from pyspark.sql.types import StructType, StructField, StringType
import pytest


@pytest.mark.function_scope_evaluation_template
def test_table_writes(function_scope_evaluation_template):
    """Test creating a new study."""
    ev = function_scope_evaluation_template

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("long_name", StringType(), True)
    ])

    sdf = ev.spark.createDataFrame(
      data=[
        ("ft/s", "Feet per second"),
      ],
      schema=schema
    )

    df = sdf.toPandas()

    # Can pass a spark dataframe, pandas dataframe, or named view (str)
    ev.write.to_warehouse(
        source_data=df,
        table_name="units",
        write_mode="append",
    )


@pytest.mark.function_scope_evaluation_template
def test_drop_user_table(function_scope_evaluation_template):
    """Test dropping a user-created (non-core) table."""
    ev = function_scope_evaluation_template

    # Create a simple user table to drop
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("value", StringType(), True)
    ])

    sdf = ev.spark.createDataFrame(
        data=[("a", "1"), ("b", "2")],
        schema=schema
    )

    # Write a user-created table
    ev.write.to_warehouse(
        source_data=sdf,
        table_name="my_user_table",
        write_mode="create_or_replace",
    )

    # Verify the table exists and is not a core table
    tbl = ev.table("my_user_table")
    assert tbl.is_core_table is False

    # Drop via table instance
    tbl.drop()

    # Verify the table is gone
    tables_df = ev.list_tables()
    assert "my_user_table" not in tables_df["name"].values


@pytest.mark.function_scope_evaluation_template
def test_drop_table_via_evaluation(function_scope_evaluation_template):
    """Test dropping a user-created table via ev.drop_table()."""
    ev = function_scope_evaluation_template

    schema = StructType([
        StructField("name", StringType(), True),
    ])

    sdf = ev.spark.createDataFrame(
        data=[("test_value",)],
        schema=schema
    )

    ev.write.to_warehouse(
        source_data=sdf,
        table_name="my_drop_test_table",
        write_mode="create_or_replace",
    )

    # Drop via ev.drop_table()
    ev.drop_table("my_drop_test_table")

    # Verify the table is gone
    tables_df = ev.list_tables()
    assert "my_drop_test_table" not in tables_df["name"].values


@pytest.mark.function_scope_evaluation_template
def test_drop_core_table_raises(function_scope_evaluation_template):
    """Test that dropping a core table raises a ValueError."""
    ev = function_scope_evaluation_template

    # Verify core tables cannot be dropped
    core_tables = [
        "primary_timeseries",
        "secondary_timeseries",
        "locations",
        "units",
        "variables",
        "configurations",
        "attributes",
        "location_attributes",
        "location_crosswalks",
    ]

    for table_name in core_tables:
        with pytest.raises(ValueError, match="Cannot drop core table"):
            ev.drop_table(table_name)


@pytest.mark.function_scope_evaluation_template
def test_is_core_table_property(function_scope_evaluation_template):
    """Test the is_core_table property on table instances."""
    ev = function_scope_evaluation_template

    # Core tables should return True
    assert ev.table("primary_timeseries").is_core_table is True
    assert ev.table("locations").is_core_table is True
    assert ev.table("units").is_core_table is True

    # User-created table names should return False
    assert ev.table("my_custom_results").is_core_table is False
    assert ev.table("joined_timeseries_materialized").is_core_table is False
