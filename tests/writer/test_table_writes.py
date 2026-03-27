"""Test the Writer class."""

import pyspark.sql as ps
from pyspark.sql.types import StructType, StructField, StringType
import pytest
from datetime import datetime
import pandas as pd


@pytest.mark.function_scope_evaluation_template
def test_table_writes(function_scope_evaluation_template):
    """Test creating a new study."""
    ev = function_scope_evaluation_template

    df = pd.DataFrame({
        "name": ["ft/s"],
        "long_name": ["Feet per second"]
    })

    # Can pass a spark dataframe, pandas dataframe, or named view (str)
    ev._write.to_warehouse(
        source_data=df,
        table_name="units",
        write_mode="append",
    )


@pytest.mark.function_scope_evaluation_template
def test_insert_write_mode(function_scope_evaluation_template):
    """Test the 'insert' write mode (INSERT INTO without duplicate checking)."""
    ev = function_scope_evaluation_template

    df = pd.DataFrame({
        "name": ["f/k"],
        "long_name": ["Fake unit for testing insert mode"]
    })

    # First insert
    ev._write.to_warehouse(
        source_data=df,
        table_name="units",
        write_mode="insert",
    )

    count_after_first = ev.units.to_sdf().count()

    # Second insert of the same data - should create a duplicate
    ev._write.to_warehouse(
        source_data=df,
        table_name="units",
        write_mode="insert",
    )

    count_after_second = ev.units.to_sdf().count()

    # insert mode does not deduplicate, so count should increase
    assert count_after_second == count_after_first + 1


@pytest.mark.function_scope_evaluation_template
def test_delete_from_with_filter(function_scope_evaluation_template):
    """Test delete_from with a filter condition."""
    ev = function_scope_evaluation_template

    df = pd.DataFrame({
        "name": ["m/s", "ft/s"],
        "long_name": ["Meters per second", "Feet per second"]
    })

    ev._write.to_warehouse(
        source_data=df,
        table_name="units",
        write_mode="append",
    )

    initial_count = ev.units.to_sdf().count()
    assert initial_count >= 2

    # Delete one unit using a SQL string filter
    deleted_count = ev._write.delete_from(
        table_name="units",
        filters=["name = 'm/s'"],
    )

    assert deleted_count == 1

    # Verify the row was actually deleted
    ev.units._load_sdf()
    remaining_count = ev.units.to_sdf().count()
    assert remaining_count == initial_count - 1

    # Verify the correct row was deleted
    names = ev.units.to_sdf().select("name").rdd.flatMap(lambda x: x).collect()
    assert "m/s" not in names
    assert "ft/s" in names


@pytest.mark.function_scope_evaluation_template
def test_delete_from_dry_run(function_scope_evaluation_template):
    """Test delete_from dry_run returns a DataFrame without deleting."""
    ev = function_scope_evaluation_template

    df = pd.DataFrame({
        "name": ["m/s", "ft/s"],
        "long_name": ["Meters per second", "Feet per second"]
    })
    ev._write.to_warehouse(
        source_data=df,
        table_name="units",
        write_mode="append",
    )

    initial_count = ev.units.to_sdf().count()

    # Dry run - should return a DataFrame without deleting
    result_sdf = ev._write.delete_from(
        table_name="units",
        filters=["name = 'm/s'"],
        dry_run=True,
    )

    # Result should be a Spark DataFrame
    assert isinstance(result_sdf, ps.DataFrame)

    # Should contain exactly the rows that match the filter
    assert result_sdf.count() == 1
    assert result_sdf.collect()[0]["name"] == "m/s"

    # Original table should not have been modified
    ev.units._load_sdf()
    assert ev.units.to_sdf().count() == initial_count


@pytest.mark.function_scope_evaluation_template
def test_delete_from_no_filter(function_scope_evaluation_template):
    """Test delete_from with no filter deletes all rows."""
    ev = function_scope_evaluation_template

    df = pd.DataFrame({
        "name": ["m/s", "ft/s"],
        "long_name": ["Meters per second", "Feet per second"]
    })
    ev._write.to_warehouse(
        source_data=df,
        table_name="units",
        write_mode="append",
    )

    initial_count = ev.units.to_sdf().count()
    assert initial_count >= 2

    # Delete all rows
    deleted_count = ev._write.delete_from(table_name="units")

    assert deleted_count == initial_count

    # Verify all rows were deleted
    ev.units._load_sdf()
    assert ev.units.to_sdf().count() == 0


@pytest.mark.function_scope_evaluation_template
def test_delete_from_dict_filter(function_scope_evaluation_template):
    """Test delete_from with a dictionary filter."""
    ev = function_scope_evaluation_template

    df = pd.DataFrame({
        "name": ["m/s", "ft/s"],
        "long_name": ["Meters per second", "Feet per second"]
    })
    ev._write.to_warehouse(
        source_data=df,
        table_name="units",
        write_mode="append",
    )

    initial_count = ev.units.to_sdf().count()

    # Delete using a dict filter
    deleted_count = ev._write.delete_from(
        table_name="units",
        filters={"column": "name", "operator": "=", "value": "ft/s"},
    )

    assert deleted_count == 1

    ev.units._load_sdf()
    assert ev.units.to_sdf().count() == initial_count - 1


@pytest.mark.function_scope_evaluation_template
def test_table_delete_method_dict_filter(function_scope_evaluation_template):
    """Test the delete() method on a table instance with a dict filter."""
    ev = function_scope_evaluation_template

    df = pd.DataFrame({
        "name": ["m/s", "ft/s"],
        "long_name": ["Meters per second", "Feet per second"]
    })

    ev._write.to_warehouse(
        source_data=df,
        table_name="units",
        write_mode="append",
    )

    initial_count = ev.units.to_sdf().count()

    # Delete using a dict filter on the table instance
    deleted_count = ev.units.delete(
        filters={"column": "name", "operator": "=", "value": "m/s"},
    )

    assert deleted_count == 1

    ev.units._load_sdf()
    assert ev.units.to_sdf().count() == initial_count - 1


@pytest.mark.function_scope_evaluation_template
def test_table_delete_method(function_scope_evaluation_template):
    """Test the delete() method on a table instance."""
    ev = function_scope_evaluation_template

    df = pd.DataFrame({
        "name": ["m/s", "ft/s"],
        "long_name": ["Meters per second", "Feet per second"]
    })

    ev.units.load_dataframe(
        df=df,
        write_mode="append",
    )

    initial_count = ev.units.to_sdf().count()
    assert initial_count >= 2

    # Delete one row via ev.table().delete()
    deleted_count = ev.table("units").delete(
        filters=["name = 'm/s'"],
    )

    assert deleted_count == 1

    ev.units._load_sdf()
    assert ev.units.to_sdf().count() == initial_count - 1


@pytest.mark.function_scope_evaluation_template
def test_table_delete_method_dry_run(function_scope_evaluation_template):
    """Test the delete() dry_run on a table instance."""
    ev = function_scope_evaluation_template

    df = pd.DataFrame({
        "name": ["m/s", "ft/s"],
        "long_name": ["Meters per second", "Feet per second"]
    })

    ev.units.load_dataframe(
        df=df,
        write_mode="append",
    )

    initial_count = ev.units.to_sdf().count()

    # Dry run via named table property
    result_sdf = ev.units.delete(
        filters=["name = 'ft/s'"],
        dry_run=True,
    )

    assert isinstance(result_sdf, ps.DataFrame)
    assert result_sdf.count() == 1
    assert result_sdf.collect()[0]["name"] == "ft/s"

    # Table should be unchanged
    ev.units._load_sdf()
    assert ev.units.to_sdf().count() == initial_count


@pytest.mark.function_scope_evaluation_template
def test_table_delete_method_no_filter(function_scope_evaluation_template):
    """Test delete() on a table instance with no filter deletes all rows."""
    ev = function_scope_evaluation_template

    df = pd.DataFrame({
        "name": ["m/s", "ft/s"],
        "long_name": ["Meters per second", "Feet per second"]
    })
    ev.units.load_dataframe(
        df=df,
        write_mode="append",
    )

    initial_count = ev.units.to_sdf().count()
    assert initial_count >= 2

    # Delete all rows via ev.units.delete()
    deleted_count = ev.units.delete()

    assert deleted_count == initial_count

    ev.units._load_sdf()
    assert ev.units.to_sdf().count() == 0


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
    ev.table("my_user_table").load_dataframe(
        df=sdf,
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

    ev.table("my_drop_test_table").load_dataframe(
        df=sdf,
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


@pytest.mark.function_scope_evaluation_template
def test_load_arbitrary_dataframe_to_new_table(function_scope_evaluation_template):
    """Test loading an arbitrary dataframe to a new table via ev.table().load_dataframe()."""
    ev = function_scope_evaluation_template

    import pandas as pd

    # Create an arbitrary dataframe with custom columns
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["alpha", "beta", "gamma"],
        "value": [10.5, 20.0, 30.5],
        "category": ["A", "B", "A"]
    })

    # Load to a new arbitrary table (not a core table)
    ev.table("my_arbitrary_results").load_dataframe(
        df=df,
        write_mode="create_or_replace"
    )

    # Verify the table was created and data was written
    result_df = ev.table("my_arbitrary_results").to_pandas()

    assert len(result_df) == 3
    assert set(result_df.columns) == {"id", "name", "value", "category", "created_at", "updated_at"}
    assert list(result_df["name"]) == ["alpha", "beta", "gamma"]

    # Verify it's not a core table
    assert ev.table("my_arbitrary_results").is_core_table is False
