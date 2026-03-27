"""Test the domain table load methods (load_csv, load_parquet)."""
from pathlib import Path
import pytest


TEST_STUDY_DATA_DIR = Path("tests", "data", "test_warehouse_data")
DOMAINS_DIR = Path(TEST_STUDY_DATA_DIR, "domains")


@pytest.mark.function_scope_evaluation_template
def test_configurations_load_csv(function_scope_evaluation_template):
    """Test loading configurations from a CSV file."""
    ev = function_scope_evaluation_template

    initial_count = ev.configurations.to_sdf().count()

    ev.configurations.load_csv(
        in_path=DOMAINS_DIR / "configurations.csv",
        write_mode="append"
    )

    ev.configurations._load_sdf()
    final_count = ev.configurations.to_sdf().count()

    # Should have added 2 configurations (nwm30_retro, usgs_observations)
    assert final_count == initial_count + 2

    # Verify the data was loaded correctly
    names = ev.configurations.to_pandas()["name"].tolist()
    assert "nwm30_retro" in names
    assert "usgs_observations" in names


@pytest.mark.function_scope_evaluation_template
def test_configurations_load_parquet(function_scope_evaluation_template):
    """Test loading configurations from a Parquet file."""
    ev = function_scope_evaluation_template

    initial_count = ev.configurations.to_sdf().count()

    ev.configurations.load_parquet(
        in_path=DOMAINS_DIR / "configurations.parquet",
        write_mode="append"
    )

    ev.configurations._load_sdf()
    final_count = ev.configurations.to_sdf().count()

    # Should have added 2 configurations
    assert final_count == initial_count + 2


@pytest.mark.function_scope_evaluation_template
def test_units_load_csv(function_scope_evaluation_template):
    """Test loading units from a CSV file."""
    ev = function_scope_evaluation_template

    initial_count = ev.units.to_sdf().count()

    ev.units.load_csv(
        in_path=DOMAINS_DIR / "units.csv",
        write_mode="append"
    )

    ev.units._load_sdf()
    final_count = ev.units.to_sdf().count()

    # Should have added 3 units (cms, cfs, km2)
    assert final_count == initial_count + 3

    # Verify the data was loaded correctly
    names = ev.units.to_pandas()["name"].tolist()
    assert "cms" in names
    assert "cfs" in names
    assert "km2" in names


@pytest.mark.function_scope_evaluation_template
def test_units_load_parquet(function_scope_evaluation_template):
    """Test loading units from a Parquet file."""
    ev = function_scope_evaluation_template

    initial_count = ev.units.to_sdf().count()

    ev.units.load_parquet(
        in_path=DOMAINS_DIR / "units.parquet",
        write_mode="append"
    )

    ev.units._load_sdf()
    final_count = ev.units.to_sdf().count()

    # Should have added 3 units
    assert final_count == initial_count + 3


@pytest.mark.function_scope_evaluation_template
def test_variables_load_csv(function_scope_evaluation_template):
    """Test loading variables from a CSV file."""
    ev = function_scope_evaluation_template

    initial_count = ev.variables.to_sdf().count()

    ev.variables.load_csv(
        in_path=DOMAINS_DIR / "variables.csv",
        write_mode="append"
    )

    ev.variables._load_sdf()
    final_count = ev.variables.to_sdf().count()

    # Should have added 2 variables (test_variable_1, test_variable_2)
    assert final_count == initial_count + 2

    # Verify the data was loaded correctly
    names = ev.variables.to_pandas()["name"].tolist()
    assert "test_variable_1" in names
    assert "test_variable_2" in names


@pytest.mark.function_scope_evaluation_template
def test_variables_load_parquet(function_scope_evaluation_template):
    """Test loading variables from a Parquet file."""
    ev = function_scope_evaluation_template

    initial_count = ev.variables.to_sdf().count()

    ev.variables.load_parquet(
        in_path=DOMAINS_DIR / "variables.parquet",
        write_mode="append"
    )

    ev.variables._load_sdf()
    final_count = ev.variables.to_sdf().count()

    # Should have added 2 variables
    assert final_count == initial_count + 2


@pytest.mark.function_scope_evaluation_template
def test_configurations_load_csv_with_field_mapping(function_scope_evaluation_template):
    """Test loading configurations with field mapping."""
    ev = function_scope_evaluation_template

    import pandas as pd
    import tempfile

    # Create a CSV with non-standard column names
    df = pd.DataFrame({
        "config_name": ["mapped_config"],
        "config_type": ["primary"],
        "config_description": ["A mapped configuration"]
    })

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        df.to_csv(f.name, index=False)
        temp_path = f.name

    initial_count = ev.configurations.to_sdf().count()

    ev.configurations.load_csv(
        in_path=temp_path,
        field_mapping={
            "config_name": "name",
            "config_type": "type",
            "config_description": "description"
        },
        write_mode="append"
    )

    ev.configurations._load_sdf()
    final_count = ev.configurations.to_sdf().count()

    assert final_count == initial_count + 1

    names = ev.configurations.to_pandas()["name"].tolist()
    assert "mapped_config" in names

    # Cleanup
    Path(temp_path).unlink()


@pytest.mark.function_scope_evaluation_template
def test_domain_load_create_or_replace(function_scope_evaluation_template):
    """Test loading domain table with create_or_replace write mode."""
    ev = function_scope_evaluation_template

    # First load some data
    ev.units.load_csv(
        in_path=DOMAINS_DIR / "units.csv",
        write_mode="append"
    )

    ev.units._load_sdf()
    count_after_first_load = ev.units.to_sdf().count()
    assert count_after_first_load >= 3

    # Now replace with the same data
    ev.units.load_csv(
        in_path=DOMAINS_DIR / "units.csv",
        write_mode="create_or_replace"
    )

    ev.units._load_sdf()
    count_after_replace = ev.units.to_sdf().count()

    # create_or_replace should result in exactly 3 units (the file contents)
    assert count_after_replace == 3
