import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch
from teehr.loading.add_domains import add_unit
from teehr.models.tables import Unit
import pandera as pa


@pytest.fixture
def mock_dataset_path(tmp_path):
    return tmp_path


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_unit_single(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test adding a single unit."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["m^3/s"],
        "long_name": ["Cubic meters per second"]
    })

    unit = Unit(
        name="na^2/me",
        long_name="Long name",
    )

    add_unit(mock_dataset_path, unit)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "units", "units.csv")
    )
    mock_to_csv.assert_called_once()


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_unit_multiple(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test adding multiple units."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["m^3/s"],
        "long_name": ["Cubic meters per second"]
    })

    units = [
        Unit(
            name="na^2/me",
            long_name="Long name",
        ),
        Unit(
            name="na^3/me^3",
            long_name="Long name 2",
        ),
    ]

    add_unit(mock_dataset_path, units)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "units", "units.csv")
    )
    mock_to_csv.assert_called_once()


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_unit_validation_error(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test that a unit with a comma in the name raises an error."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["m^3/s"],
        "long_name": ["Cubic meters per second"]
    })

    invalid_unit = Unit(
        name="na^2,me",
        long_name="Long name",
    )

    with pytest.raises(pa.errors.SchemaError):
        add_unit(mock_dataset_path, invalid_unit)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "units", "units.csv")
    )
    mock_to_csv.assert_not_called()


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_unit_validation_error2(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test that a unit with a space in the name raises an error."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["m^3/s"],
        "long_name": ["Cubic meters per second"]
    })

    invalid_unit = Unit(
        name="na^2 me",
        long_name="Long name",
    )

    with pytest.raises(pa.errors.SchemaError):
        add_unit(mock_dataset_path, invalid_unit)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "units", "units.csv")
    )
    mock_to_csv.assert_not_called()
