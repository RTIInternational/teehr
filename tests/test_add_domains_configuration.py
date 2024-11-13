import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch
from teehr.loading.add_domains import add_configuration
from teehr.models.tables import Configuration
import pandera as pa


@pytest.fixture
def mock_dataset_path(tmp_path):
    return tmp_path


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_configuration_single(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test adding a single configuration."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["config0"],
        "type": ["primary"],
        "description": ["A primary configuration"]
    })

    configuration = Configuration(
        name="config1",
        type="primary",
        description="A primary configuration"
    )

    add_configuration(mock_dataset_path, configuration)

    mock_read_csv.assert_called_once_with(Path(mock_dataset_path, "configurations", "configurations.csv"))
    mock_to_csv.assert_called_once()


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_configuration_multiple(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test adding multiple configurations."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["config0"],
        "type": ["primary"],
        "description": ["A primary configuration"]
    })

    configurations = [
        Configuration(
            name="config1",
            type="primary",
            description="A primary configuration"
        ),
        Configuration(
            name="config2",
            type="secondary",
            description="A secondary configuration"
        )
    ]

    add_configuration(mock_dataset_path, configurations)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "configurations", "configurations.csv")
    )
    mock_to_csv.assert_called_once()


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_configuration_validation_error(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test that a configuration with a comma in the name raises an error."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["config0"],
        "type": ["primary"],
        "description": ["A primary configuration"]
    })

    invalid_configuration = Configuration(
        name="config1,invalid",
        type="primary",
        description="A primary configuration"
    )

    with pytest.raises(pa.errors.SchemaError):
        add_configuration(mock_dataset_path, invalid_configuration)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "configurations", "configurations.csv")
    )
    mock_to_csv.assert_not_called()


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_configuration_validation_error2(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test that a configuration with a space in the name raises an error."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["config0"],
        "type": ["primary"],
        "description": ["A primary configuration"]
    })

    invalid_configuration = Configuration(
        name="config1 space",
        type="primary",
        description="A primary configuration"
    )

    with pytest.raises(pa.errors.SchemaError):
        add_configuration(mock_dataset_path, invalid_configuration)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "configurations", "configurations.csv")
    )
    mock_to_csv.assert_not_called()


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_configuration_validation_error3(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test that a configuration with an invalid type raises an error."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["config0"],
        "type": ["primary"],
        "description": ["A primary configuration"]
    })

    invalid_configuration = Configuration(
        name="config1",
        type="invalid_type",
        description="A primary configuration"
    )

    with pytest.raises(pa.errors.SchemaError):
        add_configuration(mock_dataset_path, invalid_configuration)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "configurations", "configurations.csv")
    )
    mock_to_csv.assert_not_called()