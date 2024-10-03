import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch
from teehr.loading.add_domains import add_variable
from teehr.models.tables import Variable
import pandera as pa


@pytest.fixture
def mock_dataset_path(tmp_path):
    return tmp_path


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_variable_single(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test adding a single variable."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["streamflow_hourly_inst"],
        "long_name": ["Hourly inst. Q"]
    })

    variable = Variable(
        name="streamflow_daily_mean",
        long_name="Daily mean Q",
    )

    add_variable(mock_dataset_path, variable)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "variables", "variables.csv")
    )
    mock_to_csv.assert_called_once()


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_variable_multiple(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test adding multiple variables."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["streamflow_hourly_inst"],
        "long_name": ["Hourly inst. Q"]
    })

    variables = [
        Variable(
            name="streamflow_daily_mean",
            long_name="Daily mean Q",
        ),
        Variable(
            name="streamflow_daily_mean2",
            long_name="Daily mean Q",
        )
    ]

    add_variable(mock_dataset_path, variables)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "variables", "variables.csv")
    )
    mock_to_csv.assert_called_once()


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_variable_validation_error(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test that a variable with a comma in the name raises an error."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["streamflow_hourly_inst"],
        "long_name": ["Hourly inst. Q"]
    })

    invalid_variable = Variable(
        name="streamflow_daily,mean",
        long_name="Daily mean Q",
    )

    with pytest.raises(pa.errors.SchemaError):
        add_variable(mock_dataset_path, invalid_variable)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "variables", "variables.csv")
    )
    mock_to_csv.assert_not_called()


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_variable_validation_error2(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test that a variable with a space in the name raises an error."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["streamflow_hourly_inst"],
        "long_name": ["Hourly inst. Q"]
    })

    invalid_variable = Variable(
        name="streamflow_daily mean",
        long_name="Daily mean Q",
    )

    with pytest.raises(pa.errors.SchemaError):
        add_variable(mock_dataset_path, invalid_variable)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "variables", "variables.csv")
    )
    mock_to_csv.assert_not_called()


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_variable_validation_error3(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test that a variable with a space in the name raises an error."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["streamflow_hourly_inst"],
        "long_name": ["Hourly inst. Q"]
    })

    invalid_variable = Variable(
        name="streamflow/daily/mean",
        long_name="Daily mean Q",
    )

    with pytest.raises(pa.errors.SchemaError):
        add_variable(mock_dataset_path, invalid_variable)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "variables", "variables.csv")
    )
    mock_to_csv.assert_not_called()