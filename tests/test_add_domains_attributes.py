import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch
from teehr.loading.add_domains import add_attribute
from teehr.models.tables import Attribute
import pandera as pa


@pytest.fixture
def mock_dataset_path(tmp_path):
    return tmp_path


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_attribute_single(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test adding a single attribute."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["attr0"],
        "type": ["categorical"],
        "description": ["A categorical attribute"]
    })

    attribute = Attribute(
        name="attr1",
        type="categorical",
        description="A categorical attribute"
    )

    add_attribute(mock_dataset_path, attribute)

    mock_read_csv.assert_called_once_with(Path(mock_dataset_path, "attributes", "attributes.csv"))
    mock_to_csv.assert_called_once()


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_attribute_multiple(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test adding multiple attributes."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["attr0"],
        "type": ["categorical"],
        "description": ["A categorical attribute"]
    })

    attributes = [
        Attribute(
            name="attr1",
            type="categorical",
            description="A categorical attribute"
        ),
        Attribute(
            name="attr2",
            type="categorical",
            description="A categorical attribute"
        )
    ]

    add_attribute(mock_dataset_path, attributes)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "attributes", "attributes.csv")
    )
    mock_to_csv.assert_called_once()


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_attribute_validation_error(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test that a attribute with a comma in the name raises an error."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["attr0"],
        "type": ["categorical"],
        "description": ["A categorical attribute"]
    })

    invalid_attribute = Attribute(
        name="attr1,invalid",
        type="categorical",
        description="A categorical attribute"
    )

    with pytest.raises(pa.errors.SchemaError):
        add_attribute(mock_dataset_path, invalid_attribute)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "attributes", "attributes.csv")
    )
    mock_to_csv.assert_not_called()


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_attribute_validation_error2(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test that a attribute with a space in the name raises an error."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["attr0"],
        "type": ["categorical"],
        "description": ["A categorical attribute"]
    })

    invalid_attribute = Attribute(
        name="attr1 space",
        type="categorical",
        description="A categorical attribute"
    )

    with pytest.raises(pa.errors.SchemaError):
        add_attribute(mock_dataset_path, invalid_attribute)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "attributes", "attributes.csv")
    )
    mock_to_csv.assert_not_called()


@patch("teehr.loading.add_domains.pd.read_csv")
@patch("teehr.loading.add_domains.pd.DataFrame.to_csv")
def test_add_attribute_validation_error3(
    mock_to_csv,
    mock_read_csv,
    mock_dataset_path
):
    """Test that a attribute with an invalid type raises an error."""
    mock_read_csv.return_value = pd.DataFrame({
        "name": ["attr0"],
        "type": ["categorical"],
        "description": ["A categorical attribute"]
    })

    invalid_attribute = Attribute(
        name="attr1",
        type="invalid_type",
        description="A categorical attribute"
    )

    with pytest.raises(pa.errors.SchemaError):
        add_attribute(mock_dataset_path, invalid_attribute)

    mock_read_csv.assert_called_once_with(
        Path(mock_dataset_path, "attributes", "attributes.csv")
    )
    mock_to_csv.assert_not_called()