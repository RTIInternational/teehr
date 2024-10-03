import pytest
import pandas as pd
from pathlib import Path
from teehr.visualization.dataframe_accessor import TEEHRDataFrameAccessor
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
def sample_dataframe():
    """Create sample data for test object."""
    data = {
        'location_id': [1, 2, 1, 2],
        'variable_name': ['var1', 'var1', 'var2', 'var2'],
        'configuration_name': ['config1', 'config2', 'config1', 'config2'],
        'unit_name': ['unit1', 'unit1', 'unit2', 'unit2'],
        'value_time': pd.date_range(start='1/1/2022', periods=4, freq='D'),
        'value': [10, 20, 30, 40]
    }
    return pd.DataFrame(data)


def test_initialization(sample_dataframe):
    """Initialize test object."""
    # Test that the accessor initializes correctly
    accessor = sample_dataframe.teehr
    assert isinstance(accessor, TEEHRDataFrameAccessor)


def test_validation(sample_dataframe):
    """Test validation method."""
    # Test that validation raises an error for missing 'location_id'
    with pytest.raises(AttributeError):
        df_invalid = sample_dataframe.drop(columns=['location_id'])
        df_invalid.teehr

    # Test that validation raises an error for empty DataFrame
    with pytest.raises(AttributeError):
        df_empty = pd.DataFrame(columns=sample_dataframe.columns)
        df_empty.teehr


def test_get_unique_values(sample_dataframe):
    """Test unique values method."""
    accessor = sample_dataframe.teehr
    unique_values = accessor._get_unique_values(sample_dataframe)
    expected_values = {
        'location_id': [1, 2],
        'variable_name': ['var1', 'var2'],
        'configuration_name': ['config1', 'config2'],
        'unit_name': ['unit1', 'unit2'],
        'value_time': list(sample_dataframe['value_time'].unique()),
        'value': [10, 20, 30, 40]
    }
    assert unique_values == expected_values


def test_timeseries_schema(sample_dataframe):
    """Test generation of default schema."""
    accessor = sample_dataframe.teehr
    schema = accessor._timeseries_schema()
    expected_schema = {
        'var1': [('config1', 1), ('config2', 2)],
        'var2': [('config1', 1), ('config2', 2)]
    }
    assert schema == expected_schema


def test_timeseries_generate_plot(sample_dataframe):
    """Test plot generation."""
    accessor = sample_dataframe.teehr
    schema = accessor._timeseries_schema()
    # Generate the plot and save it to the current directory
    accessor._timeseries_generate_plot(schema, sample_dataframe, 'var1', None)

    # Check if the file exists in the current directory
    current_dir = Path(__file__).parent
    plot_file = current_dir / 'test_dataframe_accessor.html'
    logger.info(f"Checking if {plot_file} exists.")
    assert plot_file.exists()

    # Clean up the file
    plot_file.unlink()


def test_timeseries_plot(mocker, sample_dataframe):
    """Test timeseries plot with a custom output directory."""
    accessor = sample_dataframe.teehr
    # Mock the Path.mkdir method
    mock_mkdir = mocker.patch('pathlib.Path.mkdir')

    # Call with specified directory to trigger the save() condition
    output_dir = Path(__file__).parent
    accessor.timeseries_plot(output_dir=output_dir)
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

    # Check if the files exist
    var1_file = output_dir / "timeseries_plot_var1.html"
    var2_file = output_dir / "timeseries_plot_var2.html"
    logger.info(f"Checking if {var1_file} exists.")
    assert var1_file.exists()
    logger.info(f"Checking if {var2_file} exists.")
    assert var2_file.exists()

    # Clean up the files
    var1_file.unlink()
    var2_file.unlink()
