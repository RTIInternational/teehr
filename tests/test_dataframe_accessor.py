import unittest
import pandas as pd
from unittest.mock import patch
from pathlib import Path
from teehr.visualization.dataframe_accessor import TEEHRDataFrameAccessor
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestTEEHRDataFrameAccessor(unittest.TestCase):
    """Test class created to evaluate the dataframe_accessor methods."""

    def setUp(self):
        """Create sample data for test object."""
        # Create a sample DataFrame for testing
        data = {
            'location_id': [1, 2, 1, 2],
            'variable_name': ['var1', 'var1', 'var2', 'var2'],
            'configuration_name': ['config1', 'config2', 'config1', 'config2'],
            'unit_name': ['unit1', 'unit1', 'unit2', 'unit2'],
            'value_time': pd.date_range(start='1/1/2022', periods=4, freq='D'),
            'value': [10, 20, 30, 40]
        }
        self.df = pd.DataFrame(data)

    def test_initialization(self):
        """Initialize test object."""
        # Test that the accessor initializes correctly
        accessor = self.df.teehr
        self.assertIsInstance(accessor, TEEHRDataFrameAccessor)

    def test_validation(self):
        """Test validation method."""
        # Test that validation raises an error for missing 'location_id'
        with self.assertRaises(AttributeError):
            df_invalid = self.df.drop(columns=['location_id'])
            df_invalid.teehr

        # Test that validation raises an error for empty DataFrame
        with self.assertRaises(AttributeError):
            df_empty = pd.DataFrame(columns=self.df.columns)
            df_empty.teehr

    def test_get_unique_values(self):
        """Test unique values method."""
        accessor = self.df.teehr
        unique_values = accessor._get_unique_values(self.df)
        expected_values = {
            'location_id': [1, 2],
            'variable_name': ['var1', 'var2'],
            'configuration_name': ['config1', 'config2'],
            'unit_name': ['unit1', 'unit2'],
            'value_time': list(self.df['value_time'].unique()),
            'value': [10, 20, 30, 40]
        }
        self.assertEqual(unique_values, expected_values)

    def test_timeseries_schema(self):
        """Test generation of default schema."""
        accessor = self.df.teehr
        schema = accessor._timeseries_schema()
        expected_schema = {
            'var1': [('config1', 1), ('config2', 2)],
            'var2': [('config1', 1), ('config2', 2)]
        }
        self.assertEqual(schema, expected_schema)

    def test_timeseries_generate_plot(self):
        """Test plot generation."""
        accessor = self.df.teehr
        schema = accessor._timeseries_schema()
        # Generate the plot and save it to the current directory
        # (triggers the shown condition)
        accessor._timeseries_generate_plot(schema, self.df, 'var1', None)

        # Check if the file exists in the current directory
        current_dir = Path(__file__).parent
        plot_file = current_dir / 'test_dataframe_accessor.html'
        logger.info(f"Checking if {plot_file} exists.")
        self.assertTrue(plot_file.exists())

        # Clean up the file
        plot_file.unlink()

    @patch('pathlib.Path.mkdir')
    @patch('pathlib.Path.exists', return_value=False)
    def test_timeseries_plot(self, mock_exists, mock_mkdir):
        """Test timeseries plot with a custom output directory."""
        accessor = self.df.teehr
        output_dir = Path(__file__).parent
        mock_exists.return_value = False  # Ensure the directory does not exist
        accessor.timeseries_plot(output_dir=output_dir)
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

        # Check if the files exist
        var1_file = output_dir / "timeseries_plot_var1.html"
        var2_file = output_dir / "timeseries_plot_var2.html"
        logger.info(f"Checking if {var1_file} exists.")
        if not os.path.isfile(var1_file):
            raise AssertionError(f"File {var1_file} does not exist.")
        logger.info(f"Checking if {var2_file} exists.")
        if not os.path.isfile(var2_file):
            raise AssertionError(f"File {var2_file} does not exist.")

        # Clean up the files
        var1_file.unlink()
        var2_file.unlink()


if __name__ == '__main__':
    unittest.main()
