import unittest
import pandas as pd
from unittest.mock import patch
from pathlib import Path
from teehr.visualization.dataframe_accessor import TEEHRDataFrameAccessor


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

    def test_timeseries_unique_values(self):
        """Test unique values method."""
        accessor = self.df.teehr
        unique_values = accessor._timeseries_unique_values(self.df)
        expected_values = {
            'location_id': [1, 2],
            'variable_name': ['var1', 'var2'],
            'configuration_name': ['config1', 'config2'],
            'unit_name': ['unit1', 'unit2'],
            'value_time': list(self.df['value_time'].unique()),
            'value': [10, 20, 30, 40]
        }
        self.assertEqual(unique_values, expected_values)

    def test_timeseries_default_schema(self):
        """Test generation of default schema."""
        accessor = self.df.teehr
        schema = accessor._timeseries_default_schema()
        expected_schema = {
            'var1': [('config1', 1), ('config1', 2),
                     ('config2', 1), ('config2', 2)],
            'var2': [('config1', 1), ('config1', 2),
                     ('config2', 1), ('config2', 2)]
        }
        self.assertEqual(schema, expected_schema)

    @patch('bokeh.plotting.show')
    def test_timeseries_generate_plot(self, mock_show):
        """Test plot generation."""
        accessor = self.df.teehr
        schema = accessor._timeseries_default_schema()
        accessor._timeseries_generate_plot(schema, self.df, 'var1', None)
        mock_show.assert_called_once()

    @patch('bokeh.plotting.save')
    @patch('pathlib.Path.exists', return_value=False)
    @patch('pathlib.Path.mkdir')
    def test_timeseries_plot(self, mock_mkdir, mock_exists, mock_save):
        """Test timeseries plot."""
        accessor = self.df.teehr
        output_dir = Path(Path().home(), "temp", "test_fig")
        output_dir.mkdir(parents=True, exist_ok=True)
        accessor.timeseries_plot(output_dir=output_dir)
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_save.assert_called()


if __name__ == '__main__':
    unittest.main()
