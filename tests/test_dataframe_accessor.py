"""This module tests the dataframe_accessor functions."""
import pandas as pd
import geopandas as gpd
import pytest
from pathlib import Path
import logging
from teehr.visualization.dataframe_accessor import TEEHRDataFrameAccessor
import tempfile

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_init_with_dataframe():
    """Test validation with pd.DataFrame."""
    df = pd.DataFrame({
        'variable_name': ['var1', 'var1'],
        'configuration_name': ['config1', 'config1'],
        'location_id': ['loc1', 'loc1'],
        'reference_time': [None, None],
        'value_time': pd.to_datetime(['2021-01-01', '2021-01-02']),
        'value': [1, 2],
        'unit_name': ['unit1', 'unit1']
    })
    df.attrs['table_type'] = 'primary_timeseries'
    accessor = TEEHRDataFrameAccessor(df)
    assert accessor._df is not None
    assert accessor._gdf is None


def test_init_with_geodataframe():
    """Test validation with gpd.GeoDataFrame."""
    gdf = gpd.GeoDataFrame({
        'id': [1, 2],
        'name': ['loc1', 'loc2'],
        'geometry': gpd.points_from_xy([0, 1], [0, 1])
    })
    gdf.attrs['table_type'] = 'locations'
    gdf.crs = "EPSG:4326"
    accessor = TEEHRDataFrameAccessor(gdf)
    assert accessor._df is None
    assert accessor._gdf is not None


def test_validate_missing_table_type():
    """Test missing table type."""
    df = pd.DataFrame({'a': [1, 2, 3]})
    with pytest.raises(AttributeError):
        TEEHRDataFrameAccessor._validate(None, df)


def test_validate_missing_fields():
    """Test missing columns."""
    df = pd.DataFrame({'a': [1, 2, 3]})
    df.attrs['table_type'] = 'primary_timeseries'
    with pytest.raises(AttributeError):
        TEEHRDataFrameAccessor._validate(None, df)


def test_primary_timeseries_plot(tmpdir):
    """Test timeseries plot."""
    df = pd.DataFrame({
        'variable_name': ['var1', 'var1'],
        'configuration_name': ['config1', 'config1'],
        'location_id': ['loc1', 'loc1'],
        'reference_time': ['2021-01-01', None],
        'value_time': pd.to_datetime(['2021-01-01', '2021-01-02']),
        'value': [1, 2],
        'unit_name': ['unit1', 'unit1']
    })
    df.attrs['table_type'] = 'primary_timeseries'
    accessor = TEEHRDataFrameAccessor(df)
    accessor.timeseries_plot(output_dir=tmpdir)
    assert Path(tmpdir, 'primary_timeseries_plot_var1.html').is_file()


def test_secondary_timeseries_plot(tmpdir):
    """Test secondary timeseries plot."""
    df = pd.DataFrame({
        'variable_name': ['var1', 'var1'],
        'configuration_name': ['config1', 'config1'],
        'location_id': ['loc1', 'loc1'],
        'reference_time': ['2021-01-01', None],
        'value_time': pd.to_datetime(['2021-01-01', '2021-01-02']),
        'value': [1, 2],
        'unit_name': ['unit1', 'unit1'],
        'member': [None, 'member1']
    })
    df.attrs['table_type'] = 'secondary_timeseries'
    accessor = TEEHRDataFrameAccessor(df)
    accessor.timeseries_plot(output_dir=tmpdir)
    assert Path(tmpdir, 'secondary_timeseries_plot_var1.html').is_file()


def test_locations_map(tmpdir):
    """Test locations table mapping."""
    gdf = gpd.GeoDataFrame({
        'id': [1, 2],
        'name': ['loc1', 'loc2'],
        'geometry': gpd.points_from_xy([0, 1], [0, 1])
    })
    gdf.attrs['table_type'] = 'locations'
    gdf.crs = "EPSG:4326"
    accessor = TEEHRDataFrameAccessor(gdf)
    accessor.locations_map(output_dir=tmpdir)
    assert Path(tmpdir, 'location_map.html').is_file()


def test_location_attributes_map(tmpdir):
    """Test location_attributes table mapping."""
    gdf = gpd.GeoDataFrame({
        'location_id': [1, 1],
        'attribute_name': ['attr1', 'attr2'],
        'value': [10, 20],
        'geometry': gpd.points_from_xy([0, 0], [0, 0])
    })
    gdf.attrs['table_type'] = 'location_attributes'
    gdf.crs = "EPSG:4326"
    accessor = TEEHRDataFrameAccessor(gdf)
    accessor.location_attributes_map(output_dir=tmpdir)
    assert Path(tmpdir, 'location_attributes_map.html').is_file()


def test_location_crosswalks_map(tmpdir):
    """Test location_crosswalks table mapping."""
    gdf = gpd.GeoDataFrame({
        'primary_location_id': [1, 2],
        'secondary_location_id': [3, 4],
        'geometry': gpd.points_from_xy([0, 1], [0, 1])
    })
    gdf.attrs['table_type'] = 'location_crosswalks'
    gdf.crs = "EPSG:4326"
    accessor = TEEHRDataFrameAccessor(gdf)
    accessor.location_crosswalks_map(output_dir=tmpdir)
    assert Path(tmpdir, 'location_crosswalks_map.html').is_file()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tmpdir:
        test_init_with_dataframe()
        test_init_with_geodataframe()
        test_validate_missing_table_type()
        test_validate_missing_fields()
        test_primary_timeseries_plot(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tmpdir
            )
        )
        test_secondary_timeseries_plot(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tmpdir
            )
        )
        test_locations_map(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tmpdir
            )
        )
        test_location_attributes_map(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tmpdir
            )
        )
        test_location_crosswalks_map(
            tempfile.mkdtemp(
                prefix="4-",
                dir=tmpdir
            )
        )
        logger.info("All tests passed.")
