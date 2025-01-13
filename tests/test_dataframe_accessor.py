import pytest
import pandas as pd
import geopandas as gpd
from teehr.visualization.dataframe_accessor import TEEHRDataFrameAccessor


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


def test_timeseries_plot():
    """Test timeseries plot."""
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
    accessor.timeseries_plot(output_dir=None)


def test_locations_map():
    """Test locations table mapping."""
    gdf = gpd.GeoDataFrame({
        'id': [1, 2],
        'name': ['loc1', 'loc2'],
        'geometry': gpd.points_from_xy([0, 1], [0, 1])
    })
    gdf.attrs['table_type'] = 'locations'
    gdf.attrs['fields'] = ['id', 'name']
    gdf.crs = "EPSG:4326"
    accessor = TEEHRDataFrameAccessor(gdf)
    accessor.locations_map(output_dir=None)


def test_location_attributes_map():
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
    accessor.location_attributes_map(output_dir=None)


def test_location_crosswalks_map():
    """Test location_crosswalks table mapping."""
    gdf = gpd.GeoDataFrame({
        'primary_location_id': [1, 2],
        'secondary_location_id': [3, 4],
        'geometry': gpd.points_from_xy([0, 1], [0, 1])
    })
    gdf.attrs['table_type'] = 'location_crosswalks'
    gdf.crs = "EPSG:4326"
    accessor = TEEHRDataFrameAccessor(gdf)
    accessor.location_crosswalks_map(output_dir=None)
