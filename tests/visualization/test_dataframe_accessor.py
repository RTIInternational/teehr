"""This module tests the dataframe_accessor functions."""
import pandas as pd
import geopandas as gpd
import pytest
import teehr
import shutil
from pathlib import Path
import logging
from teehr.visualization.dataframe_accessor import TEEHRDataFrameAccessor
from teehr.examples.setup_nwm_grid_example import setup_nwm_example
import tempfile

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TEST_STUDY_DATA_DIR = Path("tests", "data", "fetch_nwm_grids")


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
    """Test locations table mapping with test data."""
    # Define the directory where the Evaluation will be created.
    test_eval_dir = Path(tmpdir, 'test_eval_home')
    if not test_eval_dir.is_dir():
        test_eval_dir.mkdir(parents=True)

    # Setup the example evaluation using data from the TEEHR repository.
    shutil.rmtree(test_eval_dir, ignore_errors=True)
    setup_nwm_example(tmpdir=test_eval_dir)

    # Initialize the evaluation.
    ev = teehr.Evaluation(dir_path=test_eval_dir, create_dir=True)

    # add polygons from parquet in test data
    location_data_path = Path(test_eval_dir, "three_huc10s_radford.parquet")
    ev.locations.load_spatial(
        location_data_path,
        field_mapping={
            "huc10": "id"
        },
        location_id_prefix="wbd",
        write_mode="append"  # this is the default
    )

    # output locations map w/ points and polygons
    gdf = ev.locations.to_geopandas()
    outDir = Path(tmpdir, 'outputs')
    if not outDir.is_dir():
        outDir.mkdir(parents=True)
    gdf.teehr.locations_map(output_dir=outDir)
    assert Path(outDir, 'location_map.html').is_file()
    for file in outDir.glob('*'):
        file.unlink()

    # output locations map w/ points only
    gdf = ev.locations.to_geopandas()
    gdf = gdf[gdf.geometry.type == 'Point']
    gdf.teehr.locations_map(output_dir=outDir)
    assert Path(outDir, 'location_map.html').is_file()
    for file in outDir.glob('*'):
        file.unlink()

    # output locations map w/ polygons only
    gdf = ev.locations.to_geopandas()
    gdf = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]
    gdf.teehr.locations_map(output_dir=outDir)
    assert Path(outDir, 'location_map.html').is_file()


def test_location_attributes_map(tmpdir):
    """Test location_attributes table mapping with test data."""
    # Define the directory where the Evaluation will be created.
    test_eval_dir = Path(tmpdir, 'test_eval_home')
    if not test_eval_dir.is_dir():
        test_eval_dir.mkdir(parents=True)

    # Setup the example evaluation using data from the TEEHR repository.
    shutil.rmtree(test_eval_dir, ignore_errors=True)
    setup_nwm_example(tmpdir=test_eval_dir)

    # Initialize the evaluation.
    ev = teehr.Evaluation(dir_path=test_eval_dir, create_dir=True)

    # add polygons from parquet in test data
    location_data_path = Path(test_eval_dir, "three_huc10s_radford.parquet")
    ev.locations.load_spatial(
        location_data_path,
        field_mapping={
            "huc10": "id"
        },
        location_id_prefix="wbd",
        write_mode="append"  # this is the default
    )

    # add dummy attributes to evaluation
    test_attribute_polys = teehr.Attribute(
        name='aridity',
        type='continuous',
        description='Aridity index based on the ratio of precipitation to potential evapotranspiration.'
    )
    test_attribute_points = teehr.Attribute(
        name='tester',
        type='continuous',
        description='testity test test.'
    )
    ev.attributes.add([test_attribute_polys, test_attribute_points])

    # add dummy location_attributes to evaluation
    columns = ['location_id', 'attribute_name', 'value']
    data = [
        ['wbd-0505000115', 'aridity', 0.5],
        ['wbd-0505000117', 'aridity', 0.6],
        ['wbd-0505000118', 'aridity', 0.7],
        ['usgs-03171000', 'tester', 0.762]
    ]
    location_attributes_df = pd.DataFrame(data, columns=columns)
    outPath = Path(tmpdir, 'loc_atts', 'location_attributes.csv')
    if not outPath.parent.is_dir():
        outPath.parent.mkdir(parents=True)
    location_attributes_df.to_csv(outPath, index=False)
    ev.location_attributes.load_csv(
        outPath,
        write_mode="append"
    )

    # output location attributes map w/ points and polygons
    gdf = ev.location_attributes.to_geopandas()
    outDir = Path(tmpdir, 'outputs')
    if not outDir.is_dir():
        outDir.mkdir(parents=True)
    gdf.teehr.location_attributes_map(output_dir=outDir)
    assert Path(outDir, 'location_attributes_map.html').is_file()
    for file in outDir.glob('*'):
        file.unlink()

    # output location attributes map w/ points only
    gdf = ev.location_attributes.to_geopandas()
    gdf = gdf[gdf.geometry.type == 'Point']
    gdf.teehr.location_attributes_map(output_dir=outDir)
    assert Path(outDir, 'location_attributes_map.html').is_file()
    for file in outDir.glob('*'):
        file.unlink()

    # output location attributes map w/ polygons only
    gdf = ev.location_attributes.to_geopandas()
    gdf = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]
    gdf.teehr.location_attributes_map(output_dir=outDir)
    assert Path(outDir, 'location_attributes_map.html').is_file()
    for file in outDir.glob('*'):
        file.unlink()


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
                prefix="2-",
                dir=tmpdir
            )
        )
        test_locations_map(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tmpdir
            )
        )
        test_location_attributes_map(
            tempfile.mkdtemp(
                prefix="4-",
                dir=tmpdir
            )
        )
        test_location_crosswalks_map(
            tempfile.mkdtemp(
                prefix="5-",
                dir=tmpdir
            )
        )
        logger.info("All tests passed.")
