"""Test the import_timeseries function in the Evaluation class."""
from pathlib import Path
from teehr import Evaluation, Configuration
import tempfile
import geopandas as gpd
import numpy as np
import pytest

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from data.setup_v0_3_study import setup_v0_3_study  # noqa


TEST_DATA_DIR = Path("tests", "data", "v0_3_test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_DATA_DIR, "geo", "gages.geojson")
PRIMARY_TIMESERIES_FILEPATH = Path(
    TEST_DATA_DIR, "timeseries", "test_short_obs.parquet"
)
CROSSWALK_FILEPATH = Path(TEST_DATA_DIR, "geo", "crosswalk.csv")
SECONDARY_TIMESERIES_FILEPATH = Path(
    TEST_DATA_DIR, "timeseries", "test_short_fcast.parquet"
)
GEO_FILEPATH = Path(TEST_DATA_DIR, "geo")


def test_create_joined_timeseries(tmpdir):
    """Test the validate_locations function."""
    tmpdir = Path(tmpdir)
    ev = Evaluation(dir_path=tmpdir, create_dir=True)

    # Clone the template
    ev.clone_template()

    # Enable logging
    ev.enable_logging()

    # Load the location data
    ev.locations.load_spatial(in_path=GEOJSON_GAGES_FILEPATH)

    ev.configurations.add(
        Configuration(
            name="usgs_observations",
            type="primary",
            description="test primary configuration"
        )
    )

    # Load the timeseries data and map over the fields and set constants
    ev.primary_timeseries.load_parquet(
        in_path=PRIMARY_TIMESERIES_FILEPATH,
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration": "configuration_name",
            "measurement_unit": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        },
        constant_field_values={
            "unit_name": "m^3/s",
            "variable_name": "streamflow_hourly_inst",
            "configuration_name": "usgs_observations"
        }
    )

    # Load the crosswalk data
    ev.location_crosswalks.load_csv(
        in_path=CROSSWALK_FILEPATH
    )

    ev.configurations.add(
        Configuration(
            name="nwm30_retrospective",
            type="secondary",
            description="test secondary configuration"
        )
    )

    # Load the secondary timeseries data and map over the fields
    #  and set constants
    ev.secondary_timeseries.load_parquet(
        in_path=SECONDARY_TIMESERIES_FILEPATH,
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration": "configuration_name",
            "measurement_unit": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        },
        constant_field_values={
            "unit_name": "m^3/s",
            "variable_name": "streamflow_hourly_inst",
            "configuration_name": "nwm30_retrospective"
        }
    )

    # Load the location attribute data
    ev.location_attributes.load_parquet(
        in_path=GEO_FILEPATH,
        field_mapping={"attribute_value": "value"},
        pattern="test_attr_*.parquet",
        update_attrs_table=True
    )

    # Create the joined timeseries with only specified attributes
    # include one invalid attribute 'tester' alongside valid attributes
    attr_list = ['drainage_area', 'ecoregion', 'tester']
    ev.joined_timeseries.create(add_attrs=True,
                                execute_scripts=True,
                                attr_list=attr_list)

    columns = ev.joined_timeseries.to_sdf().columns
    expected_columns = [
        'reference_time',
        'value_time',
        'primary_location_id',
        'secondary_location_id',
        'primary_value',
        'secondary_value',
        'unit_name',
        'drainage_area',
        'ecoregion',
        'month',
        'year',
        'water_year',
        'configuration_name',
        'variable_name',
        'member',
        'season'
    ]
    # Make sure secondary geodataframe is created correctly
    secondary_gdf = ev.secondary_timeseries.to_geopandas()

    assert isinstance(secondary_gdf, gpd.GeoDataFrame)
    assert all(np.sort(secondary_gdf["location_id"].unique()) == [
        "fcst-1",
        "fcst-2",
        "fcst-3"
    ])
    assert len(columns) == len(expected_columns)
    assert sorted(columns) == sorted(expected_columns)
    # ev.spark.stop()


def test_distinct_values(tmpdir):
    """Test base_table.distinct_values() using joined_timeseries."""
    tmpdir = Path(tmpdir)
    ev = setup_v0_3_study(tmpdir)

    # test primary_timeseries with location_prefixes==False (valid)
    distinct_vals = ev.primary_timeseries.distinct_values(column='location_id')
    assert len(distinct_vals) == 3

    # test primary_timeseries with location_prefixes==True (valid)
    prefixes = ev.primary_timeseries.distinct_values(
        column='location_id',
        location_prefixes=True
    )
    assert len(prefixes) == 1
    assert prefixes[0] == "gage"

    # test secondary_timeseries with location_prefixes==True (valid)
    prefixes = ev.secondary_timeseries.distinct_values(
        column='location_id',
        location_prefixes=True
    )
    assert len(prefixes) == 1
    assert prefixes[0] == "fcst"

    # test joined_timeseries for primary_location_id with
    # location_prefixes==True (valid)
    prefixes = ev.joined_timeseries.distinct_values(
        column='primary_location_id',
        location_prefixes=True
    )
    assert len(prefixes) == 1
    assert prefixes[0] == "gage"

    # test joined_timeseries for secondary_location_id with
    # location_prefixes==True (valid)
    prefixes = ev.joined_timeseries.distinct_values(
        column='secondary_location_id',
        location_prefixes=True
        )
    assert len(prefixes) == 1
    assert prefixes[0] == "fcst"

    # test locations for id with location_prefixes==True (valid)
    prefixes = ev.locations.distinct_values(
        column='id',
        location_prefixes=True
    )
    assert len(prefixes) == 1
    assert prefixes[0] == "gage"

    # test location_attributes for location_id with
    # location_prefixes==True (valid)
    prefixes = ev.location_attributes.distinct_values(
        column='location_id',
        location_prefixes=True
    )
    assert len(prefixes) == 1
    assert prefixes[0] == "gage"

    # test location_crosswalk for primary_location_id with
    # location_prefixes==True (valid)
    prefixes = ev.location_crosswalks.distinct_values(
        column='primary_location_id',
        location_prefixes=True
    )
    assert len(prefixes) == 1
    assert prefixes[0] == "gage"

    # test location_crosswalk for secondary_location_id with
    # location_prefixes=True (valid)
    prefixes = ev.location_crosswalks.distinct_values(
        column='secondary_location_id',
        location_prefixes=True
    )
    assert len(prefixes) == 1
    assert prefixes[0] == "fcst"

    # test invalid table handling with location_prefixes==True
    with pytest.raises(ValueError):
        prefixes = ev.locations.distinct_values(column='name',
                                                location_prefixes=True)

    # test invalid column handling for location_prefixes==True
    with pytest.raises(ValueError):
        prefixes = ev.joined_timeseries.distinct_values(column='location_id',
                                                        location_prefixes=True)

    # test invalid column handling for location_prefixes==False
    with pytest.raises(ValueError):
        prefixes = ev.joined_timeseries.distinct_values(column='test')
    # ev.spark.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_create_joined_timeseries(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_distinct_values(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
