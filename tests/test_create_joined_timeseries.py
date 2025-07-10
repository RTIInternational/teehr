"""Test the import_timeseries function in the Evaluation class."""
from pathlib import Path
from teehr import Evaluation, Configuration
from teehr.models.pydantic_table_models import (
    Attribute
)
import tempfile
import geopandas as gpd
import numpy as np


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
    ev = Evaluation(dir_path=tmpdir)

    # Enable logging
    ev.enable_logging()

    # Clone the template
    ev.clone_template()

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

    # Add some attributes
    ev.attributes.add(
        [
            Attribute(
                name="drainage_area",
                type="continuous",
                description="Drainage area in square kilometers"
            ),
            Attribute(
                name="ecoregion",
                type="categorical",
                description="Ecoregion"
            ),
            Attribute(
                name="year_2_discharge",
                type="continuous",
                description="2-yr discharge in cubic meters per second"
            ),
        ]
    )

    # Load the location attribute data
    ev.location_attributes.load_parquet(
        in_path=GEO_FILEPATH,
        field_mapping={"attribute_value": "value"},
        pattern="test_attr_*.parquet",
    )

    # Create the joined timeseries with only specified attributes
    attr_list = ['drainage_area', 'ecoregion']
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
