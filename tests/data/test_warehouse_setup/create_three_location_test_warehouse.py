"""A script to recreate the three_location_test_warehouse.tar.gz for testing."""
import tarfile
import tempfile
from pathlib import Path

import teehr
import teehr.models.calculated_fields.row_level as rcf


def _create_warehouse(dir_path):
    """Create three_location_test_warehouse.tar.gz."""
    ev = teehr.LocalReadWriteEvaluation(
        dir_path=Path(dir_path) / "three_location_test_warehouse",
        create_dir=True
    )

    locations_filepath = "tests/data/test_warehouse_data/geo/gages.geojson"
    location_attributes_filepath = "tests/data/test_warehouse_data/geo"
    primary_filepath = "tests/data/test_warehouse_data/timeseries/test_short_obs.parquet"
    secondary_filepath = "tests/data/test_warehouse_data/timeseries/test_short_fcast.parquet"
    location_crosswalk_filepath = "tests/data/test_warehouse_data/geo/crosswalk.csv"

    ev.configurations.add([
        teehr.Configuration(
            name="usgs_observations",
            timeseries_type="primary",
            description="setup_v0_3_study primary configuration"
        ),
        teehr.Configuration(
            name="nwm30_retrospective",
            timeseries_type="secondary",
            description="setup_v0_3_study secondary configuration"
        )
    ])
    # Add some attributes
    ev.attributes.add(
        [
            teehr.Attribute(
                name="drainage_area",
                timeseries_type="continuous",
                description="Drainage area in square kilometers"
            ),
            teehr.Attribute(
                name="ecoregion",
                timeseries_type="categorical",
                description="Ecoregion"
            ),
            teehr.Attribute(
                name="year_2_discharge",
                timeseries_type="continuous",
                description="2-yr discharge in cubic meters per second"
            ),
        ]
    )

    # Load the location data
    ev.locations.load_spatial(in_path=locations_filepath)
    # Load the crosswalk data
    ev.location_crosswalks.load_csv(
        in_path=location_crosswalk_filepath
    )
    # Load the location attribute data
    ev.location_attributes.load_parquet(
        in_path=location_attributes_filepath,
        field_mapping={"attribute_value": "value"},
        pattern="test_attr_*.parquet",
    )

    # Load the timeseries data and map over the fields and set constants
    ev.primary_timeseries.load_parquet(
        in_path=primary_filepath,
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
    # Load the secondary timeseries data and map over the fields and set constants
    ev.secondary_timeseries.load_parquet(
        in_path=secondary_filepath,
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
    # Create the joined timeseries
    ev.joined_timeseries_view(add_attrs=True).add_calculated_fields([
        rcf.Month(),
        rcf.Year(),
        rcf.WaterYear(),
        rcf.Seasons()
    ]).write("joined_timeseries")

    # Save the warehouse to a tar.gz file for testing
    # Note. This will silently overwrite the file if it already exists.
    output = "tests/data/test_warehouse_data/three_location_test_warehouse.tar.gz"
    with tarfile.open(output, "w:gz") as tar:
        tar.add(ev.dir_path, arcname=ev.dir_path.name)


def main():
    """Create the three_location_test_warehouse.tar.gz file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        _create_warehouse(tmpdir)


if __name__ == "__main__":
    main()
