"""A script to recreate the two_location_test_warehouse.tar.gz for testing."""
import tarfile
import tempfile
from pathlib import Path

import pandas as pd

import teehr
import teehr.models.calculated_fields.row_level as rcf


def _create_warehouse(dir_path):
    """Create two_location_test_warehouse.tar.gz."""
    ev = teehr.LocalReadWriteEvaluation(
        dir_path=Path(dir_path) / "two_location_test_warehouse",
        create_dir=True
    )

    locations_filepath = "tests/data/test_warehouse_data/two_location_data/locations.parquet"
    location_attributes_filepath = "tests/data/test_warehouse_data/two_location_data/location_attributes.parquet"
    primary_filepath = "tests/data/test_warehouse_data/two_location_data/primary_timeseries.parquet"
    secondary_filepath = "tests/data/test_warehouse_data/two_location_data/secondary_timeseries.parquet"
    location_crosswalk_filepath = "tests/data/test_warehouse_data/two_location_data/location_crosswalks.parquet"
    attributes_filepath = "tests/data/test_warehouse_data/two_location_data/attributes.parquet"

    ev.configurations.add([
        teehr.Configuration(
            name="usgs_observations",
            timeseries_type="primary",
            description="USGS observations"
        ),
        teehr.Configuration(
            name="nwm30_retrospective",
            timeseries_type="secondary",
            description="NWM 3.0 Retrospective"
        )
    ])
    # Add some attributes
    attrs_df = pd.read_parquet(attributes_filepath)
    attr_list = []
    for row in attrs_df.itertuples(index=False):
        attr_list.append(
            teehr.Attribute(
                name=row.name,
                timeseries_type=row.type,
                description=row.description
            )
        )
    ev.attributes.add(attr_list)

    # Load the location data
    ev.locations.load_spatial(in_path=locations_filepath)
    # Load the crosswalk data
    ev.location_crosswalks.load_parquet(
        in_path=location_crosswalk_filepath
    )
    # Load the location attribute data
    ev.location_attributes.load_parquet(
        in_path=location_attributes_filepath,
    )

    # Load the timeseries data and map over the fields and set constants
    ev.primary_timeseries.load_parquet(
        in_path=primary_filepath
    )
    # Load the secondary timeseries data and map over the fields and set constants
    ev.secondary_timeseries.load_parquet(
        in_path=secondary_filepath
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
    output = "tests/data/test_warehouse_data/two_location_test_warehouse.tar.gz"
    with tarfile.open(output, "w:gz") as tar:
        tar.add(ev.dir_path, arcname=ev.dir_path.name)


def main():
    """Create the two_location_test_warehouse.tar.gz file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        _create_warehouse(tmpdir)


if __name__ == "__main__":
    main()
