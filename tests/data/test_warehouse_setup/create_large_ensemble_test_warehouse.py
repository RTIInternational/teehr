"""A script to recreate the ensemble_test_warehouse_large.tar.gz for testing."""  # noqa
import tarfile
import tempfile
from pathlib import Path

import pandas as pd
from shapely.geometry import Point
import geopandas as gpd

import teehr
import teehr.models.calculated_fields.row_level as rcf


def _create_warehouse(dir_path):
    """Create ensemble_test_warehouse_large.tar.gz."""
    ev = teehr.LocalReadWriteEvaluation(
        dir_path=Path(dir_path) / "ensemble_test_warehouse_large",
        create_dir=True
    )

    primary_ts_path = "tests/data/test_warehouse_data/large_ensemble_data/primary_timeseries.parquet"
    secondary_ts_path = "tests/data/test_warehouse_data/large_ensemble_data/secondary_timeseries.parquet"
    location_xwalk_path = "tests/data/test_warehouse_data/large_ensemble_data/location_crosswalks.parquet"
    configurations_path = "tests/data/test_warehouse_data/large_ensemble_data/configurations.parquet"
    variables_path = "tests/data/test_warehouse_data/large_ensemble_data/variables.parquet"

    # create locations
    location_dict = {
        'id': 'obs-GILN6',
        'name': 'Schoharie Creek at Schoharie',
        'geometry': Point(-74.45043182373047, 42.397300720214844),
    }
    gdf = gpd.GeoDataFrame(
        [location_dict],
        geometry='geometry',
        crs="EPSG:4269"
        )
    ev.locations.load_dataframe(df=gdf, write_mode="overwrite")

    # load crosswalk
    ev.location_crosswalks.load_parquet(
        in_path=location_xwalk_path
    )
    # add configurations
    df = pd.read_parquet(configurations_path)
    for _, row in df.iterrows():
        ev.configurations.add(
            teehr.Configuration(
                name=row["name"],
                type=row["type"],
                description=row["description"]
            )
        )
    # add variables table
    df = pd.read_parquet(variables_path)
    for _, row in df.iterrows():
        ev.variables.add(
            teehr.Variable(
                name=row["name"],
                long_name=row["long_name"],
            )
        )
    # load primary timeseries
    ev.primary_timeseries.load_parquet(
        in_path=primary_ts_path
    )
    # load secondary timeseries
    ev.secondary_timeseries.load_parquet(
        in_path=secondary_ts_path
    )

    # create JTS
    ev.joined_timeseries_view(add_attrs=False).add_calculated_fields([
        rcf.Month(),
        rcf.Year(),
        rcf.WaterYear(),
        rcf.Seasons()
    ]).write("joined_timeseries")

    # Save the warehouse to a tar.gz file for testing
    # Note. This will silently overwrite the file if it already exists.
    output = "tests/data/test_warehouse_data/ensemble_test_warehouse_large.tar.gz"
    with tarfile.open(output, "w:gz") as tar:
        tar.add(ev.dir_path, arcname=ev.dir_path.name)


def main():
    """Create the ensemble_test_warehouse_large.tar.gz file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        _create_warehouse(tmpdir)


if __name__ == "__main__":
    main()
