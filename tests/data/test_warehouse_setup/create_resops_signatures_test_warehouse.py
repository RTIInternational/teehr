"""Create a tar.gz archive of the test warehouse."""
import tarfile
import tempfile
from pathlib import Path
import pandas as pd

import teehr


def _create_warehouse(dir_path):
    """Create resops_test_warehouse.tar.gz from exported parquet files."""
    # Create evaluation in temp directory
    ev = teehr.LocalReadWriteEvaluation(
        dir_path=Path(dir_path) / "resops_signatures_test_warehouse",
        create_dir=True
    )

    primary_ts_path = "tests/data/test_warehouse_data/resops_signatures_data/primary_timeseries.parquet"
    secondary_ts_path = "tests/data/test_warehouse_data/resops_signatures_data/secondary_timeseries.parquet"
    location_xwalk_path = "tests/data/test_warehouse_data/resops_signatures_data/location_crosswalks.parquet"
    configurations_path = "tests/data/test_warehouse_data/resops_signatures_data/configurations.parquet"
    variables_path = "tests/data/test_warehouse_data/resops_signatures_data/variables.parquet"
    locations_path = "tests/data/test_warehouse_data/resops_signatures_data/locations.parquet"

    # Load locations from parquet
    ev.locations.load_spatial(locations_path)

    # Load crosswalks
    ev.location_crosswalks.load_parquet(location_xwalk_path)

    # Load configurations
    df = pd.read_parquet(configurations_path)
    for _, row in df.iterrows():
        ev.configurations.add(
            teehr.Configuration(
                name=row["name"],
                timeseries_type=row["timeseries_type"],
                description=row["description"]
            )
        )

    # Load variables
    df = pd.read_parquet(variables_path)
    for _, row in df.iterrows():
        ev.variables.add(
            teehr.Variable(
                name=row["name"],
                long_name=row["long_name"],
            )
        )

    # Load primary timeseries
    ev.primary_timeseries.load_parquet(primary_ts_path)

    # Load secondary timeseries
    ev.secondary_timeseries.load_parquet(secondary_ts_path)

    # Save the warehouse to a tar.gz file
    # Note: This will silently overwrite the file if it already exists
    output = "tests/data/test_warehouse_data/resops_signatures_test_warehouse.tar.gz"
    with tarfile.open(output, "w:gz") as tar:
        tar.add(ev.dir_path, arcname=ev.dir_path.name)


def main():
    """Create the resops_signatures_test_warehouse.tar.gz file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        _create_warehouse(tmpdir)


if __name__ == "__main__":
    main()
