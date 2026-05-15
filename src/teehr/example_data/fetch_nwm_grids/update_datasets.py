"""A helper script to update the datasets for the NWM gridded example."""
from pathlib import Path

import pandas as pd


def update_datasets():
    """Update the datasets for the NWM gridded example."""
    # Load the existing configurations
    current_dir = Path(__file__).resolve().parent

    filenames = [
        "joined_timeseries.parquet",
        "primary_timeseries.parquet",
        "secondary_timeseries.parquet"
    ]
    for filename in filenames:
        df = pd.read_parquet(current_dir / filename)
        df.loc[(df.variable_name == "rainfall_hourly_rate"), "variable_name"] = "rainrate_hourly_mean"
        df.to_parquet(current_dir / filename, index=False)


if __name__ == "__main__":
    update_datasets()
