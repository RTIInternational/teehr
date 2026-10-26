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
        # The joined timeseries has prefixed variable name columns.
        variable_cols = [
            c for c in ["variable_name", "primary_variable_name", "secondary_variable_name"]
            if c in df.columns
        ]
        for col in variable_cols:
            df.loc[(df[col] == "rainfall_hourly_rate"), col] = "rainrate_hourly_mean"
        df.to_parquet(current_dir / filename, index=False)


if __name__ == "__main__":
    update_datasets()
