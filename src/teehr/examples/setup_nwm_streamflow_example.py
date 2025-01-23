"""Utility function for setting up the NWM streamflow fetching example."""
from pathlib import Path

import teehr
import teehr.example_data.fetch_nwm_streamflow as fetch_nwm_streamflow_data


def setup_nwm_example(tmpdir):
    """Set up the NWM streamflow fetching example."""
    # Create an Evaluation object and create the directory
    ev = teehr.Evaluation(dir_path=tmpdir, create_dir=True)

    # Clone the template
    ev.clone_template()

    # Fetch the test data
    location_data_path = Path(tmpdir, "usgs_at_radford_location.parquet")
    fetch_nwm_streamflow_data.fetch_file("usgs_at_radford_location.parquet", location_data_path)

    crosswalk_data_path = Path(tmpdir, "location_crosswalks.parquet")
    fetch_nwm_streamflow_data.fetch_file("location_crosswalks.parquet", crosswalk_data_path)

    primary_timeseries_path = Path(tmpdir, "usgs_timeseries.parquet")
    fetch_nwm_streamflow_data.fetch_file("usgs_timeseries.parquet", primary_timeseries_path)

    secondary_timeseries_path = Path(tmpdir, "nwm_forecasts.parquet")
    fetch_nwm_streamflow_data.fetch_file("nwm_forecasts.parquet", secondary_timeseries_path)

    # Load the data into the Evaluation
    ev.locations.load_spatial(in_path=location_data_path)
    ev.location_crosswalks.load_parquet(in_path=crosswalk_data_path)
    ev.primary_timeseries.load_parquet(in_path=primary_timeseries_path)
    ev.secondary_timeseries.load_parquet(in_path=secondary_timeseries_path)