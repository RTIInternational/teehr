"""Utility function for setting up the NWM streamflow fetching example."""
from pathlib import Path

import teehr
import teehr.example_data.nwm_gridded_example_data as fetch_nwm_grid_data


def setup_nwm_example(tmpdir):
    """Set up the NWM streamflow fetching example."""
    # Create an Evaluation object and create the directory
    ev = teehr.Evaluation(dir_path=tmpdir, create_dir=True)

    # Clone the template
    ev.clone_template()

    # Fetch the test data
    location_data_path = Path(tmpdir, "usgs_at_radford_location.parquet")
    fetch_nwm_grid_data.fetch_file("usgs_at_radford_location.parquet", location_data_path)

    crosswalk_data_path = Path(tmpdir, "location_crosswalks.parquet")
    fetch_nwm_grid_data.fetch_file("location_crosswalks.parquet", crosswalk_data_path)

    forcing_crosswalk_data_path = Path(tmpdir, "location_crosswalks.parquet")
    fetch_nwm_grid_data.fetch_file("location_crosswalks.parquet", crosswalk_data_path)

    primary_timeseries_path = Path(tmpdir, "primary_timeseries.parquet")
    fetch_nwm_grid_data.fetch_file("primary_timeseries.parquet", primary_timeseries_path)

    secondary_timeseries_path = Path(tmpdir, "secondary_timeseries.parquet")
    fetch_nwm_grid_data.fetch_file("secondary_timeseries.parquet", secondary_timeseries_path)

    joined_timeseries_path = Path(tmpdir, "joined_timeseries.parquet")
    fetch_nwm_grid_data.fetch_file("joined_timeseries.parquet", joined_timeseries_path)

    configurations_path = Path(tmpdir, "configurations.parquet")
    fetch_nwm_grid_data.fetch_file("configurations.parquet", configurations_path)

    weights_path = Path(tmpdir, "nwm30_forcing_analysis_assim_pixel_weights.parquet")
    fetch_nwm_grid_data.fetch_file("nwm30_forcing_analysis_assim_pixel_weights.parquet", configurations_path)

    # Manually load the data into the Evaluation
    # joined timeseries
    shutil.copy(
        src=joined_timeseries_path,
        dst=ev.joined_timeseries.dir
    )
    # configurations
    shutil.copy(
        src=configurations_path,
        dst=ev.configurations.dir
    )
    # secondary
    shutil.copy(
        src=secondary_timeseries_path,
        dst=ev.secondary_timeseries.dir
    )
    # primary
    shutil.copy(
        src=primary_timeseries_path,
        dst=ev.primary_timeseries.dir
    )
    # location crosswalks
    shutil.copy(
        src=crosswalk_data_path,
        dst=ev.location_crosswalks.dir
    )
    # forcing location crosswalks
    shutil.copy(
        src=forcing_crosswalk_data_path,
        dst=tmpdir
    )
    # locations
    shutil.copy(
        src=location_data_path,
        dst=ev.locations.dir
    )
    # wbd locations
    shutil.copy(
        src=location_data_path,
        dst=tmpdir
    )
    # weights file
    shutil.copy(
        src=weights_path,
        dst=tmpdir
    )


if __name__ == "__main__":
    import shutil

    # Define the directory where the Evaluation will be created.
    test_eval_dir = Path(Path().home(), "temp", "11_fetch_nwm_gridded_data_TEMP")
    shutil.rmtree(test_eval_dir, ignore_errors=True)

    # Setup the example evaluation using data from the TEEHR repository.
    setup_nwm_example(tmpdir=test_eval_dir)

    # Initialize the evaluation.
    ev = teehr.Evaluation(dir_path=test_eval_dir)

    pass