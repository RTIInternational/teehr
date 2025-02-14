"""Setup an ensemble example for testing using data in the test directory."""
from pathlib import Path

import teehr
import teehr.example_data.ensemble_example_data as ensemble_example_data


def setup_hefs_example(tmpdir):
    """Set up the ensemble metrics example."""
    # Create an Evaluation object and create the directory
    ev = teehr.Evaluation(dir_path=tmpdir, create_dir=True)

    # Clone the template
    ev.clone_template()

    # Fetch the test data
    location_data_path = Path(tmpdir, "two_locations.parquet")
    ensemble_example_data.fetch_file("two_locations.parquet", location_data_path)

    crosswalk_data_path = Path(tmpdir, "location_crosswalks.parquet")
    ensemble_example_data.fetch_file("location_crosswalks.parquet", crosswalk_data_path)

    primary_timeseries_path = Path(tmpdir, "primary_timeseries.parquet")
    ensemble_example_data.fetch_file("primary_timeseries.parquet", primary_timeseries_path)

    secondary_timeseries_path = Path(tmpdir, "secondary_timeseries.parquet")
    ensemble_example_data.fetch_file("secondary_timeseries.parquet", secondary_timeseries_path)

    ev.configurations.add(
        teehr.Configuration(
            name="CNRFC_HEFS",
            type="secondary",
            description="CNRFC HEFS Extended Forecast"
        )
    )

    ev.configurations.add(
        teehr.Configuration(
            name="usgs_observations",
            type="primary",
            description="USGS Observations"
        )
    )

    # Load the data into the Evaluation
    ev.locations.load_spatial(in_path=location_data_path)
    ev.location_crosswalks.load_parquet(in_path=crosswalk_data_path)
    ev.primary_timeseries.load_parquet(in_path=primary_timeseries_path)
    ev.secondary_timeseries.load_parquet(in_path=secondary_timeseries_path)


if __name__ == "__main__":
    import shutil

    # Define the directory where the Evaluation will be created.
    test_eval_dir = Path(Path().home(), "temp", "09_ensemble_metrics")
    shutil.rmtree(test_eval_dir, ignore_errors=True)

    # Setup the example evaluation using data from the TEEHR repository.
    setup_hefs_example(tmpdir=test_eval_dir)

    # Initialize the evaluation.
    ev = teehr.Evaluation(dir_path=test_eval_dir)

    pass