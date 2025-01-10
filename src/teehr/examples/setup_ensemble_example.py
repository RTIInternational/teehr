"""Setup an ensemble example for testing using data in the test directory."""
from pathlib import Path
from teehr import Evaluation, Configuration

# Specify a path to the directory where the evaluation will be created.
EVALUATION_DIR = Path(Path.home(), "teehr", "test_ensemble_evaluation")

TEST_STUDY_DATA_DIR_v0_4 = Path(Path.cwd(), "tests", "data", "test_study")


usgs_location = Path(
    TEST_STUDY_DATA_DIR_v0_4, "geo", "USGS_PlatteRiver_location.parquet"
)

secondary_filename = "MEFP.MBRFC.DNVC2LOCAL.SQIN.xml"
secondary_filepath = Path(
    TEST_STUDY_DATA_DIR_v0_4,
    "timeseries",
    secondary_filename
)
primary_filepath = Path(
    TEST_STUDY_DATA_DIR_v0_4,
    "timeseries",
    "usgs_hefs_06711565.parquet"
)

ev = Evaluation(dir_path=EVALUATION_DIR, create_dir=True)
ev.enable_logging()
ev.clone_template()

ev.locations.load_spatial(
    in_path=usgs_location
)
ev.location_crosswalks.load_csv(
    in_path=Path(TEST_STUDY_DATA_DIR_v0_4, "geo", "hefs_usgs_crosswalk.csv")
)
ev.configurations.add(
    Configuration(
        name="MEFP",
        type="primary",
        description="MBRFC HEFS Data"
    )
)
constant_field_values = {
    "unit_name": "ft^3/s",
    "variable_name": "streamflow_hourly_inst",
}
ev.secondary_timeseries.load_fews_xml(
    in_path=secondary_filepath,
    constant_field_values=constant_field_values
)
ev.primary_timeseries.load_parquet(
    in_path=primary_filepath
)
ev.joined_timeseries.create(execute_udf=False)